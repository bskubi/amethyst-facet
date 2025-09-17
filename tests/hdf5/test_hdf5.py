from pathlib import Path
import tempfile
from functools import partial
import time
import threading

import h5py
import numpy as np
import pytest

import amethyst_facet as fct

def test_hdf5_manager_writes():
    """Test writing to and reading from multiple h5 files using HDF5Manager
    """
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        size = (5_000_000,)
        # Write large blocks to files
        with fct.hdf5.HDF5Manager() as h5:
            random_data = np.random.randint(low=0, high=1000, size=size)
            h5.submit(tmp / "file1.h5", h5py.File.create_dataset, "test1", data=random_data)
            h5.submit(tmp / "file1.h5", h5py.File.create_dataset, "test2", data=random_data)
            h5.submit(tmp / "file2.h5", h5py.File.create_dataset, "test1", data=random_data)
            
        # Check to make sure right number of items written
        with (h5py.File(tmp / "file1.h5") as file1, h5py.File(tmp / "file2.h5") as file2):
            assert file1["test1"].shape == size
            assert file1["test2"].shape == size
            assert file2["test1"].shape == size
        
def test_hdf5_manager_exceptions():
    """Test that creating duplicate dataset name with HDF5Manager raises exception
    """
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp) / "file1.h5"
        
        with pytest.raises(match="name already exists"):
            with fct.hdf5.HDF5Manager() as h5:
                common = (tmp, h5py.File.create_dataset)
                # Test creating datasets with same name in same file (should raise exception)
                h5.submit(*common, "test1", data=0)
                h5.submit(*common, "test1", data=1)
                h5.submit(*common, "test2", data=2)

        # This should still have created the other two datasets, so verify that
        with h5py.File(tmp) as file:
            assert file["/test1"][()] == 0
            assert file["/test2"][()] == 2

def sleep_then_create_dataset(file: h5py.File, *args, **kwargs):
    """Simulate slow write
    """
    # Write takes 0.02s
    time.sleep(.02),
    file.create_dataset(*args, **kwargs)

def test_long_concurrent_writes_to_separate_files():
    """Simulate concurrent nonblocking slow and fast writes to separate files
    """
    with tempfile.TemporaryDirectory() as tmp:
        tmp1 = Path(tmp) / "file1.h5"
        tmp2 = Path(tmp) / "file2.h5"

        with fct.hdf5.HDF5Manager() as h5:
            # Submit slow write to "test1" and fast write to "test2"
            # in separate files. "test2" should be written while waiting for "test1"
            h5.submit(tmp1, sleep_then_create_dataset, "test1", data=0)
            h5.submit(tmp2, h5py.File.create_dataset, "test2", data=0)

            # Brief delay to allow dataset to be written to tmp2, but
            # less than the 0.02s required to write to test1
            time.sleep(.01)
            with (h5py.File(tmp1) as file1, h5py.File(tmp2) as file2):
                # At this point, "test2" (fast write) should be written
                # but "test1" (slow write simulation) should not be.
                assert "/test2" in file2
                assert "/test1" not in file1

        # The manager should have blocked until all writes are complete
        # so both files should now be created.
        with (h5py.File(tmp1) as file1, h5py.File(tmp2) as file2):
            assert "/test2" in file2
            assert "/test1" in file1

def test_shutdown_blocks():
    """Ensure that HDF5Manager.shutdown() actually blocks until the write finishes
    """
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp) / "file1.h5"
        h5 = fct.hdf5.HDF5Manager()

        # Submit a write that delays 0.02s before starting the write
        h5.submit(tmp, sleep_then_create_dataset, "test1", data=0)

        # Time the length of the write process to ensure it takes at least 0.02s
        # to confirm that shutdown is truly blocking until the end of the write
        start = time.time()
        h5.shutdown()
        end = time.time()
        duration = end-start
        assert duration > 0.02

def test_swmr_mode_allows_concurrent_reads():
    """
    Tests that a file opened in SWMR mode by the manager can be
    read by another thread while writes are in progress.
    """
    # Use threading.Event for synchronization between writer and reader
    writer_has_started = threading.Event()
    writer_has_finished = threading.Event()
    
    reader_errors = [] # To capture exceptions from the reader thread

    def reader_thread_target(file_path):
        """The function for our reader thread."""
        try:
            # 1. Wait until the writer has created the file and initial dataset
            writer_has_started.wait(timeout=5)
            assert writer_has_started.is_set(), "Timed out waiting for writer to start."

            with h5py.File(file_path, 'r', swmr=True, libver='latest') as f:
                dset = f['data']
                # 2. Assert the initial state of the dataset
                assert dset.shape == (10,)
                
                # 3. Wait until the writer signals it has finished appending data
                writer_has_finished.wait(timeout=5)
                assert writer_has_finished.is_set(), "Timed out waiting for writer to finish."
                
                # 4. Refresh the dataset to see the new data
                dset.refresh()
                
                # 5. Assert the final state of the dataset
                assert dset.shape == (20,)
        except Exception as e:
            reader_errors.append(e)

    def writer_task(h5_file):
        """The task submitted to the HDF5Manager."""
        # Create the initial dataset
        dset = h5_file.create_dataset('data', shape=(10,), maxshape=(None,), dtype='i8')
        dset[:] = np.arange(10)
        h5_file.flush()
        
        # Signal to the reader that it can now open the file
        writer_has_started.set()
        
        # Simulate more work before appending
        time.sleep(0.1)
        
        # Append more data
        dset.resize((20,))
        dset[10:] = np.arange(10, 20)
        h5_file.flush()
        
        # Signal to the reader that the final write is done
        writer_has_finished.set()

    with tempfile.TemporaryDirectory() as tmp:
        file_path = Path(tmp) / "swmr_test.h5"
        
        # Start the reader thread. It will wait for the writer.
        reader = threading.Thread(target=reader_thread_target, args=(file_path,))
        reader.start()

        # Use the manager to run the writer task
        with fct.hdf5.HDF5Manager() as manager:
            manager.submit(
                file_path, 
                writer_task,
                file_options={'mode': 'w', 'swmr': True, 'libver': 'latest'}
            )
        
        # Wait for the reader thread to finish its assertions
        reader.join()
        
        # If the reader thread had an exception, re-raise it here
        if reader_errors:
            raise reader_errors[0]