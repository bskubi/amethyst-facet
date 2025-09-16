import glob
from pathlib import Path

import h5py
import numpy as np
import pytest
import polars as pl

import amethyst_facet as fct

# def test_ingest_from_parquet():
#     # Load from ScaleMethyl format parquet files
#     file = fct.from_scalemethyl_parquet(contexts = {"CG": "test.met_CG.parquet", "CH": "test.met_CH.parquet"})

#     for dataset in file.datasets:
#         # Autodetects schema
#         fct.Schema.to_polars(dataset)
#         assert (schema := fct.Schema.detect(dataset) in ["bp", "agg"]), f"Unknown schema detected: {schema}"

# def test_read_files():
#     v1 = fct.File("amethyst_v1.h5")
#     v2 = fct.File("amethyst_v2.h5")
#     # Transfer files from the V1 file to the V2 file
#     for dataset in v1.datasets(skip_h5 = lambda dset: fct.Schema.barcode(dset) in ["ACGTGTCA"]):
#         with fct.H5LazyWriter.open_from(v2) as file:
#             path = Path(dataset.filename) / "1"
#             file.create_dataset(dataset, data=dataset[:])

# def test_inspect_files():
#     amethyst_inspector = fct.AmethystFileInspector(files = glob.glob("*.h5"))
#     require_data = lambda data: len(data) > 1_000

#     # Autodetects V1 vs V2
#     for dataset in amethyst_inspector.datasets(
#         skip_h5 = lambda dset: fct.Schema.context(dset) == "CH" or fct.Schema.barcode(dset) in ["ACATGCA", "ACGTGCA"],
#         require_h5 = lambda dset: fct.Schema.dataset_name(dset) == "1"
#     ):
#         if not require_data(dataset[:]):
#             continue

#         df = fct.Schema.to_polars(dataset)
#         df = fct.aggregate(df)

#         # fct.File.open_from(dataset) returns an H5LazyWriter

#         with fct.H5LazyWriter.open_from(dataset) as file:
#             path = Path(dataset.parent) / "agg"

#             # This should lazily dispatch a request to write to the file when it's not blocked.
#             # Data is temporarily stored in diskcache until the file can be written to
#             file.delete(path, missing_ok=True, groups=True)
#             file.create_dataset(path, data=fct.Schema.from_polars(df))

def test_single_file_write():
    """
    Tests that the manager can create a single file and write a dataset correctly.
    """
    # 1. Arrange: Create a temporary path and test data
    temp_dir = Path("./temp_test_files")
    temp_dir.mkdir(exist_ok=True)
    test_file_path = temp_dir / "test1.h5"
    test_data = np.array([1, 2, 3, 4, 5])
    dataset_name = "/my_data/test_array"

    # 2. Act: Use the manager to write the data
    with fct.hdf5.file.HDF5WriteManager() as manager:
        manager.create_dataset(
            path=test_file_path, 
            name=dataset_name, 
            data=test_data, 
            mode="w"
        )

    # 3. Assert: Read the file back and verify its contents
    assert test_file_path.exists()
    with h5py.File(test_file_path, "r") as f:
        assert dataset_name in f
        read_data = f[dataset_name][:]
        np.testing.assert_array_equal(read_data, test_data)

    # Clean up the test file
    test_file_path.unlink()
    if not any(temp_dir.iterdir()):
        temp_dir.rmdir()

def test_capacity_with_multiple_files(tmp_path: Path):
    """
    Tests that the manager correctly processes a large, accumulating queue 
    of writes destined for multiple files.
    """
    # 1. Arrange: Define the test parameters and file paths
    num_files = 5
    writes_per_file = 100
    file_paths = [tmp_path / f"capacity_test_{i}.h5" for i in range(num_files)]
    data_to_write = np.random.randint(0, 1000, 10000)

    # 2. Act: Rapidly queue all write operations
    with fct.hdf5.file.HDF5WriteManager() as manager:
        for file_idx, file_path in enumerate(file_paths):
            for write_idx in range(writes_per_file):
                # Create deterministic data and dataset name for later verification
                dataset_name = f"/data/set_{write_idx}"
                 # Unique data
                
                manager.create_dataset(
                    path=file_path,
                    name=dataset_name,
                    data=data_to_write,
                    mode="a"
                )

    # The 'with' block exit ensures all 500 writes are completed.

    # 3. Assert: Verify every single write in every file
    for file_idx, file_path in enumerate(file_paths):
        assert file_path.exists()
        with h5py.File(file_path, "r") as f:
            assert len(f["/data"].keys()) == writes_per_file
            for write_idx in range(writes_per_file):
                dataset_name = f"/data/set_{write_idx}"
                expected_data = data_to_write
                
                assert dataset_name in f
                read_data = f[dataset_name][:]
                np.testing.assert_array_equal(read_data, expected_data)