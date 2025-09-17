import h5py
from pathlib import Path
from queue import Queue, Empty
from threading import Thread
from typing import Any, Callable, Dict, Tuple

def hdf5_worker_loop(queue: Queue, error_queue: Queue, file_path: Path, file_options: Dict[str, Any]):
    """
    The target function for a writer thread; it owns a single h5py.File handle.

    This function opens a single HDF5 file with the provided options, then
    enters a loop to wait for and execute tasks from its queue.

    Args:
        queue: The queue from which to receive tasks.
        error_queue: The queue to which any exceptions will be sent.
        file_path: The absolute path to the HDF5 file.
        file_options: A dictionary of keyword arguments for opening the file
            (e.g., mode, swmr, libver). Note that the value of the file's swmr_mode
            attribute is set to the value of swmr after opening; swmr is not applied to
            the File.__init__ method.
    """
    try:
        swmr_mode = file_options.pop("swmr", False)
        with h5py.File(file_path, **file_options) as h5_file:
            h5_file.swmr_mode = swmr_mode
            while True:
                command = queue.get()
                if command is None:
                    break
                
                target_func, args, kwargs = command
                try:
                    target_func(h5_file, *args, **kwargs)
                except Exception as e:
                    # If a task fails, put the exception on the error queue
                    error_info = (file_path, command, e)
                    error_queue.put(error_info)
                finally:
                    queue.task_done()
    except Exception as e:
        # Handle errors opening the file itself
        error_queue.put((file_path, None, e))

class HDF5Manager:
    """Manages background threads for non-blocking, thread-safe HDF5 I/O.

    This class provides a thread-safe interface for performing non-blocking
    writes to multiple HDF5 files. It works around the limitation that
    h5py.File objects are not safe for concurrent writes by creating a
    dedicated background worker thread for each file. This ensures all
    operations for a single file are safely serialized, while allowing for
    parallel I/O across different files.

    By default, files are opened in append ('a') and SWMR (Single-Writer /
    Multiple-Reader) mode, allowing other processes to safely read a file
    while it is being written to.

    The manager should be used as a context manager (with a 'with' statement)
    to ensure graceful shutdown of all background threads.

    The manager is designed to be used as a context manager (with a 'with'
    statement) to ensure that all background threads are gracefully shut down
    and all file handles are cleanly closed.

    Exceptions that occur in background threads are caught and placed in an
    error queue. They will be re-raised in the main thread when the manager
    is shut down or when check_errors() is called.

    Example:
        with HDF5Manager() as manager:
            # Submit a task to be executed in the background
            manager.submit(
                "my_file.h5",
                h5py.File.create_dataset,
                "dset1",
                data=np.arange(10)
            )
            # The main thread is not blocked and can continue other work.
    """
    def __init__(self):
        self._workers: Dict[Path, Dict[str, Any]] = {}
        self.error_queue = Queue()

    def submit(self, file_path: str | Path, target_func: Callable, *args, file_options: Dict[str, Any] = None, **kwargs):
        """Submits a task to be executed on a specific HDF5 file.

        The task is added to a queue and will be executed by a dedicated
        background thread for the given file path. This call is non-blocking.

        Args:
            file_path: The path to the target HDF5 file.
            target_func: The function to execute. It will be called with the
                h5py.File object as its first argument, followed by any
                provided *args and **kwargs.
            file_options (Dict, optional): Options for opening the HDF5 file.
                These are only used when a file's worker is first created.
                Defaults to append mode and SWMR mode. SWMR mode is controlled by
                {'swmr': True, 'libver': 'latest'}.
            *args: Positional arguments for the target_func.
            **kwargs: Keyword arguments for the target_func.
        
        Example:
            # Write to a dataset with default file options
            manager.submit(
                "my_file.h5",
                h5py.File.create_dataset,
                "dset1",
                data=np.arange(10)
            )

            # Open a new file in write ('w') mode instead of append
            manager.submit(
                "new_file.h5",
                h5py.File.create_dataset,
                "dset1",
                file_options={'mode': 'w'},
                data=np.arange(10)
            )
        """
        path = Path(file_path).resolve()
        queue = self._get_or_create_queue(path, file_options or {})
        command = (target_func, args, kwargs)
        queue.put(command)

    def _get_or_create_queue(self, path: Path, user_options: Dict[str, Any]) -> Queue:
        """Starts a worker for a file path if it doesn't already exist."""
        if path not in self._workers:
            default_options = {'mode': 'a', 'swmr': True, 'libver': 'latest'}
            final_options = default_options.copy()
            final_options.update(user_options)

            queue = Queue()
            # Pass the error_queue to the worker
            thread = Thread(target=hdf5_worker_loop, args=(queue, self.error_queue, path, final_options), daemon=True)
            thread.start()
            self._workers[path] = {"queue": queue, "thread": thread}
        return self._workers[path]["queue"]
        
    def check_errors(self):
        """Checks for and raises the first exception found from a worker.

        If a background task has failed, this method will raise the
        original exception in the main thread. This allows for "fail-fast"
        error checking. The shutdown() method calls this automatically
        to ensure no errors are missed.

        Raises:
            Exception: The original exception from the failed worker task.
        """
        try:
            file_path, command, exc = self.error_queue.get_nowait()
            # Re-raise the original exception to preserve its type and traceback
            raise exc from RuntimeError(f"Error in worker for file '{file_path}'")
        except Empty:
            # No errors in the queue
            pass

    def shutdown(self):
        """Shuts down all worker threads gracefully and checks for final errors."""
        for worker in self._workers.values():
            worker["queue"].put(None)
            worker["thread"].join()
        
        # Final error check to catch any remaining issues
        self.check_errors()
        self._workers.clear()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()