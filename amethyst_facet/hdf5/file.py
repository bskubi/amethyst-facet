from dataclasses import dataclass, field
from pathlib import Path
from queue import Queue
from threading import Thread
from typing import Any, Dict

import h5py
import numpy as np

@dataclass(frozen=True)
class _WriteCommand:
    """A simple object to hold the details of a single write operation."""
    action: str
    args: tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)

def _worker_loop(queue: Queue, path: Path, file_open_kwargs: Dict[str, Any]):
    """
    The target function for each writer thread.
    It waits for commands from the queue and executes them.
    """
    try:
        with h5py.File(path, **file_open_kwargs) as h5_file:
            while True:
                # Wait for the next command from the queue
                command = queue.get()

                # A 'None' command is the signal to shut down
                if command is None:
                    break
                
                # Dispatch the action based on the command
                if command.action == "create_dataset":
                    h5_file.create_dataset(*command.args, **command.kwargs)
                elif command.action == "create_group":
                    h5_file.create_group(*command.args, **command.kwargs)
                else:
                    raise NotImplementedError(f"Action not implemented: {command.action}")
                
                queue.task_done()
    finally:
        # This ensures the h5py.File is closed by the 'with' statement
        # before the thread terminates.
        pass

class HDF5WriteManager:
    """Manages background writer threads for multiple HDF5 files."""
    def __init__(self):
        self._workers: Dict[str, Dict[str, Any]] = {}

    def _get_or_create_queue(self, path: Path, file_open_kwargs: Dict[str, Any]) -> Queue:
        """Initializes a worker thread for a file path if it doesn't exist."""
        path_str = str(path.resolve())
        if path_str not in self._workers:
            queue = Queue()
            # Daemon threads exit when the main program exits
            thread = Thread(
                target=_worker_loop,
                args=(queue, path, file_open_kwargs),
                daemon=True 
            )
            thread.start()
            self._workers[path_str] = {"queue": queue, "thread": thread}
        return self._workers[path_str]["queue"]

    def create_dataset(self, path: Path, name: str, **kwargs):
        """Queue a 'create_dataset' operation."""
        # Separate file-opening kwargs from dataset-creation kwargs
        file_kwargs = {"mode": kwargs.pop("mode", "a")} # Default to append mode
        command = _WriteCommand(action="create_dataset", args=(name,), kwargs=kwargs)
        queue = self._get_or_create_queue(path, file_kwargs)
        queue.put(command)

    def shutdown(self, wait: bool = True):
        """Signals all worker threads to shut down and closes all files."""
        for worker in self._workers.values():
            # Send the 'None' sentinel to signal shutdown
            worker["queue"].put(None)
            if wait:
                # Wait for the thread to finish its work and close the file
                worker["thread"].join()
        self._workers.clear()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # The context manager ensures shutdown() is always called.
        self.shutdown()