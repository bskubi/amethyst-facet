from contextlib import contextmanager
import dataclasses as dc
import logging
from pathlib import Path
from typing import *

import h5py

import amethyst_facet as fct

class InvalidDecrement(Exception):
    def __init__(self, message: str):
        message = (
            f"Attempted to decrement count on H5UserCounter with zero or negative count.\n"
            f"{message}"
        )
        super().__init__(message)

@dc.dataclass
class H5UserCounter:
    file: h5py.File
    count: int = 0

    def __post_init__(self):
        self.increment()

    def increment(self):
        self.count += 1
    
    def decrement(self):
        if self.count <= 0:
            raise InvalidDecrement(f"{self}")
        self.count -= 1

    def closed_unused(self):
        if self.count <= 0:
            self.file.close()
            return True
        return False

handles: Dict[str, H5UserCounter] = dict()

def read_version(path: str | Path):
    try:
        with open(path) as file:
            return file["/metadata/version"][()].decode()
    except:
        return None

def version_match(path: str | Path):
    return read_version(path) == fct.h5.version

def write_version(path: str | Path):
    with open(path) as file:
        file.create_dataset("/metadata/version", data=fct.h5.version)
        logging.debug(f"Wrote Amethyst /metadata/version='{fct.h5.version}' to {path}")

def close(path: str | Path):
    if path in handles:
        handles[path].decrement()
        if handles[path].closed_unused():
            del handles[path]


@contextmanager
def open(path: str | Path, mode: str = "a", *args, **kwargs) -> Generator[h5py.File, None, None]:
    try:
        if path not in handles:
            file = h5py.File(path, mode=mode, *args, **kwargs)
            handles[path] = H5UserCounter(file)
        else:
            handles[path].increment()
        yield handles[path].file
    finally:
        close(path)