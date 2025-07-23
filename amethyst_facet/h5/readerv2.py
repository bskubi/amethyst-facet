import dataclasses as dc
from pathlib import Path
from typing import *

import h5py

from .dataset import Dataset
from .reader import Reader

class ReaderException(Exception):
    def __init__(self, message: str):
        super().__init__(message)

@dc.dataclass
class ReaderV2(Reader):
    reader_type: str = "ReaderV2"

    def barcode_observations(self, barcode: h5py.Group):
        def ignore(it):
            if not isinstance(it, h5py.Dataset):
                return f"type={type(it)}"
            elif not self.is_observations(it):
                return f"dtype={it.dtype}"
            return False
        
        yield from self.read(barcode, "observations", ignore)

    def barcode_windows(self, barcode: h5py.Group):
        def ignore(it):
            if not isinstance(it, h5py.Dataset):
                return f"type={type(it)}"
            elif not self.is_windows(it):
                return f"dtype={it.dtype}"
            return False
    
        yield from self.read(barcode, "windows", ignore)

    def barcodes(self) -> Generator[h5py.Group]:
        for context in self.contexts():
            yield from self.context_barcodes(context)

    def create_dataset(self, path, data):
        path = path.split("/")[1:]
        context, barcode, name = path
        return Dataset(context, barcode, name, data, path=path)

    def observations(self) -> Generator[Dataset]:
        for barcode in self.barcodes():
            for path, data in self.barcode_observations(barcode):
                yield self.create_dataset(path, data)

    def windows(self) -> Generator[Dataset]:
        for barcode in self.barcodes():
            for path, data in self.barcode_windows(barcode):
                yield self.create_dataset(path, data)