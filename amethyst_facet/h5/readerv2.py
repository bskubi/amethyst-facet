import dataclasses as dc
import logging
from pathlib import Path
from typing import *
import warnings

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
                return f"not h5py.Dataset (type={type(it)})"
            elif not self.is_observations(it):
                return f"not observations dtype (dtype={it.dtype})"
            return False
        
        yield from self.read(barcode, "observations", ignore)

    def barcode_windows(self, barcode: h5py.Group):
        def ignore(it):
            if not isinstance(it, h5py.Dataset):
                return f"not h5py.Dataset (type={type(it)})"
            elif not self.is_windows(it):
                return f"not windows dtype (dtype={it.dtype})"
            return False
    
        yield from self.read(barcode, "windows", ignore)

    def barcodes(self) -> Generator[h5py.Group, None, None]:
        for context in self.contexts():
            yield from self.context_barcodes(context)

    def create_dataset(self, file_path, h5_path, data):
        context, barcode, name = h5_path.split("/")[1:]
        result = Dataset(context, barcode, name, data, path=Path(file_path))
        return result

    def observations(self) -> Generator[Dataset, None, None]:
        for barcode in self.barcodes():
            for it in self.barcode_observations(barcode):
                yield self.create_dataset(*it)

    def windows(self) -> Generator[Dataset, None, None]:
        for barcode in self.barcodes():
            for it in self.barcode_windows(barcode):
                yield self.create_dataset(*it)