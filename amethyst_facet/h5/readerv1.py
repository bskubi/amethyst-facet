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
class ReaderV1(Reader):
    default_name: str = "1"
    reader_type: str = "ReaderV1"

    def create_dataset(self, file_path, h5_path, data):
        context, barcode = h5_path.split("/")[1:]
        name = self.default_name or h5_path
        return Dataset(context, barcode, name, data, path=Path(file_path))

    def barcodes(self):
        def ignore(it):
            if not isinstance(it, h5py.Dataset):
                return f"not h5py.Dataset (type={type(it)})"
            return False

        for context in self.contexts():
            for it in self.read(context, "barcodes", ignore):
                yield self.create_dataset(*it)

    def observations(self):
        def ignore(it):
            if not isinstance(it, h5py.Dataset):
                return f"not h5py.Dataset(type={type(it)})"
            elif not self.is_observations(it):
                return f"not observations dtype (dtype={it.dtype})"
            return False

        for context in self.contexts():
            for it in self.read(context, "barcodes", ignore):
                yield self.create_dataset(*it)

    def windows(self):
        def ignore(it):
            if not isinstance(it, h5py.Dataset):
                return f"not h5py.Dataset (type={type(it)})"
            elif not self.is_windows(it):
                return f"not windows dtype (dtype={it.dtype})"
            return False

        for context in self.contexts():
            for it in self.read(context, "barcodes", ignore):
                yield self.create_dataset(*it)

    def barcode_observations(self, barcode: h5py.Group):
        raise NotImplementedError("Not implemented in ReaderV1 as all barcodes are Datasets in Amethyst file format v1.")

    def barcode_windows(self, barcode: h5py.Group):
        raise NotImplementedError("Not implemented in ReaderV1 as all barcodes are Datasets in Amethyst file format v1.")