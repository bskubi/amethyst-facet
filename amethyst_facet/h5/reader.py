import dataclasses as dc
import logging
from pathlib import Path
from typing import *

import h5py

from .dataset import Dataset
import amethyst_facet as fct

class ReaderException(Exception):
    def __init__(self, message: str):
        super().__init__(message)

@dc.dataclass
class Reader:
    paths: List[str | Path] = dc.field(default_factory=list)
    skip: Dict[str, Set] = dc.field(default_factory=dict)
    only: Dict[str, Set] = dc.field(default_factory=dict)
    mode: str = "a"
    reader_type: str = "Reader"

    def obtain(self, item: h5py.Dataset):
        if isinstance(item, h5py.Dataset):
            return item.name, item[:]
        else:
            return item
    
    def not_implemented_error(self):
        raise NotImplementedError("Cannot use amethyst_facet.h5.Reader class directly -- use ReaderV1 or ReaderV2")

    def read(
            self, 
            file_or_group: h5py.File | h5py.Group, 
            level: str,
            ignore: Callable = lambda x: False
            ) -> Generator[h5py.Group | h5py.Dataset]:
        skip = self.skip.get(level, set())
        only = self.only.get(level, set())
        logging.debug(f"{self.reader_type} reading from {file_or_group} {level}")
        if only:
            only = only.difference(skip)
            logging.debug(f"only={only}\n")
            for only_item in only:
                present = only_item in file_or_group
                ignore_it = ignore(file_or_group[only_item])
                if present and not ignore_it:
                    logging.debug(f"Yielding {level} {file_or_group[only_item].name}")
                    yield self.obtain(file_or_group[only_item])
                else:
                    logging.debug(f"Skipped {h5_item} (present={present}, ignored={ignore_it}, type={type(file_or_group[only_item])})")
        else:
            for h5_item in file_or_group:
                not_skipped = h5_item not in skip
                ignore_it = ignore(file_or_group[h5_item])
                if not_skipped and not ignore_it:
                    logging.debug(f"Yielding {level} {file_or_group[h5_item].name}")
                    yield self.obtain(file_or_group[h5_item])
                else:
                    logging.debug(f"Skipped {h5_item} (not_skipped={not_skipped}, ignored={ignore_it}, type={type(file_or_group[h5_item])})")

    def is_observations(self, dataset: h5py.Dataset) -> bool:
        return isinstance(dataset, h5py.Dataset) and all(col in dataset.dtype.names for col in ["chr", "pos"])

    def is_windows(self, dataset: h5py.Dataset) -> bool:
        return isinstance(dataset, h5py.Dataset) and all(col in dataset.dtype.names for col in ["chr", "start", "end"])

    def file_contexts(self, file: h5py.File):        
        def ignore(it):
            if not isinstance(it, h5py.Group):
                return f"type={type(it)}"
            elif it.name == "/metadata":
                return f"name=/metadata"
            return False

        yield from self.read(file, "contexts", ignore)

    def context_barcodes(self, context: h5py.Group):
        def ignore(it):
            if not isinstance(it, h5py.Group):
                return f"type={type(it)}"
            return False
        yield from self.read(context, "barcodes", ignore)

    def contexts(self) -> Generator[h5py.Group]:
        for path in self.paths:
            with fct.h5.open(path, mode=self.mode) as file:
                yield from self.file_contexts(file)

    def barcode_observations(self, barcode: h5py.Group):
        self.not_implemented_error()

    def barcode_windows(self, barcode: h5py.Group):
        self.not_implemented_error()

    def barcodes(self) -> Generator[h5py.Group]:
        self.not_implemented_error()

    def observations(self) -> Generator[Dataset]:
        self.not_implemented_error()

    def windows(self) -> Generator[Dataset]:
        self.not_implemented_error()