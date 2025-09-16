import dataclasses as dc
import logging
from pathlib import Path
from typing import *
import warnings

import h5py
from numpy.typing import NDArray

from .dataset import Dataset
import amethyst_facet as fct

class ReaderException(Exception):
    def __init__(self, message: str):
        super().__init__(message)

class ReaderFileMismatch(ReaderException):
    def __init__(self, class_type: type, path: str | Path, file_format: str):
        message = (
            f"Mismatch between amethyst_facet.h5.Reader of type {class_type} and file format '{file_format}' of {path}. "
            f"This error may occur by attempting to convert a V2 file with 'facet convert', or by attempting to perform "
            f"other operations such as 'facet agg' on a V1 file. Make sure to convert V1 files using 'facet convert' before "
            f"running other facet operations on them."
        )
        super().__init__(message)


@dc.dataclass
class Reader:
    paths: List[str | Path] = dc.field(default_factory=list)
    skip: Dict[str, Set] = dc.field(default_factory=dict)
    only: Dict[str, Set] = dc.field(default_factory=dict)
    mode: str = "a"
    reader_type: str = "Reader"

    def __post_init__(self):
        for k in self.skip:
            if self.skip[k] is None:
                self.skip[k] = set()
        for k in self.only:
            if self.only[k] is None:
                self.only[k] = set()

    def obtain(self, item: h5py.Dataset):
        if isinstance(item, h5py.Dataset):
            return item.file.filename, item.name, item[:]
        else:
            return item
    
    def not_implemented_error(self):
        raise NotImplementedError("Cannot use amethyst_facet.h5.Reader class directly -- use ReaderV1 or ReaderV2")

    def read(
            self, 
            file_or_group: h5py.File | h5py.Group, 
            level: str,
            ignore: Callable = lambda x: False
            ) -> Generator[h5py.Group | h5py.Dataset, None, None]:

        logging.debug(f"Reader.read(file_or_group={file_or_group}, level={level})")
        skip = set(self.skip.get(level, set())) or set()
        only = set(self.only.get(level, set())) or set()
        logging.debug(f"{self.reader_type} reading from {level} {file_or_group}")
        if only:
            only = only.difference(skip)
            logging.debug(f"only={only}\n")
            for only_item in only:
                present = only_item in file_or_group
                
                if present:
                    ignore_it = ignore(file_or_group[only_item])
                else:
                    ignore_it = True
                if present and not ignore_it:
                    logging.debug(f"Yielding {level} {file_or_group.file.filename}::{file_or_group[only_item].name}")
                    yield self.obtain(file_or_group[only_item])
                elif present:
                    logging.debug(f"Skipped {file_or_group.file.filename}::{file_or_group[only_item].name} (present: {present}, ignored: {ignore_it})")
                elif not present:
                    logging.debug(f"Skipped {file_or_group.file.filename}::{only_item} (present: {present}, ignored: {ignore_it})")
        else:
            for h5_item in file_or_group:
                
                not_skipped = h5_item not in skip
                ignore_it = ignore(file_or_group[h5_item])
                if not_skipped and not ignore_it:
                    logging.debug(f"Yielding {level} {file_or_group.file.filename}::{file_or_group[h5_item].name}")
                    yield self.obtain(file_or_group[h5_item])
                else:
                    logging.debug(f"Skipped {file_or_group.file.filename}::{file_or_group[h5_item].name} (not_skipped: {not_skipped}, ignored: {ignore_it})")

    def is_observations(self, dataset: h5py.Dataset) -> bool:
        return isinstance(dataset, h5py.Dataset) and all(col in dataset.dtype.names for col in ["chr", "pos"])

    def is_windows(self, dataset: h5py.Dataset) -> bool:
        return isinstance(dataset, h5py.Dataset) and all(col in dataset.dtype.names for col in ["chr", "start", "end"])

    def file_contexts(self, file: h5py.File):
        def ignore(it):
            if not isinstance(it, h5py.Group):
                return f"not h5py.Group (type={type(it)})"
            elif it.name == "/metadata":
                return f"name=/metadata"
            return False

        yield from self.read(file, "contexts", ignore)

    def context_barcodes(self, context: h5py.Group):
        def ignore(it):
            if isinstance(it, h5py.Dataset) and type(self) == fct.h5.ReaderV2:
                logging.debug("Raising V1/ReaderV2 mismatch")
                raise ReaderFileMismatch(type(self), it.file.filename, "V2")
            elif isinstance(it, h5py.Group) and type(self) == fct.h5.ReaderV1:
                logging.debug("Raising V2/ReaderV1 mismatch")
                raise ReaderFileMismatch(type(self), it.file.filename, "V1")
            if not isinstance(it, h5py.Group):
                return f"not h5py.Group (type={type(it)})"
            return False
        yield from self.read(context, "barcodes", ignore)

    def contexts(self) -> Generator[h5py.Group, None, None]:
        for path in set(self.paths):
            with fct.h5.open(path, mode=self.mode) as file:
                yield from self.file_contexts(file)

    def barcode_observations(self, barcode: h5py.Group):
        self.not_implemented_error()

    def barcode_windows(self, barcode: h5py.Group):
        self.not_implemented_error()

    def barcodes(self) -> Generator[h5py.Group, None, None]:
        self.not_implemented_error()

    def observations(self) -> Generator[Dataset, None, None]:
        self.not_implemented_error()

    def windows(self) -> Generator[Dataset, None, None]:
        self.not_implemented_error()