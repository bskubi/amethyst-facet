from pathlib import Path
from typing import *

import polars as pl
import numpy as np
import h5py

class AmethystFile:
    def __init__(self, file: h5py.File):
        self.file = file

    @property
    def path(self) -> Path:
        return Path(self.file.filename)

    def create_dataset(self, data: np.recarray, context: str, barcode: str, dataset: str = "1"):
        """Create Amethyst H5 dataset
        """
        pass

    def ingest_polars_dataset(self, df: pl.DataFrame):
        pass

class AmethystFileV1(AmethystFile):
    def create_dataset(self, data: np.recarray, context: str, barcode: str = None, dataset: str = "1"):
        pass

class AmethystFileV2(AmethystFile):
    def create_dataset(self, data: np.recarray, context: str, barcode: str, dataset: str = "1"):
        pass
        
class AmethystFileSchema:
    path: Path
    file_init_args: list
    file_init_kwargs: dict
    amethyst_file_type: AmethystFile

    def open(self) -> AmethystFile:
        handle = h5py.File(self.path, *self.file_init_args, **self.file_init_kwargs)
        return AmethystFile(handle)



class AmethystFileCollection:
    pass
