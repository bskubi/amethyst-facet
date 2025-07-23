import dataclasses as dc
from typing import *
from pathlib import Path
import warnings

import h5py
import numpy as np
from numpy.typing import NDArray
import pandas as pd
import polars as pl

observations_v1_dtype = [("chr", "S10"), ("pos", int), ("pct", float), ("c", int), ("t", int)]
observations_v2_dtype = [("chr", "S10"), ("pos", int), ("c", int), ("t", int)]
observations_dtype = observations_v2_dtype
windows_dtype = [("chr", "S10"), ("start", int), ("end", int), ("c", int), ("t", int), ("c_nz", int), ("t_nz", int)]

import amethyst_facet as fct

@dc.dataclass
class Dataset:
    context: str
    barcode: str
    name: str
    data: NDArray | pl.DataFrame
    path: str | Path = ""

    def __post_init__(self):
        if isinstance(self.data, pl.DataFrame):
            self.data = self.data.to_numpy(structured=True)
            if self.format == "obsv1":
                self.data = self.datav1
            elif self.format == "obsv2":
                self.data = self.datav2
            elif self.format == "windows":
                self.data = self.data.astype(windows_dtype)

    def pl(self):
        return pl.from_numpy(self.data)
    
    def pd(self):
        return pd.DataFrame(self.data)
    
    def check_version(self, path: str | Path):
        version = None
        try:
            version = fct.h5.read_version(path)
            assert fct.h5.version_match(path)
        except:
            warnings.warn(f"Amethyst H5 file {path} version='{version}'")

    @property
    def format(self) -> str:
        if "pos" in self.data.dtype.names and "pct" in self.data.dtype.names:
            return "obsv1"
        elif "pos" in self.data.dtype.names and "pct" not in self.data.dtype.names:
            return "obsv2"
        elif "start" in self.data.dtype.names and "end" in self.data.dtype.names:
            return "windows"

    @property
    def datav1(self) -> NDArray:
        data = self.data
        if self.format == "obsv2":
            df = pl.from_numpy(data)
            df = df.with_columns(pct = pl.col.c / (pl.col.c + pl.col.t))
            df = df.select("chr", "pos", "pct", "c", "t")
            data = df.to_numpy(structured=True)
            data = data.astype(observations_v1_dtype)
        return data

    @property
    def datav2(self) -> NDArray:
        data = self.data
        if self.format == "obsv1":
            df = pl.from_numpy(data)
            df = df.select("chr", "pos", "c", "t")
            data = df.to_numpy(structured=True)
            data = data.astype(observations_v2_dtype)
        return data

    def write(self, path: str | Path | None = None, compression: str | None = "gzip", compression_opts: Any | None = 6):
        path = Path(path) if path else self.path
        exists = path.exists()
        with fct.h5.open(path) as file:
            if not exists:
                # Only add metadata version to file when it's first created.
                fct.h5.write_version(path)

            file.create_dataset(self.h5path, data=self.datav2, compression=compression, compression_opts=compression_opts)
            self.check_version(path)

    def writev1(self, path: str | Path | None = None, how = "barcode", compression: str | None = "gzip", compression_opts: Any | None = 6):
        path = Path(path) if path else self.path
        with fct.h5.open(path) as file:
            h5v1path = f"/{self.context}/{getattr(self, how)}"
            file.create_dataset(h5v1path, data=self.datav1, compression=compression, compression_opts=compression_opts)

    def writev2(self, path: str | Path | None = None, compression: str | None = "gzip", compression_opts: Any | None = 6):
        path = Path(path) if path else self.path
        self.write(path, compression, compression_opts)

    @property
    def h5path(self):
        return f"/{self.context}/{self.barcode}/{self.name}"
    
    @property
    def display_path(self):
        if self.path:
            return f"{self.path}::{self.h5path}"
        else:
            return self.h5path
    
    def __eq__(self, other: "Dataset") -> bool:
        return all([
            self.context == other.context,
            self.barcode == other.barcode,
            self.name == other.name,
            np.array_equal(self.data, other.data)
        ])