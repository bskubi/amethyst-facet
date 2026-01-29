import dataclasses as dc
from typing import *
from pathlib import Path
import warnings

import numpy as np
from numpy.typing import NDArray
import pandas as pd
import polars as pl
from loguru import logger

import amethyst_facet as fct

observations_v1_dtype = [("chr", "S10"), ("pos", "<i8"), ("pct", "<f8"), ("c", "<i8"), ("t", "<i8")]
observations_v2_dtype = [("chr", "S10"), ("pos", "<i8"), ("c", "<i8"), ("t", "<i8")]
observations_dtype = observations_v2_dtype
windows_dtype = [("chr", "S10"), ("start", "<i8"), ("end", "<i8"), ("c", "<i8"), ("t", "<i8"), ("c_nz", "<i8"), ("t_nz", "<i8")]

class DatasetException(Exception):
    def __init__(self, dataset: "Dataset", message: str):
        dataset: Dataset
        message = (
            f"Problem occurred for dataset with context='{dataset.context}', barcode='{dataset.barcode}', name='{dataset.name}', path='{dataset.path}': "
            f"{message}"
        )
        super().__init__(self)

class InvalidWindowsDtype(DatasetException):
    def __init__(self, dataset: "Dataset", dtype: List[Tuple[str, type]]):
        message = (
            f"Attempted invalid conversion from dtype {dtype} to dtype {windows_dtype}.\n"
        )
        super().__init__(dataset, message)

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
        for name in ["c", "t", "c_nz", "t_nz"]:
            if name in self.data.dtype.names:
                count = sum(np.isnan(self.data[name]))
                if count:
                    logger.info(
                        "{} nan values discovered in Dataset for {}. This will be converted to zero.",
                        count,
                        self
                    )

                self.data[name] = np.nan_to_num(self.data[name], nan=0)
        if self.format == "obsv1":
            self.data = self.datav1
        elif self.format == "obsv2":
            self.data = self.datav2
        elif self.format == "windows":
            self.data = self.windows

    def convert_dtype(self, dtype: List[Tuple[str, type]], from_df: pl.DataFrame = None):
        logger.debug(f"Converting to dtype {dtype}")
        data = from_df if from_df is not None else self.data
        if isinstance(data, pl.DataFrame) or data.dtype != dtype:
            if isinstance(data, pl.DataFrame):
                data = data.to_numpy(structured=True)
            new_data = np.zeros(data.shape, dtype=dtype)
            for name, _ in dtype:
                new_data[name] = data[name]
        else:
            new_data = data
        return new_data

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
        if self.format == "obsv1":
            data = self.convert_dtype(observations_v1_dtype, data)
        if self.format == "obsv2":
            data = pl.from_numpy(data)
            data = data.with_columns(pct = pl.col.c / (pl.col.c + pl.col.t))
            data = self.convert_dtype(observations_v1_dtype, data)
        elif self.format == "windows":
            data = self.convert_dtype(windows_dtype, data)
        return data

    @property
    def datav2(self) -> NDArray:
        data = self.data
        if self.format == "obsv1":
            data = self.convert_dtype(observations_v2_dtype, data)
        if self.format == "obsv2":
            data = self.convert_dtype(observations_v2_dtype, data)
        elif self.format == "windows":
            data = self.convert_dtype(windows_dtype, data)
        return data
    
    @property
    def windows(self) -> NDArray:
        data = self.data
        if self.format == "windows":
            data = self.convert_dtype(windows_dtype, data)
        else:
            raise InvalidWindowsDtype(self, data.dtype)
        return data

    def write(self, path: str | Path | None = None, compression: str | None = "gzip", compression_opts: Any | None = 6):
        path = Path(path) if path else self.path
        self.writev2(path, compression, compression_opts)

    def writev1(self, path: str | Path | None = None, how = "barcode", compression: str | None = "gzip", compression_opts: Any | None = 6):
        path = Path(path) if path else self.path
        with fct.h5.open(path) as file:
            h5v1path = f"/{self.context}/{getattr(self, how)}"
            logger.info("Writing data to {}::{}", file.filename, h5v1path)
            file.create_dataset(h5v1path, data=self.datav1, compression=compression, compression_opts=compression_opts)
            logger.info("Finished writing data to {}::{}", file.filename, h5v1path)

    def writev2(self, path: str | Path | None = None, compression: str | None = "gzip", compression_opts: Any | None = 6, display_sample = False):
        path = Path(path) if path else self.path
        exists = path.exists()
        
        with fct.h5.open(path) as file:
            if not exists:
                # Only add metadata version to file when it's first created.
                fct.h5.write_version(path)
            
            data = self.datav2
            logger.info("Writing data with dtype={} to {}::{}", data.dtype, file.filename, self.h5path)
            file.create_dataset(self.h5path, data=data, compression=compression, compression_opts=compression_opts)
            if display_sample:
                df = pl.from_numpy(file[self.h5path][:])
                with pl.Config(tbl_rows=100):
                    df_string = str(df)
                logger.info("First sample of current window schema as loaded from H5 file:\n{}", df_string)
            logger.debug("Finished writing data to {}::{}", file.filename, self.h5path)
            self.check_version(path)

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