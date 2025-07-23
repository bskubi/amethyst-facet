import dataclasses as dc
from pathlib import Path
from typing import *
import warnings

import duckdb
import polars as pl

from .windows_aggregator import WindowsAggregator

import amethyst_facet as fct

class VariableWindowAggregatorException(Exception):
    def __init__(self, message: str):
        message = f"Problem with variable windows aggregation.\n{message}"
        super().__init__(message)

class InvalidVariableWindows(VariableWindowAggregatorException):
    def __init__(self, path: str | Path, message: str):

        message = f"Windows at path {path} {message}\n" if path is not None else f"Windows {message}\n"
        message += (
            f"\nVariable windows schema:\n"
            f"A columnar plaintext format (i.e. CSV, TSV) with a header including column names 'chr', 'start', 'end'."
            f"Observations with window.chr == obs.chr and window.start <= obs.pos < window.end are summed for the window."
            f"Window sizes (end-start) must be positive."
        )
        super().__init__(message)

class InvalidHeader(InvalidVariableWindows):
    def __init__(self, path: str | Path, columns: List[str]):
        message = f"had invalid columns {columns}."
        super().__init__(path, message)

class InvalidWindowSize(InvalidVariableWindows):
    def __init__(self, path: str | Path, invalid_rows: pl.DataFrame):
        message = f"had window sizes <= 0. Invalid rows:\n{invalid_rows}"
        super().__init__(path, message)

class InvalidColumn(InvalidVariableWindows):
    def __init__(self, path: str | Path, column: str):
        message = f"had non-numeric values in column '{column}'"
        super().__init__(path, message)

class DuplicateWindows(InvalidVariableWindows):
    def __init__(self, path: str | Path, duplicates: pl.DataFrame):
        message = f"had duplicate rows:\n{duplicates}"
        super().__init__(path, message)

class NullWindows(InvalidVariableWindows):
    def __init__(self, path: str | Path, nulls: pl.DataFrame):
        message = f"had rows containing null values:\n{nulls}"
        super().__init__(path, message)

@dc.dataclass
class VariableWindowsAggregator(WindowsAggregator):
    name: str
    path: str | Path = None
    windows: pl.DataFrame = None
    

    def check_header(self):
        if not all(col in self.windows.columns for col in ["chr", "start", "end"]):
            raise InvalidHeader(self.path, self.windows.columns)
   
    def check_empty(self):
        if self.windows.is_empty():
            warnings.warn(f"Empty windows for variable windows at {self.path}.")

    def check_null(self):
        null_count = self.windows.null_count()
        no_nulls = null_count.filter(pl.any_horizontal(pl.all() > 0)).is_empty()

        if not no_nulls:
            nulls = self.windows.filter(pl.any_horizontal(pl.all().is_null()))
            raise NullWindows(self.path, nulls)

    def check_numeric(self):
        if not self.windows["start"].dtype.is_numeric():
            raise InvalidColumn(self.path, "start")
        if not self.windows["end"].dtype.is_numeric():
            raise InvalidColumn(self.path, "end")

    def check_widths(self):
        invalid_rows = self.windows.filter(pl.col.end - pl.col.start <= 0)
        if not all(self.windows["end"] - self.windows["start"] > 0):
            raise InvalidWindowSize(self.path, invalid_rows)

    def check_duplicate(self):
        if len(self.windows.unique()) < len(self.windows):
            duplicates = self.windows.group_by("chr", "start", "end")
            duplicates = duplicates.agg(count = pl.len())
            duplicates = duplicates.filter(pl.col.count > 1)
            raise DuplicateWindows(self.windows, duplicates)

    def check_chroms(
            self, 
            dataset: fct.h5.Dataset,
            values_chrom: pl.Series,
            windows_chrom: pl.Series
        ):
        values_chrom: set = set(values_chrom.unique().to_list())
        windows_chrom: set = set(windows_chrom.unique().to_list())
        common_chroms = values_chrom.intersection(windows_chrom)
        if not common_chroms:
            values_chrom = sorted(list(values_chrom))
            windows_chrom = sorted(list(windows_chrom))
            message = (
                f"No common chromosome names between observations at {dataset.display_path} "
                f"and windows at {self.path}.\n"
                f"Observation chromosome names: {values_chrom}\n"
                f"Windows chromosome names: {windows_chrom}\n"
                f"This often occurs when one set of chromosome names are formatted like 'chr1', 'chr2', etc. "
                f"and the other set is formatted like '1', '2', etc."
            )
            warnings.warn(message)


    def __post_init__(self):
        if self.path is not None and self.windows is None:
            self.path = Path(self.path)
            self.windows = duckdb.read_csv(self.path, header=True).pl()
            self.check_header()
        
        self.windows = self.windows.select("chr", "start", "end")
        schema = {"chr": pl.String, "start": pl.Int64, "end": pl.Int64}
        self.windows = self.windows.cast(schema)

        self.check_empty()
        self.check_null()
        self.check_numeric()
        self.check_widths()
        self.check_duplicate()

    def aggregate(
            self,
            observations: fct.h5.Dataset
        ) -> fct.h5.Dataset:
        values = self.clean_values(observations.pl())
        self.check_chroms(observations, values["chr"], self.windows["chr"])
        values = self.windows.join_where(
            values,
            pl.col("chr") == pl.col("chr_right"),
            pl.col("start") <= pl.col("pos"),
            pl.col("end") > pl.col("pos")
        )
        values = values.select("chr", "start", "end", "c", "t")
        
        values = self._group_agg_sort(values)

        result = fct.h5.Dataset(
            observations.context,
            observations.barcode,
            self.name,
            values,
            observations.path
        )

        return result

