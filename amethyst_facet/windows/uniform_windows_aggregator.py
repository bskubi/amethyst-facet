import dataclasses as dc
from .windows_aggregator import WindowsAggregator
import amethyst_facet as fct

import polars as pl

class UniformWindowsAggregatorException(Exception):
    def __init__(self, properties: str, message: str):
        message = f"Problem with uniform windows aggregation with {properties}:\n{message}"
        super().__init__(message)

class InvalidSize(UniformWindowsAggregatorException):
    def __init__(self, properties: str):
        message = "Size must be a positive integer."
        super().__init__(properties, message)

class InvalidStep(UniformWindowsAggregatorException):
    def __init__(self, properties: str):
        message = "Step must be a positive integer, and size must be divisible by step."
        super().__init__(properties, message)

@dc.dataclass
class UniformWindowsAggregator(WindowsAggregator):
    size: int
    step: int = None
    offset: int = 1
    name: str = None
    start_min: int | None = None
    end_min: int | None = None

    @property
    def properties(self) -> str:
        return f"size: {self.size} step: {self.step} offset: {self.offset} name: {self.name}"

    def __post_init__(self):
        if self.size <= 0:
            raise InvalidSize(self.properties)
        if not self.step:
            self.step = self.size
        if self.step <= 0 or self.size % self.step != 0:
            raise InvalidStep(self.properties)
        if self.name is None or not self.name.strip():
            self.name = f"{self.size}:{self.step}+{self.offset}"

    def aggregate(
            self,
            observations: fct.h5.Dataset
        ) -> fct.h5.Dataset:
        # Create empty dataframe to avoid error when concatenating
        windows_schema = {"chr": pl.String, "start": pl.Int64(), "end": pl.Int64, "c": pl.Int64, "t": pl.Int64}
        values_strides = [pl.DataFrame(schema=windows_schema)]
        values = self.clean_values(observations.pl())

        # Accumulate uniform windows at offsets determined by step
        for stride in range(0, self.size, self.step):
            offset = self.offset + stride
            windowed = values.with_columns(
                start = (pl.col.pos - offset) // self.size * self.size + offset
            )
            windowed = windowed.with_columns(
                end = pl.col.start + self.size
            )
            windowed = windowed.select("chr", "start", "end", "c", "t")
            values_strides.append(windowed)
        values = pl.concat(values_strides)

        # Compute aggregations
        values = self._group_agg_sort(values)

        # Remove negative values
        if self.start_min is not None and self.end_min is not None:
            values = values.filter((pl.col.start >= self.start_min) & (pl.col.end >= self.end_min))
        elif self.start_min is not None:
            values = values.filter(pl.col.start >= self.start_min)
        elif self.end_min is not None:
            values = values.filter(pl.col.end >= self.end_min)

        result = fct.h5.Dataset(
            observations.context,
            observations.barcode,
            self.name,
            values,
            observations.path
        )

        return result