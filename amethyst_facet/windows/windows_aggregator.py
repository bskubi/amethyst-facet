from typing import *

import polars as pl
import amethyst_facet as fct
from amethyst_facet.h5 import Dataset

class WindowsAggregator:

    def window(
            self,
            observations: Dataset
        ) -> Dataset:
        raise NotImplementedError("Use a UniformWindowAggregator or VariableWindowAggregator subclass")

    def aggregate(
            self,
            values: pl.DataFrame,
            aggregations: Dict[str, Any] = {"c": pl.sum, "t": pl.sum, "c_nz": pl.sum, "t_nz": pl.sum}
        ) -> pl.DataFrame:
        values = values.with_columns(
            c_nz = (pl.col.c > 0).cast(pl.Int64),
            t_nz = (pl.col.t > 0).cast(pl.Int64)
        )
        values = values.group_by("chr", "start", "end")
        aggregations = [agg(col) for col, agg in aggregations]
        values = values.agg(*aggregations)
        values = values.sort("chr", "start", "end")
        return values
