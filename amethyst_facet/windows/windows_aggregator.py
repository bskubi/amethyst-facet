from typing import *

import polars as pl
import amethyst_facet as fct
from amethyst_facet.h5 import Dataset

class WindowsAggregator:

    def aggregate(
            self,
            observations: Dataset
        ) -> Dataset:
        raise NotImplementedError("Use a UniformWindowAggregator or VariableWindowAggregator subclass")

    def clean_values(
            self,
            values: pl.DataFrame
    ) -> pl.DataFrame:
        values = values.select("chr", "pos", "c", "t")
        values = values.fill_nan(0)
        values = values.fill_null(0)
        values_schema = {"chr": pl.String, "pos": pl.Int64(), "c": pl.Int64, "t": pl.Int64}
        values = values.cast(values_schema)
        return values

    def _group_agg_sort(
            self,
            values: pl.DataFrame,
            aggregations: Dict[str, Any] = {"c": pl.sum, "t": pl.sum, "c_nz": pl.sum, "t_nz": pl.sum}
        ) -> pl.DataFrame:
        values = values.with_columns(
            c_nz = (pl.col.c > 0).cast(pl.Int64),
            t_nz = (pl.col.t > 0).cast(pl.Int64)
        )
        values = values.group_by("chr", "start", "end")
        aggregations = [agg(col) for col, agg in aggregations.items()]
        values = values.agg(*aggregations)
        values = values.sort("chr", "start", "end")
        return values
