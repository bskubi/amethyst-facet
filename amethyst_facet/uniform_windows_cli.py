import dataclasses as dc
import itertools
from typing import *

import parse
import polars as pl

import amethyst_facet as fct

@dc.dataclass
class UniformWindows:
    size: int
    step: int | None = None
    offset: int = 1

    def __post_init__(self):
        if self.step is None:
            self.step = self.size
        err_msg = f"Invalid UniformWindows {self}"
        assert self.size > 0, f"{err_msg}. Window size must be a positive integer."
        assert self.size % self.step == 0, f"{err_msg}. Window size must be divisible by step."

    def aggregate(
        self, 
        values: pl.DataFrame,
        agg = {"c": pl.sum, "t": pl.sum, "c_nz": pl.sum, "t_nz": pl.sum},
        start = "start",
        end = "end",
        pos = "pos",
        positive_cols = {"c": "c_nz", "t": "t_nz"},
        start_min = None,
        end_min = None
        ) -> pl.DataFrame:
        """Compute aggregations on all chromosomes

        Returns --\n
        polars DataFrame starting with position columns 'chr', 'start', 'end',
        sorted in order of these position columns.
        Remaining columns are value columns with chosen aggregations.
        """

        try:
            values = values.partition_by("chr", as_dict = True)
            values = {k[0]: v for k, v in values.items()}
        except:
            raise Exception(f"Failed to partition values by column 'chr'. Values:\n{values}")
        
        result = []
        for chrom, chrom_values in values.items():
            chrom_result = fct.aggregate_uniform(
                size = self.size, 
                step = self.step, 
                offset = self.offset, 
                values = chrom_values,
                agg = agg,
                start = start,
                end = end,
                pos = pos,
                positive_cols = positive_cols,
                start_min = start_min,
                end_min = end_min
            )
            cols = ["chr"] + chrom_result.columns
            chrom_result = chrom_result.with_columns(chr = pl.lit(chrom))
            chrom_result = chrom_result.select(cols)
            result.append(chrom_result)

        try:
            result = pl.concat(result)
        except:
            raise Exception(f"Failed to concatenate per-chrom variable window aggregation results with result:\n{result}\nWindows:\n{self.windows}\nValues:\n{values}")

        try:
            result = result.sort("chr", "start", "end")
        except:
            raise Exception(f"Failed to sort per-chrom variable window aggregation results by 'chr', 'start', 'end' with result:\n{result}\nWindows:\n{self.windows}\nValues:\n{values}")

        return result


def parse_uniform_windows(arg: str) -> UniformWindows:
    parser = fct.UniformWindowsParser(arg)
    name = parser.name
    windows = parser.windows
    return name, windows

