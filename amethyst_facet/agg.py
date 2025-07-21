from typing import *
import polars as pl

def add_positive_cols(values, positive_cols):
    positive_columns = []
    for k, v in positive_cols.items():
        positive_columns.append((pl.col(k) > 0).alias(v))
    if positive_columns:
        values = values.with_columns(*positive_columns)
    return values

def aggregate(df, agg):
    aggregations = [agg[k](k) for k in agg]
    df = df.agg(*aggregations)
    return df

def validate_uniform_windows(
        size: int,
        step: int
    ):
    if size <= 0:
        raise ValueError(f"Window size was {size} but must be a positive integer.")
    if size % step != 0:
        raise ValueError(f"Could not aggregate uniform windows as size {size} is not divisible by step {step}.")

def aggregate_uniform_stride(
        size: int, 
        offset: int, 
        stride: int, 
        values: pl.DataFrame, 
        agg: Dict, 
        start: str,
        pos: str,
        ) -> pl.DataFrame:

    stride_offset = stride + offset
    result = values.with_columns(
        ((pl.col(pos) - stride_offset) // size * size + stride_offset).alias(start)
    )
    result = result.group_by(start)
    result = aggregate(result, agg)

    return result

def aggregate_uniform(
        size: int, 
        values: pl.DataFrame, 
        agg = {"c": pl.sum, "t": pl.sum, "c_nz": pl.sum, "t_nz": pl.sum},
        start = "start",
        end = "end",
        pos = "pos",
        offset: int = 1, 
        step: int = None, 
        positive_cols = {"c": "c_nz", "t": "t_nz"},
        start_min = None,
        end_min = None
    ) -> pl.DataFrame:
    step = step or size
    validate_uniform_windows(size, step)
    values = add_positive_cols(values, positive_cols)

    results = []
    for stride in range(0, size, step):
        result = aggregate_uniform_stride(size, offset, stride, values, agg, start, pos)
        results.append(result)
    results = pl.concat(results)

    results = results.sort(start)
    results = results.with_columns(
        (pl.col.start + size).alias(end)
    )

    value_cols = [c for c in results.columns if c not in [start, end]]
    results = results.select(start, end, *value_cols)
    if start_min is not None:
        results = results.filter(pl.col(start) >= start_min)
    if end_min is not None:
        results = results.filter(pl.col(end) >= end_min)

    return results

def aggregate_variable(
        windows: pl.DataFrame, 
        values: pl.DataFrame, 
        agg = {"c": pl.sum, "t": pl.sum, "c_nz": pl.sum, "t_nz": pl.sum},
        start = "start",
        end = "end",
        pos = "pos",
        positive_cols = {"c": "c_nz", "t": "t_nz"}

    ) -> pl.DataFrame:
    values = add_positive_cols(values, positive_cols)
    result = windows.join_where(values, pl.col(start) <= pl.col(pos), pl.col(end) > pl.col(pos))
    result = result.group_by(start, end)
    result = aggregate(result, agg)
    result = result.sort(start, end)
    return result