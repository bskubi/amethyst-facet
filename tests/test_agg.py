import polars as pl
import amethyst_facet as fct

def test_aggregate_variable():
    starts = [0, 4, 6, 9]
    ends = [2, 5, 8, 12]
    windows = pl.DataFrame({"start": starts, "end": ends})
    positions = [-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
    c =         [ 1, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3,   3,  4,  4]
    t =         [ 1, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3,   3,  4,  4]
    values = pl.DataFrame({"pos": positions, "c": c, "t": t})
    result = fct.aggregate_variable(windows, values)
    expected = pl.DataFrame({
        "start": [0, 4, 6, 9],
        "end": [2, 5, 8, 12],
        "c": [0, 1, 4, 9],
        "t": [0, 1, 4, 9],
        "c_nz": [0, 1, 2, 3],
        "t_nz": [0, 1, 2, 3]
    })
    assert result.equals(expected), f"{result} != {expected}"

def test_aggregate_uniform():
    positions = [-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
    c =         [ 1, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3,   3,  4,  4]
    t =         [ 1, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3,   3,  4,  4]
    values = pl.DataFrame({"pos": positions, "c": c, "t": t})
    result = fct.aggregate_uniform(size=2, values = values, offset = 1, step = 1, start_min = 1, end_min = 2)
    expected = pl.DataFrame({
        "start": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
        "end":   [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        "c":     [0, 1, 2, 2, 3, 4, 4, 5, 6, 6, 7, 8, 4],
        "t":     [0, 1, 2, 2, 3, 4, 4, 5, 6, 6, 7, 8, 4],
        "c_nz":  [0, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1],
        "t_nz":  [0, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1]
    })
    assert result.equals(expected), f"{result} != {expected}"

def test_aggregate_na():
    N = None
    positions = [-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
    c =         [ 1, 0, N, 0, 1, N, 1, 2, 2, 2, 3, 3,   3,  4,  4]
    t =         [ 1, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3,   3,  4,  4]
    values = pl.DataFrame({"pos": positions, "c": c, "t": t})
    result = fct.aggregate_uniform(size=2, values = values, offset = 1, step = 1, start_min = 1, end_min = 2)
    expected = pl.DataFrame({
        "start": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
        "end":   [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        "c":     [0, 1, 1, 1, 3, 4, 4, 5, 6, 6, 7, 8, 4],
        "t":     [0, 1, 2, 2, 3, 4, 4, 5, 6, 6, 7, 8, 4],
        "c_nz":  [0, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 1],
        "t_nz":  [0, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1]
    })
    assert result.equals(expected), f"{result} != {expected}"