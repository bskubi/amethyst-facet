import polars as pl

import amethyst_facet as fct

def test_uniform_windows_aggregator_basic():

    chr =       [ 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2,  2,  2,  2,  2]
    positions = [-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
    c =         [ 1, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3,   3,  4,  4]
    t =         [ 1, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3,   3,  4,  4]
    chr = [str(c) for c in chr]
    data = pl.DataFrame({"chr": chr, "pos": positions, "c": c, "t": t})
    observations = fct.h5.Dataset(
        "CG",
        "barcode1",
        "1",
        data
    )
    aggregator = fct.windows.UniformWindowsAggregator(size=2, step=1, offset=1, start_min=1, end_min=2)
    result = aggregator.aggregate(observations).pl()
    expected = pl.DataFrame({
        "chr":   [1, 1, 1, 1, 1, 1, 2, 2, 2, 2,  2,  2,  2,  2],
        "start": [1, 2, 3, 4, 5, 6, 6, 7, 8, 9, 10, 11, 12, 13],
        "end":   [3, 4, 5, 6, 7, 8, 8, 9, 10, 11, 12, 13, 14, 15],
        "c":     [0, 1, 2, 2, 3, 2, 2, 4, 5, 6, 6, 7, 8, 4],
        "t":     [0, 1, 2, 2, 3, 2, 2, 4, 5, 6, 6, 7, 8, 4],
        "c_nz":  [0, 1, 2, 2, 2, 1, 1, 2, 2, 2, 2, 2, 2, 1],
        "t_nz":  [0, 1, 2, 2, 2, 1, 1, 2, 2, 2, 2, 2, 2, 1]
    })
    expected = expected.cast({"chr": pl.String})
    pl.Config.set_tbl_rows(-1)
    assert result.equals(expected), f"{result} != {expected}"

def test_uniform_windows_aggregator_with_nulls():
    N = None
    chr =       [ 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2,  2,  2,  2,  2]
    positions = [-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
    c =         [ 1, N, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3,   3,  4,  4]
    t =         [ 1, N, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3,   3,  4,  4]
    chr = [str(c) for c in chr]

    data = pl.DataFrame({"chr": chr, "pos": positions, "c": c, "t": t})
    observations = fct.h5.Dataset(
        "CG",
        "barcode1",
        "1",
        data
    )
    aggregator = fct.windows.UniformWindowsAggregator(size=2, step=1, offset=1, start_min=1, end_min=2)
    result = aggregator.aggregate(observations).pl()
    expected = pl.DataFrame({
        "chr":   [1, 1, 1, 1, 1, 1, 2, 2, 2, 2,  2,  2,  2,  2],
        "start": [1, 2, 3, 4, 5, 6, 6, 7, 8, 9, 10, 11, 12, 13],
        "end":   [3, 4, 5, 6, 7, 8, 8, 9, 10, 11, 12, 13, 14, 15],
        "c":     [0, 1, 2, 2, 3, 2, 2, 4, 5, 6, 6, 7, 8, 4],
        "t":     [0, 1, 2, 2, 3, 2, 2, 4, 5, 6, 6, 7, 8, 4],
        "c_nz":  [0, 1, 2, 2, 2, 1, 1, 2, 2, 2, 2, 2, 2, 1],
        "t_nz":  [0, 1, 2, 2, 2, 1, 1, 2, 2, 2, 2, 2, 2, 1]
    })
    expected = expected.cast({"chr": pl.String})
    pl.Config.set_tbl_rows(-1)
    assert result.equals(expected), f"{result} != {expected}"