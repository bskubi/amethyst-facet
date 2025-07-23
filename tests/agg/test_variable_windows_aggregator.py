import polars as pl

import amethyst_facet as fct

def test_variable_windows_aggregator_basic():
    chr =    ["1", "1", "2", "2"]
    starts = [0, 4, 6, 9]
    ends =   [2, 5, 8, 12]
    windows = pl.DataFrame({"chr": chr, "start": starts, "end": ends})

    chr =       [ 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2,  2,  2,  2,  2]
    positions = [-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
    c =         [ 1, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3,   3,  4,  4]
    t =         [ 1, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3,   3,  4,  4]
    values = pl.DataFrame({"chr": chr, "pos": positions, "c": c, "t": t})
    values = values.cast({"chr": pl.String})
    observations = fct.h5.Dataset("CG", "barcode1", "1", values)

    aggregator = fct.windows.VariableWindowsAggregator(name="test", windows=windows)
    result = aggregator.aggregate(observations).pl()
    expected = pl.DataFrame({
        "chr": ["1", "1", "2", "2"],
        "start": [0, 4, 6, 9],
        "end":   [2, 5, 8, 12],
        "c":     [0, 1, 2, 9],
        "t":     [0, 1, 2, 9],
        "c_nz":  [0, 1, 1, 3],
        "t_nz":  [0, 1, 1, 3]
    })
    assert result.equals(expected), f"{result} != {expected}"

