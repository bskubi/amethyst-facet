import polars as pl
import amethyst_facet as fct

def test_uniform_windows_agg():
    windows = fct.UniformWindows(size=10, step=10, offset=1)
    values = pl.DataFrame(
        {
            "chr": ["1","1","1","2","2","2"],
            "pos": [1,2,11,1,2,11],
            "c": [1,1,1,1,1,1],
            "t": [1,1,1,1,1,1]
        }
    )
    result = windows.aggregate(values)
    expected = pl.DataFrame(
        {
            "chr": ["1","1","2","2"],
            "start": [1,11,1,11],
            "end": [11, 21, 11, 21],
            "c": [2,1,2,1],
            "t": [2,1,2,1],
            "c_nz": [2,1,2,1],
            "t_nz": [2,1,2,1]
        }
    )
    assert result.equals(expected), f"{result} != {expected}"

def test_variable_windows_agg():
    windows = pl.DataFrame({
        "chr": ["1", "1", "2", "2"],
        "start": [1, 11, 1, 11],
        "end": [11, 21, 11, 21]
    })
    values = pl.DataFrame(
        {
            "chr": ["1","1","1","2","2","2"],
            "pos": [1,2,11,1,2,11],
            "c": [1,1,1,1,1,1],
            "t": [1,1,1,1,1,1]
        }
    )
    aggregator = fct.VariableWindows()
    result = aggregator.aggregate(windows, values)
    expected = pl.DataFrame({
        "chr": ["1","1","2","2"],
        "start": [1,11,1,11],
        "end": [11, 21, 11, 21],
        "c": [2,1,2,1],
        "t": [2,1,2,1],
        "c_nz": [2,1,2,1],
        "t_nz": [2,1,2,1]
    })
    assert result.equals(expected), f"{result} != {expected}"