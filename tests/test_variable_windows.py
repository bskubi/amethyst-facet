import polars as pl
import pytest
import amethyst_facet as fct

def test_parse_variable_windows_filename():
    arg = "tests/assets/windows.tsv"
    name, result = fct.parse_variable_windows(arg)
    expected = pl.DataFrame({
        "chr":["chr1", "chr1", "chr2", "chr2"],
        "start": [1, 21, 1, 21],
        "end": [10, 30, 10, 30]
    })
    assert name == "windows", f"In {result}, {name} != 'windows'"
    assert result.equals(expected), f"{result} != {expected}"

def test_parse_variable_windows_name_filename():
    arg = f"test=tests/assets/windows.tsv"
    name, result = fct.parse_variable_windows(arg)

    expected = pl.DataFrame({
        "chr":["chr1", "chr1", "chr2", "chr2"],
        "start": [1, 21, 1, 21],
        "end": [10, 30, 10, 30]
    })
    assert name == "test", f"In {result}, {name} != 'test'"
    assert result.equals(expected), f"{result} != {expected}"

def test_parse_variable_windows_empty():
    arg = ""
    with pytest.raises(Exception, match="Failed to parse variable windows CLI argument"):
        fct.parse_variable_windows(arg)

def test_parse_variable_windows_nonexistant():
    arg = "tests/assets/nonexistant.tsv"
    with pytest.raises(Exception, match="file does not exist"):
        fct.parse_variable_windows(arg)

def test_parse_variable_windows_badfile():
    arg = "tests/assets/test.h5"
    with pytest.raises(Exception, match="Unable to load windows"):
        fct.parse_variable_windows(arg)

def test_parse_variable_windows_badcols():
    arg = "tests/assets/bad_columns.tsv"
    with pytest.raises(Exception, match="Unable to load windows"):
        fct.parse_variable_windows(arg)

def test_parse_variable_windows_badname():
    arg = " =tests/assets/.tsv"
    with pytest.raises(Exception, match="failed to parse name"):
        fct.parse_variable_windows(arg)