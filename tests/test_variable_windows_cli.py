import polars as pl
import pytest
import amethyst_facet as fct

def test_parse_variable_windows_filename():
    arg = "tests/assets/windows.tsv"
    name, result = fct.parse_variable_windows(arg)
    expected = {
        "chr1": pl.DataFrame({
            "chr": ["chr1", "chr1"],
            "start": [1, 21],
            "end": [10, 30]
            }),
        "chr2": pl.DataFrame({
            "chr": ["chr2", "chr2"],
            "start": [1, 21],
            "end": [10, 30]
            })
    }
    result_keys = sorted(list(result.windows.keys()))
    expected_keys = sorted(list(expected.keys()))
    assert result_keys == expected_keys, f"In {result}, mismatched key sets: {result_keys} != {expected_keys}"
    assert name == "windows", f"In {result}, {name} != 'windows'"

    for k in expected:
        assert result.windows[k].equals(expected[k]), f"In {result}, {result.windows[k]} != {expected[k]}"

def test_parse_variable_windows_name_filename():
    arg = f"test=tests/assets/windows.tsv"
    name, result = fct.parse_variable_windows(arg)
    expected = {
        "chr1": pl.DataFrame({
            "chr": ["chr1", "chr1"],
            "start": [1, 21],
            "end": [10, 30]
            }),
        "chr2": pl.DataFrame({
            "chr": ["chr2", "chr2"],
            "start": [1, 21],
            "end": [10, 30]
            })
    }
    result_keys = sorted(list(result.windows.keys()))
    expected_keys = sorted(list(expected.keys()))
    assert result_keys == expected_keys, f"In {result}, mismatched key sets: {result_keys} != {expected_keys}"
    assert name == name, f"In {result}, {name} != 'test'"

    for k in expected:
        assert result.windows[k].equals(expected[k]), f"In {result}, {result.windows[k]} != {expected[k]}"

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