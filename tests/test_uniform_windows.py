import pytest
import amethyst_facet as fct

def test_parse_uniform_windows_size():
    cli = "10"
    name, result = fct.parse_uniform_windows(cli)
    expected = fct.UniformWindows(10, 10, 1)
    assert name == "10"
    assert result == expected, f"{expected} != {result}"

def test_parse_uniform_windows_name_size():
    cli = "test=10"
    name, result = fct.parse_uniform_windows(cli)
    expected = fct.UniformWindows(10, 10, 1)
    assert name == "test"
    assert result == expected, f"{expected} != {result}"

def test_parse_uniform_windows_name_size_step():
    cli = "test=10:5"
    name, result = fct.parse_uniform_windows(cli)
    expected = fct.UniformWindows(10, 5, 1)
    assert name == "test"
    assert result == expected, f"{expected} != {result}"

def test_parse_uniform_windows_name_size_step_offset():
    cli = "test=10:5+0"
    name, result = fct.parse_uniform_windows(cli)
    expected = fct.UniformWindows(10, 5, 0)
    assert name == "test"
    assert result == expected, f"{expected} != {result}"

def test_parse_uniform_windows_misformatted():
    cli = ":5+0"
    with pytest.raises(Exception, match="Failed to parse window CLI argument"):
        fct.parse_uniform_windows(cli)

def test_parse_uniform_windows_bad_size():
    cli = "size:5+0"
    with pytest.raises(Exception):
        fct.parse_uniform_windows(cli)

def test_parse_uniform_windows_bad_step():
    cli = "10:step+0"
    with pytest.raises(Exception):
        fct.parse_uniform_windows(cli)

def test_parse_uniform_windows_bad_offset():
    cli = "10:5+offset"
    with pytest.raises(Exception):
        fct.parse_uniform_windows(cli)

def test_parse_uniform_windows_empty():
    cli = ""
    with pytest.raises(Exception):
        fct.parse_uniform_windows(cli)

def test_parse_uniform_windows_zero_size():
    cli = "0"
    with pytest.raises(Exception):
        fct.parse_uniform_windows(cli)

def test_parse_uniform_windows_non_divisible():
    cli = "10:3"
    with pytest.raises(Exception):
        fct.parse_uniform_windows(cli)

def test_parse_uniform_windows_whitespace_name():
    cli = " =10"
    with pytest.raises(Exception):
        fct.parse_uniform_windows(cli)