import pytest
import amethyst_facet as fct

def test_uniform_windows_parser():
    parser = fct.cli.UniformWindowsParser()
    result = parser.parse("10")

    expected = fct.windows.UniformWindowsAggregator(10, 10, 1, "10:10+1")
    assert result == expected, f"{expected} != {result}"

def test_uniform_windows_parser_name_size():
    parser = fct.cli.UniformWindowsParser()
    result = parser.parse("test=10")

    expected = fct.windows.UniformWindowsAggregator(10, 10, 1, "test")
    assert result == expected, f"{expected} != {result}"

def test_uniform_windows_parser_name_size_step():
    parser = fct.cli.UniformWindowsParser()
    result = parser.parse("test=10:5")

    expected = fct.windows.UniformWindowsAggregator(10, 5, 1, "test")
    assert result == expected, f"{expected} != {result}"

def test_uniform_windows_parser_name_size_step_offset():
    parser = fct.cli.UniformWindowsParser()
    result = parser.parse("test=10:5+0")

    expected = fct.windows.UniformWindowsAggregator(10, 5, 0, "test")
    assert result == expected, f"{expected} != {result}"

def test_uniform_windows_parser_misformatted():
    parser = fct.cli.UniformWindowsParser()

    with pytest.raises(fct.cli.UniformWindowsParseFailed):
        parser.parse("=10:5+0")

def test_uniform_windows_parser_noninteger_size():
    parser = fct.cli.UniformWindowsParser()

    with pytest.raises(fct.cli.FailedCastToInt):
        parser.parse("invalid_size")

def test_uniform_windows_parser_noninteger_step():
    parser = fct.cli.UniformWindowsParser()

    with pytest.raises(fct.cli.FailedCastToInt):
        parser.parse("10:invalid_step")

def test_uniform_windows_parser_nonpositive_size():
    parser = fct.cli.UniformWindowsParser()

    with pytest.raises(fct.cli.InvalidSize):
        parser.parse("0:3")

def test_uniform_windows_parser_nonpositive_step():
    parser = fct.cli.UniformWindowsParser()

    with pytest.raises(fct.cli.InvalidStep):
        parser.parse("10:-10")

def test_uniform_windows_parser_nondivisible_step():
    parser = fct.cli.UniformWindowsParser()

    with pytest.raises(fct.cli.InvalidStep):
        parser.parse("10:3")

def test_uniform_windows_parser_bad_offset():
    parser = fct.cli.UniformWindowsParser()
    with pytest.raises(fct.cli.FailedCastToInt):
        parser.parse("10+invalid_offset")

def test_uniform_windows_parser_empty():
    parser = fct.cli.UniformWindowsParser()
    with pytest.raises(fct.cli.UniformWindowsParserException):
        parser.parse("")

def test_uniform_windows_parser_whitespace_name():
    parser = fct.cli.UniformWindowsParser()
    with pytest.raises(fct.cli.InvalidUniformWindowsName):
        parser.parse(" =10")