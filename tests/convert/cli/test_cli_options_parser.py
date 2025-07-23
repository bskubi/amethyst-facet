import pytest

import amethyst_facet as fct

def test_cli_options_parser_compression():
    parser = fct.cli.CLIOptionsParser()
    compression = "gzip"
    compression_opts = "6"
    compression, compression_opts = parser.parse_h5py_compression(compression, compression_opts)
    assert compression_opts == 6, f"Failed to make valid cast to int for '{compression_opts}'"

def test_cli_options_parser_no_compression():
    parser = fct.cli.CLIOptionsParser()
    compression = ""
    compression_opts = ""
    compression, compression_opts = parser.parse_h5py_compression(compression, compression_opts)
    assert compression is None and compression_opts is None, (
        f"Failed to convert compression and compression_opts to None "
        f"for compression='{compression}', compression_opts='{compression_opts}'"
    )

def test_cli_options_parser_no_compression_opts():
    parser = fct.cli.CLIOptionsParser()
    compression = "gzip"
    compression_opts = ""
    compression, compression_opts = parser.parse_h5py_compression(compression, compression_opts)
    assert compression == "gzip" and compression_opts is None, (
        f"Failed to convert compression_opts to None "
        f"for compression='{compression}', compression_opts='{compression_opts}'"
    )

def test_cli_options_parser_compression_unspecified():
    parser = fct.cli.CLIOptionsParser()
    compression = ""
    compression_opts = "6"
    with pytest.raises(fct.cli.InvalidCompressionArgs):
        parser.parse_h5py_compression(compression, compression_opts)

def test_cli_options_parser_invalid_compression_opts():
    parser = fct.cli.CLIOptionsParser()
    compression = "gzip"
    compression_opts = "invalid"
    with pytest.raises(fct.cli.InvalidCompressionArgs):
        parser.parse_h5py_compression(compression, compression_opts)