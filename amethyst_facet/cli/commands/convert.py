from loguru import logger
import click

from ..parse import CLIOptionsParser
from ..decorators import *

class AmethystH5Converter:
    def convert(self, globs, observations, windows, only_contexts, only_barcodes, skip_barcodes, compression, compression_opts, h5_out, h5_in):
        import amethyst_facet as fct
        parser = CLIOptionsParser()
        compression, compression_opts = parser.parse_h5py_compression(compression, compression_opts)
        paths = parser.combine_paths_globs(h5_in, globs)
        only_barcodes = parser.read_barcode_file(only_barcodes)
        skip_barcodes = parser.read_barcode_file(skip_barcodes)

        v1reader = fct.h5.ReaderV1(paths, skip={"barcodes":skip_barcodes}, only={"contexts": only_contexts, "barcodes":only_barcodes}, mode="r")

        for observations in v1reader.observations():
            observations.writev2(h5_out, compression, compression_opts)
        for windows in v1reader.windows():
            windows.writev2(h5_out, compression, compression_opts)

@click.command
@input_globs
@click.option(
    "--observations", "--name", "--default-name", "-n",
    default="1",
    type=str,
    show_default=True,
    help="Observations will be stored in V2 file at /context/barcode/[observations]."
    )
@click.option(
    "--windows", "-w",
    default="windows",
    type=str,
    show_default=True,
    help="Windows will be stored in V2 file at /context/barcode/[windows]."
    )
@h5_subsets
@compression
@click.argument("h5_out")
@click.argument("h5_in", nargs=-1)
def convert(globs, observations, windows, only_contexts, only_barcodes, skip_barcodes, compression, compression_opts, h5_out, h5_in):
    """Convert one or more old Amethyst HDF5 file format to v2.0.0 format.

    The V1 format stores bp-level observations as (chr, pos, pct, c, t) in an HDF5 dataset at /context/barcode.
    The V2 format stores base-pair observations as (chr, pos, c, t) in an HDF5 dataset at /context/barcode/1.
    1 is the conventional name for the bp-level unaggregated observations.
    The V2 format stores window aggregations in a dataset at /context/barcode/[window_dataset_name] 
    as (chr, start, end, c, t, c_nz, t_nz), with c_nz and t_nz being the count of nonzero observations in the window.
    It also contains a dataset /metadata/version='amethyst2.0.0'.

    If more than one input file is specified in [H5_IN] and/or via the -g option, they are all appended to [H5_OUT].
    If the same /context/barcode dataset is found in two or more input files, the conversion fails.
    """
    converter = AmethystH5Converter()
    converter.convert(globs, observations, windows, only_contexts, only_barcodes, skip_barcodes, compression, compression_opts, h5_out, h5_in)