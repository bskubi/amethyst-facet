import logging
from typing import *
import warnings

import click

from ..parse import CLIOptionsParser, VariableWindowsParser, UniformWindowsParser
from ..decorators import *

class AmethystH5Aggregator():
    def aggregate(
        self,
        globs,
        only_observations, 
        only_contexts, 
        only_barcodes, 
        skip_barcodes, 
        variable_windows, 
        uniform_windows, 
        compression, 
        compression_opts, 
        h5_out, 
        h5_in
    ):
        import amethyst_facet as fct
        if not h5_in:
            warnings.warn("No paths supplied for [H5_IN], so no aggregations will be computed.")
        parser = CLIOptionsParser()
        paths = parser.combine_paths_globs(h5_in, globs)
        compression, compression_opts = parser.parse_h5py_compression(compression, compression_opts)

        parser = VariableWindowsParser()
        variable_windows = [parser.parse(arg) for arg in variable_windows]

        parser = UniformWindowsParser()
        uniform_windows = [parser.parse(arg) for arg in uniform_windows]

        windows = variable_windows + uniform_windows

        if not windows:
            warnings.warn("No window schemes supplied, so no aggregations will be computed.")

        skip = {"barcodes":skip_barcodes}
        only = {
            "observations": only_observations,
            "contexts": only_contexts,
            "barcodes": only_barcodes
        }
        reader = fct.h5.ReaderV2(paths=paths, skip=skip, only=only)
        
        for observation in reader.observations():
            for window in windows:
                result = window.aggregate(observation)
                result.writev2(h5_out, compression, compression_opts)
        

@click.command
@input_globs
@h5_subsets
@click.option(
    "--only-observations", "--observations", "--obs",
    type=str,
    multiple=True,
    show_default=True,
    help = "Name of observations dataset to aggregate in Amethyst H5 files at /[context]/[barcode]/[observations] with columns (chr, pos, c, t)"
)
@click.option(
    "--variable-windows", "--variable", "--windows", "--win", "-v",
    multiple=True,
    type=str,
    help = (
        r"Nonuniform window sums. Format options: {name}={path} or {path}. {name} "
        "will become part of the Amethyst H5 path to the window aggregation results "
        "at /[context]/[barcode]/[name]. "
        "{path} is the path to a columnar file (CSV, TSV, etc. - schema sniffed by DuckDB read_csv) with a header "
        "containing column names 'chr', 'start', and 'end'."
    )
)
@click.option(
    "--uniform-windows", "--uniform", "--unif", "--uw", "-u",
    multiple=True,
    type=str,
    help = (
        r"Uniform window sums. Format options: {size}, {name}={size}:{step}+{offset}, "
        "or subsets ({name}=, :{step}, +{offset} are optional). "
        "{name} is the datasetname for the aggregation stored under /[context]/[barcode]/[name], "
        "{size} is the window size, {step} is the constant stride between window start sites (defaults to size). "
        "Window name defaults to filename prefix. Examples: -w special_fancy_windows=sfw.tsv -w sfw.tsv"
    )
)
@compression
@verbosity
@h5_out
@click.argument("h5-in", nargs=-1)
def agg(
    globs, 
    only_observations, 
    only_contexts, 
    only_barcodes, 
    skip_barcodes, 
    variable_windows, 
    uniform_windows, 
    compression, 
    compression_opts, 
    verbosity,
    logfile,
    h5_out, 
    h5_in):
    """Compute window sums over methylation observations stored in Amethyst v2.0.0 format.

    FILENAMES: Amethyst H5 filenames in format /[context]/[barcode]/[observations] to compute window sums.
    Can be specified as a single glob (i.e. *.h5) and will be combined with additional globs specified with -g.
    """
    import amethyst_facet as fct
    fct.logging.config(verbosity, logfile)
    logging.info(
        f"Called facet agg with globs={globs}, only_observations={only_observations}, only_contexts={only_contexts}, "
        f"only_barcodes={only_barcodes}, skip_barcodes={skip_barcodes}, variable_windows={variable_windows}, "
        f"uniform_windows={uniform_windows}, compression={compression}, compression_opts={compression_opts}, verbosity={verbosity}, "
        f"logfile={logfile}, h5_out={h5_out}, h5_in={h5_in}."
    )
    aggregator = AmethystH5Aggregator()
    aggregator.aggregate(
        globs, 
        only_observations, 
        only_contexts, 
        only_barcodes, 
        skip_barcodes, 
        variable_windows, 
        uniform_windows, 
        compression, 
        compression_opts, 
        h5_out, 
        h5_in
    )