import time
from typing import *
import warnings

import click

import amethyst_facet as fct

def run_agg(globs, contexts, skipbc, requirebc, observations, uniform, variable, output, compression, compression_opts, filenames):
    """Iterate through Amethyst H5 file and run requested aggregations
    """
    compression, compression_opts = fct.parse_cli_compression(compression, compression_opts)
    filenames = fct.combine_filenames(filenames, globs)
    skip_bc = fct.read_barcode_file(skipbc)
    require_bc = fct.read_barcode_file(requirebc)
    h5reader = fct.AmethystH5ReaderV2(filenames, observations, contexts, skip_bc, require_bc)

    all_windows = dict()
    fct.load_windows(uniform, fct.parse_uniform_windows, all_windows)
    fct.load_windows(variable, fct.parse_variable_windows, all_windows)
    assert all_windows, "No window plans specified."

    # Reading datasets is 12% of total runtime
    for dataset in h5reader:
        for dataset_name, windows in all_windows.items():

            # agg_to_amethyst_h5 is 60% of total runtime 
            data = fct.agg_to_amethyst_h5(dataset, windows)

            output_filename = output or dataset.filename
            try:
                # write_amethyst_h5_data is 28% of total runtime
                fct.write_amethyst_h5_data(data, dataset, output_filename, dataset_name, compression, int(compression_opts))
            except:
                fct.write_amethyst_h5_data(data, dataset, output_filename, dataset_name, compression, compression_opts)
            if data.size == 0:
                warnings.warn(f"No aggregation results for {dataset}")
        start = time.time()
@click.command
@click.option(
    "--h5", "--globh5", "--glob", "-g", "globs",
    multiple=True,
    type=str,
    help = "Amethyst v2 files structured as /[context]/[barcode]/[observations]"
)
@click.option(
    "--context", "-c", "contexts",
    multiple=True,
    type=str,
    help = "Limit aggregations to these contexts. Multiple can be specified. If none given, aggregates all barcodes."
)
@click.option(
    "--skipbc",
    type = str,
    help = "Skip barcodes listed in the given newline-separated file."
)
@click.option(
    "--requirebc",
    type = str,
    help = "Require barcodes listed in the given newline-separated file."
)
@click.option(
    "--observations", "--obs", "-o", "observations",
    type=str,
    default = "1",
    show_default=True,
    help = "Name of observations dataset to aggregate in Amethyst H5 files at /[context]/[barcode]/[observations] with columns (chr, pos, c, t)"
)
@click.option(
    "--variable", "--windows", "--win", "-w",
    multiple=True,
    type=str,
    help = r"Nonuniform window sums. Format options: {name}={filename}, {filename} where {name} is the datasetname for the aggregation stored under /[context]/[barcode]/[name], {filename} is a CSV-like file (schema sniffed by DuckDB) with chr, start, and end headers. Window name defaults to size (if no step) or [size]by[step] (if step is specified). Examples: -u 1k=1000 -u 1k_500=1000:500 -u 1000:500 -u 1000"
)
@click.option(
    "--uniform", "--unif", "--uw", "-u",
    multiple=True,
    type=str,
    help = r"Uniform window sums. Format options: {name}={size}, {name}={size}:{step}, {size}:{step}, {size} where {name} is the datasetname for the aggregation stored under /[context]/[barcode]/[name], {size} is the window size, {step} is the constant stride between window start sites (defaults to size). Window name defaults to filename prefix. Examples: -w special_fancy_windows=sfw.tsv -w sfw.tsv"
)
@click.option(
    "--output", "--out",
    type=str,
    default = "",
    help = f"Output file for window aggregation. Defaults to writing to input file."
)
@click.option(
    "--compression",
    type=str,
    default = 'gzip'
)
@click.option(
    "--compression_opts",
    type=str,
    default = '6'
)
@click.option(
    "--nproc", "-p",
    type=int,
    default=1,
    show_default = True,
    help = "Number of processes to use when aggregating multiple barcodes in a single H5 file (placeholder, not used)"
)
@click.argument(
    "filenames",
    nargs=-1,
    type=str
)
def agg(globs, contexts, skipbc, requirebc, observations, uniform, variable, output, compression, compression_opts, nproc, filenames):
    """Compute window sums over methylation observations stored in Amethyst v2.0.0 format.

    FILENAMES: Amethyst H5 filenames in format /[context]/[barcode]/[observations] to compute window sums.
    Can be specified as a single glob (i.e. *.h5) and will be combined with additional globs specified with -g.
    """
    run_agg(globs, contexts, skipbc, requirebc, observations, uniform, variable, output, compression, compression_opts, filenames)