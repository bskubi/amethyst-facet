import click

input_globs = click.option(
    "--h5", "--globh5", "--glob", "-g", "globs",
    multiple=True,
    type=str,
    help = "Globs referring to Amethyst H5 files to use as inputs (i.e. '*.h5' would use ./file1.h5, ./file2.h5, ... as input)."
)

compression_algo = click.option(
    "--compression",
    type=str,
    default = 'gzip',
    show_default=True,
    help="Compression algorithm for writing to Amethyst H5"
)
compression_opts = click.option(
    "--compression_opts",
    type=str,
    default = '6',
    show_default=True,
    help="Compression algorithm options for writing to Amethyst H5 (default is compatible with gzip)."
)

def compression(f):
    f = compression_algo(f)
    f = compression_opts(f)
    return f

only_contexts = click.option(
    "--only-contexts", "--only-ctx", "--contexts", "-c",
    type=str,
    multiple=True,
    help="Only use these contexts. Multiple can be specified (i.e. '-c CG -c CH') If none given, uses all contexts."
)
only_barcodes = click.option(
    "--only-barcodes", "--only-bc", "--requirebc",
    type = str,
    help = "A file containing barcodes (newline-separated). Only barcodes in this file will be used."
)
skip_barcodes = click.option(
    "--skip-barcodes", "--skip-bc", "--skipbc",
    type = str,
    help = "A file containing barcodes (newline-separated). Barcodes in this file will not be used (overrides --only-barcodes for conflicts)."
)

def h5_subsets(f):
    f = only_contexts(f)
    f = only_barcodes(f)
    f = skip_barcodes(f)
    return f

h5_out = click.option(
    "--h5-out", "-o", "--out", 
    type=str,
    default = None,
    show_default=True,
    help = "Output Amethyst H5 file to write results. If None, results are appended to input file as new datasets."
)