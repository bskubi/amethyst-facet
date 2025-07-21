import warnings

import click
import h5py

import amethyst_facet as fct

def run_convert(globs, contexts, skipbc, requirebc, compression, compression_opts, h5_out, h5_in):
    compression, compression_opts = fct.parse_cli_compression(compression, compression_opts)

    filenames = fct.combine_filenames(h5_in, globs)
    skip_bc = fct.read_barcode_file(skipbc)
    require_bc = fct.read_barcode_file(requirebc)

    count = 0
    with h5py.File(h5_out, "a") as out_f:
        for dataset in fct.AmethystH5ReaderV1(filenames, contexts, skip_bc, require_bc):
            dataset_path = f"/{dataset.context}/{dataset.barcode}/1"
            try:
                out_f.create_dataset(dataset_path, data=dataset.dataset, compression=compression, compression_opts=compression_opts)
                count += 1
            except Exception as e:
                raise ValueError(
                    f"Could not create dataset {dataset.filename}::/{dataset_path}. "
                    f"Possible causes: 2+ converted files may have the same context and barcode, "
                    f"or you may be writing to a file that already contains /context/barcode/1 matching an input file"
                ) from e

        if "/metadata/version" not in out_f:
            out_f.create_dataset('/metadata/version', data = "amethyst2.0.0")
    if count == 0:
        warnings.warn(f"No barcodes converted for files {h5_in}, contexts {contexts}, skipped barcodes from file {skipbc}, required barcodes {requirebc}")
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
    help = "Only dump these contexts. Multiple can be specified. If none given, dumps all barcodes."
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
    "--compression",
    type=str,
    default = 'gzip'
)
@click.option(
    "--compression_opts",
    type=str,
    default = '6'
)
@click.argument(
    "h5_out"
)
@click.argument(
    "h5_in",
    nargs=-1
)
def convert(globs, contexts, skipbc, requirebc, compression, compression_opts, h5_out, h5_in):
    """Convert one or more old Amethyst HDF5 file format to v2.0.0 format.

    The old format stored observations as (chr, pos, pct, c, t) in a dataset at /context/barcode.
    The new format stores base-pair observations as (chr, pos, c, t) in a dataset at /context/barcode/1
    and window aggregations in a dataset at /context/barcode/[window_dataset_name].
    It also contains a dataset at /metadata/version equal to 'amethyst2.0.0'.

    If more than one input file is specified, they are all written to the same output file.
    If the same /context/barcode/observations dataset is found in two or more files, fails with an error.
    """
    run_convert(globs, contexts, skipbc, requirebc, compression, compression_opts, h5_out, h5_in)