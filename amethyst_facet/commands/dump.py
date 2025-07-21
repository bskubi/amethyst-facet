import gzip
import pathlib

import click
import shutil

import amethyst_facet as fct

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
    "--observations", "--obs", "-o", "observations",
    type=str,
    default = "1",
    show_default=True,
    help = "Name of observations dataset to dump from Amethyst H5 files at /[context]/[barcode]/[observations]"
)
@click.argument(
    "filenames",
    nargs=-1,
    type=str
)
def dump(globs, contexts, skipbc, requirebc, observations, filenames):
    """Dump requested Amethyst H5 files 
    """
    filenames = fct.combine_filenames(filenames, globs)
    skip_bc = fct.read_barcode_file(skipbc)
    require_bc = fct.read_barcode_file(requirebc)
    h5reader = fct.AmethystH5ReaderV2(filenames, observations, contexts, skip_bc, require_bc)

    for dataset in h5reader:
        filename = dataset.filename.replace("/", "_")
        path = pathlib.Path(filename) / dataset.context / dataset.barcode / dataset.observations
        assert not path.exists(), (
            f"Cannot dump data from {dataset.filename}::/{dataset.context}/{dataset.barcode}/{dataset.observations} to {path}: "
            f"as {path} already exists."
        )
        path.parent.mkdir(parents=True, exist_ok=True)

        dataset.pl().write_csv(path, include_header = True, separator="\t")
        with open(path, 'rb') as f_in:
            with gzip.open(str(path) + ".gz", 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
                path.unlink()
