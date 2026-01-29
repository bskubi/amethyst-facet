from concurrent.futures import ProcessPoolExecutor
import glob
import itertools
from typing import *
import h5py
import click
from loguru import logger

@logger.catch(reraise=True)
def delete_from_h5(args: Tuple[str]):
    """Delete all datasets matching dataset_name for all contexts and barcodes
    """
    amethyst_h5_file, name, level = args

    with h5py.File(amethyst_h5_file, 'a') as f:
        for context in f:
            if level == "context" and context == name:
                del f[context]
                logger.info("Deleted /{}", context)
            elif context == "metadata":
                continue
            elif level in ["barcode", "dataset"]:
                for barcode in f[context]:
                    if level == "barcode" and barcode == name:
                        del f[context][barcode]
                        logger.info("Deleted /{}/{}", context, barcode)
                    elif level == "dataset":
                        for dataset in f[context][barcode]:
                            if dataset == name:
                                del f[context][barcode][dataset]
                                logger.info("Deleted /{}/{}/{}", context, barcode, dataset)

@click.command
@click.option(
    "--h5", "--globh5", "--glob", "-g", "_globs",
    multiple=True,
    type=str,
    help = "Amethyst v2 files structured as /[context]/[barcode]/[observations]"
)
@click.option(
    "--nproc", "-p", "nproc",
    type=int,
    default=1,
    show_default = True,
    help = "Number of processes to use when aggregating multiple barcodes in a single H5 file"
)
@click.argument(
    "h5obj",
    type = click.Choice(["context", "barcode", "dataset"], case_sensitive=True)
)
@click.argument(
    "h5obj_name",
    type=str
)
@click.argument(
    "filenames",
    nargs=-1,
    type=str
)
def delete(_globs: Tuple[str], nproc: int, h5obj: str, h5obj_name: str, filenames: Tuple[str]):
    """Delete contexts, barcodes, or datasets from an Amethyst 2.0.0 format HDF5 file

    Required arguments:

    {context|barcode|dataset} (case sensitive), the type of object to be deleted. Skips nonexistent groups and datasets.

    H5OBJ_NAME: the name of the object type to be deleted

    FILENAMES: a glob or list of Amethyst v 2.0.0 filenames

    Example to delete all datasets named "500" in every context and barcode:

    python facet.py delete dataset 500 demo.h5
    """
    filenames: List[str] = list(filenames) + list(itertools.chain.from_iterable([glob.glob(it) for it in _globs]))
  
    if not filenames:
        logger.info("No filenames specified, no action will be taken.")

    with ProcessPoolExecutor(max_workers=nproc) as ppe:
        [
            ppe.submit(delete_from_h5, (filename, h5obj_name, h5obj)).result()
            for filename in filenames
        ]