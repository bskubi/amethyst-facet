import glob
import itertools
from typing import *
import pathlib


def combine_filenames(filenames: List[str], globs: List[str]) -> List[str]:
    filenames = list(filenames) + list(itertools.chain.from_iterable([glob.glob(it) for it in globs]))
    return filenames

def read_barcode_file(file: str) -> List[str]:
    """Read barcodes from a file containing a newline-separated list of barcodes
    """
    result = []
    if file:
        assert pathlib.Path(file).exists, f"No barcode file found at {file}"
        try:
            result = [r.strip() for r in open(file).readlines()]
        except:
            raise ValueError(f"Could not open or parse list of barcodes at {file}")
    return result

def load_windows(
        args: List[str], 
        parse_func: Callable, 
        all_windows: Dict
        ) -> None:
    """Update all_windows with a new Uniform/VariableWindow parsed from CLI arg

    args: A CLI argument like {name}={size}:{step}+{offset} (uniform windows)
    or {name}={filename} (variable windows)
    parse_func: Either fct.parse_uniform_windows or fct.parse_variable_windows
    all_windows: Dictionary with keys being the window name, values being the windows object
    """
    for arg in args:
        name, windows = parse_func(arg)
        assert name not in all_windows, f"Duplicate name '{name}' found for plans:\n{all_windows.get(name)}\nand\n{windows}"
        all_windows[name] = windows

def parse_cli_compression(compression, compression_opts) -> Tuple[str, Any]:
    """Set compression and compression_opts to None of blank, convert compression_opts to int if possible
    """
    try:
        compression_opts = int(compression_opts)
    except:
        pass

    if compression == "":
        compression = None
    if compression_opts == "":
        compression_opts = None
    return compression, compression_opts