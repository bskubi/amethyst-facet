import glob
import io
import itertools
import logging
from pathlib import Path
from typing import *
import h5py

class InvalidCompressionArgs(ValueError):
    def __init__(self, compression, compression_opts):
        message = f"Invalid h5py compression arguments compression='{compression}' and compression_opts='{compression_opts}'"
        super().__init__(message)

class InvalidGlobs(ValueError):
    def __init__(self, orig_globs, globs):
        message = (
            f"Unable to parse globs:\n"
            f"{orig_globs}\n"
            f"Final result:\n"
            f"{globs}\n"
        )
        super().__init__(message)

class FailedFilenamesGlobsConcat(ValueError):
    def __init__(self, filenames: List[str], globs: List[str]):
        message = (
            f"Unable to concatenate list of filenames and list of globs:\n"
            f"Filenames: {filenames}\n"
            f"Globs: {globs}\n"
        )
        super().__init__(message)

class BarcodeFileProblem(Exception):
    def __init__(self, message: str):
        message = message + (
            "Barcode files should be a file containing a newline-separated list "
            "of barcodes to skip (--skipbc) or require (--requirebc)\n"
        )
        super().__init__(message)

class MissingBarcodeFile(BarcodeFileProblem):
    def __init__(self, filename: str | Path):
        message = f"Could not find barcode file {filename}\n"
        super().__init__(message)

class FailedReadBarcodeFile(BarcodeFileProblem):
    def __init__(self, filename: str | Path):
        message = (
            f"{filename} exists but barcodes could not be read from it "
            "-- check read permissions, file format\n"
        )
        super().__init__(message)

class CLIOptionsParser:
    def parse_h5py_compression(self, compression: str, compression_opts: str) -> Tuple[str, Any]:
        """Make CLI compression and compression_opts args h5py-compatible
        """
        compression = compression.strip()

        if compression_opts and not compression:
            raise InvalidCompressionArgs(compression, compression_opts)

        try:
            # Some compressors (i.e. gzip) require compression_opts be an int
            # others don't. So try and convert and use the original version otherwise.
            compression_opts = int(compression_opts)
        except:
            if compression_opts == "":
                compression_opts = None

        if compression == "":
            compression = None

        try:
            bio = io.BytesIO()
            with h5py.File(bio, "w") as f:
                f.create_dataset("test", shape=1, dtype=int, compression=compression, compression_opts=compression_opts)
        except Exception as e:
            raise InvalidCompressionArgs(compression, compression_opts)

        return compression, compression_opts

    def combine_paths_globs(self, paths: List[str | Path], orig_globs: List[str]) -> List[str]:
        paths = list(paths)
        orig_globs = list(orig_globs)
        try:
            globs = [glob.glob(it) for it in orig_globs]
            globs = itertools.chain.from_iterable(globs)
            globs = list(globs)
        except Exception as e:
            raise InvalidGlobs(orig_globs, globs) from e

        try:
            paths = paths + globs
        except Exception as e:
            raise FailedFilenamesGlobsConcat(paths, globs) from e
        return paths
    
    def read_barcode_file(self, filename: str) -> List[str]:
        """Read barcodes from a file containing a newline-separated list of barcodes
        """
        filename = filename or ""
        result = []
        if filename.strip():
            if not Path(filename).exists:
                raise MissingBarcodeFile(filename)
            
            try:
                with open(filename) as file:
                    lines = file.readlines()
                    logging.debug(lines)
                    result = [r.strip() for r in lines]
                    logging.debug(result)
            except:
                raise FailedReadBarcodeFile(filename)
        return result
