from pathlib import Path
import numpy as np
import polars as pl
import amethyst_facet as fct
from ..util import *

def test_reader_v1(cleanup_temp):
    base = Path("tests/assets/temp")
    data = np.array([("1", 1, 0, 0), ("2", 1, 1, 1)], dtype=fct.h5.dataset.observations_dtype)
    dataset1 = fct.h5.Dataset("CG", "barcode1", "1", data)
    dataset2 = fct.h5.Dataset("CH", "barcode2", "1", data)
    dataset1.writev1(base / "file1.h5")
    dataset2.writev1(base / "file2.h5")
    reader = fct.h5.ReaderV1(
        paths=[base / "file1.h5", base / "file2.h5"]
    )
    
    observations = list(reader.observations())
    observations_dfs = [o.pl() for o in observations]
    expected = pl.DataFrame({
        "chr":["1","2"],
        "pos":[1,1],
        "c":[0,1],
        "t":[0,1]
    })
    assert len(observations_dfs) == 2
    assert all([o.barcode in ["barcode1", "barcode2"] for o in observations])
    assert all([observed.equals(expected) for observed in observations_dfs])

def test_convert(cleanup_temp):
    base = Path("tests/assets/temp")
    data = np.array([("1", 1, 0, 0), ("2", 1, 1, 1)], dtype=fct.h5.dataset.observations_dtype)
    dataset1 = fct.h5.Dataset("CG", "barcode1", "1", data)
    dataset2 = fct.h5.Dataset("CH", "barcode2", "1", data)
    dataset1.writev1(base / "file1.h5")
    dataset2.writev1(base / "file2.h5")
    readerv1 = fct.h5.ReaderV1(
        paths=[base / "file1.h5", base / "file2.h5"],
        default_name="1"
    )
    for dataset in readerv1.barcodes():
        dataset.write(base / "converted.h5")
    readerv2 = fct.h5.ReaderV2(
        paths=[base / "converted.h5"]
    )
    observations = list(readerv2.observations())
    assert len(observations) == 2
    assert all([it in observations for it in [dataset1, dataset2]])

    