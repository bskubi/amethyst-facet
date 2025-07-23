from pathlib import Path
import numpy as np
import polars as pl
import amethyst_facet as fct
from ..util import *

def test_reader_skip(cleanup_temp):
    base = Path("tests/assets/temp")
    data = np.array([("1", 1, 0, 0), ("2", 1, 1, 1)], dtype=fct.h5.dataset.observations_dtype)
    dataset1 = fct.h5.Dataset("CG", "barcode1", "1", data)
    dataset2 = fct.h5.Dataset("CH", "barcode2", "1", data)
    dataset3 = fct.h5.Dataset("skip", "barcode3", "1", data)
    dataset4 = fct.h5.Dataset("CH", "skip", "1", data)
    dataset1.write(base / "file1.h5")
    dataset2.write(base / "file2.h5")
    dataset3.write(base / "file2.h5")
    dataset4.write(base / "file2.h5")
    reader = fct.h5.ReaderV2(
        paths=[base / "file1.h5", base / "file2.h5"], 
        skip={
            "contexts":set(["skip"]),
            "barcodes":set(["skip"])
        },
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

def test_reader_only(cleanup_temp):
    base = Path("tests/assets/temp")
    data = np.array([("1", 1, 0, 0), ("2", 1, 1, 1)], dtype=fct.h5.dataset.observations_dtype)
    dataset1 = fct.h5.Dataset("CG", "barcode1", "1", data)
    dataset2 = fct.h5.Dataset("CG", "barcode2", "1", data)
    dataset3 = fct.h5.Dataset("CH", "barcode3", "1", data)
    dataset4 = fct.h5.Dataset("CH", "barcode4", "1", data)
    dataset1.write(base / "file1.h5")
    dataset2.write(base / "file2.h5")
    dataset3.write(base / "file2.h5")
    dataset4.write(base / "file2.h5")
    reader = fct.h5.ReaderV2(
        paths=[base / "file1.h5", base / "file2.h5"], 
        only={"contexts": set(["CG"])}
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
    assert all([observed.equals(expected) for observed in observations_dfs])

def test_reader_skip_only(cleanup_temp):
    base = Path("tests/assets/temp")
    data = np.array([("1", 1, 0, 0), ("2", 1, 1, 1)], dtype=fct.h5.dataset.observations_dtype)
    dataset1 = fct.h5.Dataset("CG", "barcode1", "1", data)
    dataset2 = fct.h5.Dataset("CG", "barcode2", "1", data)
    dataset3 = fct.h5.Dataset("CH", "barcode3", "1", data)
    dataset4 = fct.h5.Dataset("CH", "barcode4", "1", data)
    dataset1.write(base / "file1.h5")
    dataset2.write(base / "file2.h5")
    dataset3.write(base / "file2.h5")
    dataset4.write(base / "file2.h5")
    reader = fct.h5.ReaderV2(
        paths=[base / "file1.h5", base / "file2.h5"], 
        only={"contexts": set(["CG"])},
        skip={"barcodes": set(["barcode1"])}
    )
    
    observations = list(reader.observations())
    observations_dfs = [o.pl() for o in observations]
    expected = pl.DataFrame({
        "chr":["1","2"],
        "pos":[1,1],
        "c":[0,1],
        "t":[0,1]
    })
    assert len(observations_dfs) == 1
    assert observations[0].barcode == "barcode2"
    assert all([observed.equals(expected) for observed in observations_dfs])

def test_reader_observations_windows(cleanup_temp):
    base = Path("tests/assets/temp")
    windows_data = np.array([("1", 1, 10, 0, 0, 0, 0), ("2", 1, 10, 1, 1, 1, 1)], dtype=fct.h5.dataset.windows_dtype)
    w1 = fct.h5.Dataset("CG", "barcode1", "w", windows_data)
    w2 = fct.h5.Dataset("CG", "barcode2", "w", windows_data)
    observations_data = np.array([("1", 1, 0, 0), ("2", 1, 1, 1)], dtype=fct.h5.dataset.observations_dtype)
    o1 = fct.h5.Dataset("CG", "barcode1", "o", observations_data)
    o2 = fct.h5.Dataset("CG", "barcode2", "o", observations_data)
    file = base / "file1.h5"
    [it.write(file) for it in [w1,w2,o1,o2]]
    reader = fct.h5.ReaderV2(
        paths=[base / "file1.h5"]
    )
    
    w = list(reader.windows())
    o = list(reader.observations())
    w_df = (it.pl() for it in w)
    o_df = (it.pl() for it in o)

    expected_w = pl.from_numpy(windows_data)
    expected_o = pl.from_numpy(observations_data)
    assert all(it.name == "w" for it in w)
    assert all(it.name == "o" for it in o)
    assert all(it.equals(expected_w) for it in w_df), "Window mismatch"
    assert all(it.equals(expected_o) for it in o_df), "Observations mismatch"
    