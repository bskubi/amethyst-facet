import itertools
import logging
from pathlib import Path
import random
from typing import *

from click.testing import CliRunner
import polars as pl
import pytest

from amethyst_facet.cli.commands.facet import facet
import amethyst_facet as fct
from ..util import cleanup_temp


class WriteH5Exception(Exception):
    def __init__(self, previous: List[fct.h5.Dataset], new_dataset: fct.h5.Dataset):
        fmt = lambda d: f"context='{d.context}' barcode='{d.barcode}' name='{d.name}' path='{d.path}'\n"
        message = (
            f"Failed to write new dataset: {fmt(new_dataset)}\n"
            f"Previous datasets:\n"
            f"{''.join(fmt(d) for d in previous)}"
        )
        super().__init__(message)

def write_h5_observations(contexts: List[str], barcodes: List[str], names: List[str], datas: List[pl.DataFrame], paths: List[str], version: str = "v2"):
    previous = []
    dataset = None
    try:
        for context, barcode, name in itertools.product(contexts, barcodes, names):
            data = random.choice(datas)
            path = random.choice(paths)
            dataset = fct.h5.Dataset(context, barcode, name, data, path)
    
            if version == "v1":
                logging.debug(f"Writing to {path} in format v1")
                dataset.writev1()
            elif version == "v2":
                logging.debug(f"Writing to {path} in format v2")
                dataset.writev2()
            else:
                raise ValueError(f"Unrecognized Amethyst version: {version}")
            previous.append(dataset)
    except Exception as e:
        raise WriteH5Exception(previous, dataset) from e


def observations_data1():
    chr = [ 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2,  2,  2,  2,  2]
    pos = [-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
    c =   [ 1, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3,   3,  4,  4]
    t =   [ 1, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3,   3,  4,  4]
    chr = [str(c) for c in chr]
    data = pl.DataFrame({"chr": chr, "pos": pos, "c": c, "t": t})
    data = data.cast({"chr": pl.String, "pos": pl.Int64, "c": pl.Int64, "t": pl.Int64})
    return data

def test_write_read_datasets_v1(cleanup_temp):
    temp = Path("tests/assets/temp")
    expected = observations_data1()
    expected = expected.with_columns(pct = pl.col.c/(pl.col.c+pl.col.t))
    expected = expected.select("chr", "pos", "pct", "c", "t")
    contexts = ["CG", "CH"]
    barcodes = ["barcode1", "barcode2", "barcode3", "barcode4"]
    names = ["1"]
    paths = [temp / "file1.h5", temp / "file2.h5"]

    write_h5_observations(
        contexts=contexts, 
        barcodes = barcodes,
        names = names,
        datas = [observations_data1()],
        paths = paths,
        version = "v1"
    )
    reader = fct.h5.ReaderV1(paths=paths)
    observations = list(reader.observations())
    assert len(observations) == len(contexts)*len(barcodes)*len(names)
    for obs in reader.observations():
        values = obs.pl()
        values = values.cast({"chr": pl.String})
        assert values.equals(expected), f"{values} != {expected}"


def test_write_read_datasets_v2(cleanup_temp):
    temp = Path("tests/assets/temp")
    expected = observations_data1()
    contexts = ["CG", "CH"]
    barcodes = ["barcode1", "barcode2", "barcode3", "barcode4"]
    names = ["1"]
    paths = [temp / "file1.h5", temp / "file2.h5"]

    write_h5_observations(
        contexts=contexts, 
        barcodes = barcodes,
        names = names,
        datas = [observations_data1()],
        paths = paths,
        version = "v2"
    )
    reader = fct.h5.ReaderV2(paths=paths)
    observations = list(reader.observations())
    assert len(observations) == len(contexts)*len(barcodes)*len(names)
    for obs in observations:
        values = obs.pl()
        values = values.cast({"chr": pl.String})
        assert values.equals(expected), f"{values} != {expected}"

def observations_data2():
    chr = [1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2]
    pos = [1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6]
    c   = [0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2]
    t   = [1, 0, 1, 1, 2, 2, 1, 0, 1, 1, 2, 2]
    df = pl.DataFrame({
        "chr": chr,
        "pos": pos,
        "c": c,
        "t": t
    })
    return df

def test_agg_v2_to_v2_append_e2e(cleanup_temp):
    size=2
    step=1
    offset=1

    expected = pl.DataFrame({
        "chr":   [1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2],
        "start": [0, 1, 2, 3, 4, 5, 6, 0, 1, 2, 3, 4, 5, 6],
        "end":   [2, 3, 4, 5, 6, 7, 8, 2, 3, 4, 5, 6, 7, 8],
        "c":     [0, 1, 3, 2, 1, 3, 2, 0, 1, 3, 2, 1, 3, 2],
        "t":     [1, 1, 1, 2, 3, 4, 2, 1, 1, 1, 2, 3, 4, 2],
        "c_nz":  [0, 1, 2, 1, 1, 2, 1, 0, 1, 2, 1, 1, 2, 1],
        "t_nz":  [1, 1, 1, 2, 2, 2, 1, 1, 1, 1, 2, 2, 2, 1]
    }).cast({"chr": pl.String})

    temp = Path("tests/assets/temp")
    observations = observations_data2()
    contexts = ["CG", "CH"]
    barcodes = ["barcode1", "barcode2", "barcode3", "barcode4"]
    names = ["1"]
    paths = [temp / "file1.h5", temp / "file2.h5"]

    write_h5_observations(contexts=contexts, barcodes=barcodes, names=names, datas=[observations], paths=paths)

    runner = CliRunner()
    path_strings = [str(p) for p in paths]
    runner.invoke(facet, ["agg", "-u", f"{size}:{step}+{offset}", "--verbosity", "debug", *path_strings])
    # reader = fct.h5.ReaderV2(paths=paths)
    # windows = list(reader.windows())
    # assert len(windows) == (len(contexts)*len(barcodes)*len(names))
    # for windows in windows:
    #     values = windows.pl()
    #     values = values.cast({"chr": pl.String})
    #     assert values.equals(expected), f"{values} != {expected}"

def test_agg_v2_to_v2_h5_out_e2e(cleanup_temp):
    size=2
    step=1
    offset=1

    expected = pl.DataFrame({
        "chr":   [1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2],
        "start": [0, 1, 2, 3, 4, 5, 6, 0, 1, 2, 3, 4, 5, 6],
        "end":   [2, 3, 4, 5, 6, 7, 8, 2, 3, 4, 5, 6, 7, 8],
        "c":     [0, 1, 3, 2, 1, 3, 2, 0, 1, 3, 2, 1, 3, 2],
        "t":     [1, 1, 1, 2, 3, 4, 2, 1, 1, 1, 2, 3, 4, 2],
        "c_nz":  [0, 1, 2, 1, 1, 2, 1, 0, 1, 2, 1, 1, 2, 1],
        "t_nz":  [1, 1, 1, 2, 2, 2, 1, 1, 1, 1, 2, 2, 2, 1]
    }).cast({"chr": pl.String})

    temp = Path("tests/assets/temp")
    observations = observations_data2()
    contexts = ["CG", "CH"]
    barcodes = ["barcode1", "barcode2", "barcode3", "barcode4"]
    names = ["1"]
    paths = [temp / "file1.h5", temp / "file2.h5"]
    h5_out = str(temp / "output.h5")

    write_h5_observations(contexts=contexts, barcodes=barcodes, names=names, datas=[observations], paths=paths)

    runner = CliRunner()
    path_strings = [str(p) for p in paths]
    runner.invoke(facet, ["agg", "-u", f"{size}:{step}+{offset}", "--verbosity", "debug", "--h5-out", h5_out, *path_strings])
    reader = fct.h5.ReaderV2(paths=[h5_out])
    windows = list(reader.windows())
    assert len(windows) == (len(contexts)*len(barcodes)*len(names))
    for windows in windows:
        values = windows.pl()
        values = values.cast({"chr": pl.String})
        assert values.equals(expected), f"{values} != {expected}"

def test_agg_v1_to_v2_h5_out_e2e(cleanup_temp):
    with pytest.raises(fct.h5.reader.ReaderFileMismatch):
        temp = Path("tests/assets/temp")
        observations = observations_data2()
        contexts = ["CG", "CH"]
        barcodes = ["barcode1", "barcode2", "barcode3", "barcode4"]
        names = ["1"]
        paths = [temp / "file1.h5", temp / "file2.h5"]
        h5_out = str(temp / "output.h5")

        write_h5_observations(contexts=contexts, barcodes=barcodes, names=names, datas=[observations], paths=paths, version="v1")

        runner = CliRunner()
        path_strings = [str(p) for p in paths]
        result = runner.invoke(facet, ["agg", "-u", f"2:1+1", "--h5-out", h5_out, *path_strings])
        if result.exception:
            raise result.exception