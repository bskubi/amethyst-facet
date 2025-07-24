from pathlib import Path

from click.testing import CliRunner
import numpy as np
import amethyst_facet as fct
from amethyst_facet.cli.commands.facet import facet

from ..util import cleanup_temp

def test_convert(cleanup_temp):
    data = np.array(
        [
            ("1", 1, 1, 1),
            ("2", 1, 0, 1)
        ],
        dtype = fct.h5.observations_v2_dtype
    )
    dataset1 = fct.h5.Dataset("CG", "barcode1", "1", data=data)
    dataset2 = fct.h5.Dataset("CH", "barcode2", "2", data=data)
    base = Path("tests/assets/temp")
    dataset1.writev1(base/"file1.h5")
    dataset2.writev1(base/"file2.h5")
    readerv1 = fct.h5.ReaderV1(paths=[base/"file1.h5", base/"file2.h5"])
    for obs in readerv1.observations():
        assert obs.format == "obsv1"
        obs.writev2(base/"converted.h5")
    
    readerv2 = fct.h5.ReaderV2(paths=[base/"converted.h5"])
    for obs in readerv2.observations():
        assert obs.format == "obsv2"

def test_convert_e2e1(cleanup_temp):
    try:
        fct.logging.config("debug", None)
        data = np.array(
            [
                ("1", 1, 1, 1),
                ("2", 1, 0, 1)
            ],
            dtype = fct.h5.observations_v2_dtype
        )
        base = Path("tests/assets/temp")
        file1 = base / "file1.h5"
        file2 = base / "file2.h5"
        paths = [file1, file2]
        datasets =[
            fct.h5.Dataset("CG", "barcode1", "1", data, file1),
            fct.h5.Dataset("CG", "barcode2", "1", data, file1),
            fct.h5.Dataset("CH", "barcode1", "1", data, file1),
            fct.h5.Dataset("CH", "barcode2", "1", data, file1),
            fct.h5.Dataset("CG", "barcode3", "1", data, file2),
            fct.h5.Dataset("CG", "barcode4", "1", data, file2),
            fct.h5.Dataset("CH", "barcode3", "1", data, file2),
            fct.h5.Dataset("CH", "barcode4", "1", data, file2)
        ]
        
        for dataset in datasets:
            dataset.writev1()

        h5_out = base / "converted.h5"

        runner = CliRunner()
        path_strings = [str(p) for p in paths]
        with open(base / "only_barcodes.txt", "w") as file:
            file.write("barcode1\nbarcode3")
        result = runner.invoke(facet, ["convert", "--only-barcodes", str(base/"only_barcodes.txt"), "--verbosity", "debug", str(h5_out), *path_strings])
        if result.exception:
            raise result.exception
        

        reader = fct.h5.ReaderV2(paths=[h5_out])
        observations = list(reader.observations())
        
        contexts = sorted([o.context for o in observations])
        barcodes = sorted([o.barcode for o in observations])
        names = sorted([o.name for o in observations])
        dtypes = [o.data.dtype for o in observations]
        assert len(observations) == 4
        assert contexts == ["CG"]*2 + ["CH"]*2
        assert barcodes == ["barcode1"]*2 + ["barcode3"]*2
        assert names == ["1"]*4
        assert dtypes == [fct.h5.dataset.observations_v2_dtype]*4

    except Exception as e:
        raise e

def test_convert_e2e2(cleanup_temp):
    try:
        fct.logging.config("debug", None)
        data = np.array(
            [
                ("1", 1, 1, 1),
                ("2", 1, 0, 1)
            ],
            dtype = fct.h5.observations_v2_dtype
        )
        base = Path("tests/assets/temp")
        file1 = base / "file1.h5"
        file2 = base / "file2.h5"
        paths = [file1, file2]
        datasets =[
            fct.h5.Dataset("CG", "barcode1", "1", data, file1),
            fct.h5.Dataset("CG", "barcode2", "1", data, file1),
            fct.h5.Dataset("CH", "barcode1", "1", data, file1),
            fct.h5.Dataset("CH", "barcode2", "1", data, file1),
            fct.h5.Dataset("CG", "barcode3", "1", data, file2),
            fct.h5.Dataset("CG", "barcode4", "1", data, file2),
            fct.h5.Dataset("CH", "barcode3", "1", data, file2),
            fct.h5.Dataset("CH", "barcode4", "1", data, file2)
        ]
        
        for dataset in datasets:
            dataset.writev1()

        h5_out = base / "converted.h5"

        runner = CliRunner()
        path_strings = [str(p) for p in paths]
        with open(base / "skip_barcodes.txt", "w") as file:
            file.write("barcode1\nbarcode3")
        result = runner.invoke(facet, ["convert", "--skip-barcodes", str(base/"skip_barcodes.txt"), "--verbosity", "debug", str(h5_out), *path_strings])
        if result.exception:
            raise result.exception
        

        reader = fct.h5.ReaderV2(paths=[h5_out])
        observations = list(reader.observations())
        
        contexts = sorted([o.context for o in observations])
        barcodes = sorted([o.barcode for o in observations])
        names = sorted([o.name for o in observations])
        dtypes = [o.data.dtype for o in observations]
        assert contexts == ["CG"]*2 + ["CH"]*2
        assert barcodes == ["barcode2"]*2 + ["barcode4"]*2
        assert names == ["1"]*4
        assert dtypes == [fct.h5.dataset.observations_v2_dtype]*4
    except Exception as e:
        raise e

def test_convert_e2e3(cleanup_temp):
    try:
        fct.logging.config("debug", None)
        data = np.array(
            [
                ("1", 1, 1, 1),
                ("2", 1, 0, 1)
            ],
            dtype = fct.h5.observations_v2_dtype
        )
        base = Path("tests/assets/temp")
        file1 = base / "file1.h5"
        file2 = base / "file2.h5"
        paths = [file1, file2]
        datasets =[
            fct.h5.Dataset("CG", "barcode1", "1", data, file1),
            fct.h5.Dataset("CG", "barcode2", "1", data, file1),
            fct.h5.Dataset("CH", "barcode1", "1", data, file1),
            fct.h5.Dataset("CH", "barcode2", "1", data, file1),
            fct.h5.Dataset("CG", "barcode3", "1", data, file2),
            fct.h5.Dataset("CG", "barcode4", "1", data, file2),
            fct.h5.Dataset("CH", "barcode3", "1", data, file2),
            fct.h5.Dataset("CH", "barcode4", "1", data, file2)
        ]
        
        for dataset in datasets:
            dataset.writev1()

        h5_out = base / "converted.h5"

        runner = CliRunner()
        path_strings = [str(p) for p in paths]
        result = runner.invoke(facet, ["convert", "--verbosity", "debug", str(h5_out), *path_strings])
        if result.exception:
            raise result.exception
        

        reader = fct.h5.ReaderV2(paths=[h5_out])
        observations = list(reader.observations())
        
        contexts = sorted([o.context for o in observations])
        barcodes = sorted([o.barcode for o in observations])
        names = sorted([o.name for o in observations])
        dtypes = [o.data.dtype for o in observations]
        assert len(observations) == 8
        assert contexts == ["CG"]*4 + ["CH"]*4
        assert barcodes == ["barcode1"]*2 + ["barcode2"]*2 + ["barcode3"]*2 + ["barcode4"]*2
        assert names == ["1"]*8
        assert dtypes == [fct.h5.dataset.observations_v2_dtype]*8
    except Exception as e:
        raise e
