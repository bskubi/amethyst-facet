from pathlib import Path
import numpy as np
import amethyst_facet as fct
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
