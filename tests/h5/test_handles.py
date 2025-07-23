import logging
import sys

from ..util import *
import amethyst_facet as fct

def test_handles(cleanup_temp):
    filename = "tests/assets/temp/test.h5"

    # Test using multiple handles
    with fct.h5.open(filename) as file1:
        file1.create_group("1")
        with fct.h5.open(filename) as file2:
            file2.create_group("2")
    
    assert filename not in fct.h5.handles, f"Did not delete handle after all open contexts exited"

    # Test that extra close is OK
    fct.h5.close(filename)

    # Check that the written groups are present
    with fct.h5.open(filename) as file3:
        assert "1" in file3 and "2" in file3, f"Missing groups '1' and '2' in {file3}"

def test_handles_not_h5(cleanup_temp):
    logging.basicConfig(level=logging.DEBUG)
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    filename = "tests/assets/temp/test.txt"
    open(filename, "w").write("test")
    with pytest.raises(Exception):
        with fct.h5.open(filename):
            pass


