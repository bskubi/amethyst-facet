import polars as pl
import amethyst_facet as fct

def test_amethyst_h5_reader():
    reader = fct.AmethystH5ReaderV2(["tests/assets/test.h5"])
    expecteds = {
        "CH": pl.DataFrame({
            "chr": ["chr1", "chr2"],
            "pos": [10, 79],
            "t": [1, 3],
            "c": [0, 3]
        }),
        "CHG": pl.DataFrame({
            "chr": ["chr2", "chr2", "chr3", "chr3"],
            "pos": [56, 85, 78, 83],
            "t": [2, 0, 1, 5],
            "c": [3, 0, 2, 1]
        }),
        "CHH": pl.DataFrame({
            "chr": ["chr1", "chr1", "chr3", "chr3"],
            "pos": [29, 33, 18, 22],
            "t": [4, 4, 0, 4],
            "c": [5, 4, 2, 5]
        })
    }
    for dataset in reader:
        expected = expecteds[dataset.context]
        assert dataset.pl().equals(expected), f"For {dataset}, {dataset.pl()} != {expected}"