# import os
# import shutil

# import polars as pl
# import pytest

# import amethyst_facet as fct

# @pytest.fixture
# def cleanup_temp():
#     yield

#     # Loop through all the items in the temp directory
#     for filename in os.listdir("tests/assets/temp"):
#         file_path = os.path.join("tests/assets/temp", filename)
#         try:
#             # If it's a file or a symlink, delete it
#             if os.path.isfile(file_path) or os.path.islink(file_path):
#                 os.unlink(file_path)
#             # If it's a directory, delete it and all its contents
#             elif os.path.isdir(file_path):
#                 shutil.rmtree(file_path)
#         except Exception as e:
#             # Handle potential errors, e.g., permission denied
#             print(f'Failed to delete {file_path}. Reason: {e}')

# def test_reader(cleanup_temp):
#     with fct.h5.open("tests/assets/temp/test.h5") as file:
#         file["/"].create_group("test_group")


# def test_amethyst_h5_reader():
#     reader = fct.AmethystH5ReaderV2(["tests/assets/test.h5"])
#     expecteds = {
#         "CH": pl.DataFrame({
#             "chr": ["chr1", "chr2"],
#             "pos": [10, 79],
#             "t": [1, 3],
#             "c": [0, 3]
#         }),
#         "CHG": pl.DataFrame({
#             "chr": ["chr2", "chr2", "chr3", "chr3"],
#             "pos": [56, 85, 78, 83],
#             "t": [2, 0, 1, 5],
#             "c": [3, 0, 2, 1]
#         }),
#         "CHH": pl.DataFrame({
#             "chr": ["chr1", "chr1", "chr3", "chr3"],
#             "pos": [29, 33, 18, 22],
#             "t": [4, 4, 0, 4],
#             "c": [5, 4, 2, 5]
#         })
#     }
#     for dataset in reader:
#         expected = expecteds[dataset.context]
#         assert dataset.pl().equals(expected), f"For {dataset}, {dataset.pl()} != {expected}"