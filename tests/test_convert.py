# import os
# from pathlib import Path
# import shutil

# import h5py
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


# def test_convert_skip(cleanup_temp):
#     dataset = pl.DataFrame({
#         "chr": ["1", "1", "2", "2"],
#         "pos": [10, 11, 10, 11],
#         "c": [1, 1, 1, 1],
#         "t": [1, 1, 1, 1]
#     }).to_numpy(structured=True)
#     data = dataset.astype(fct.cov_dtype)

#     filename1 = "tests/assets/temp/f1.h5"
#     filename2 = "tests/assets/temp/f2.h5"

#     file1 = h5py.File(filename1, "w")
#     file1.create_dataset("/CG/barcode1", data=data)
#     file1.create_dataset("/CG/barcode2", data=data)

#     file2 = h5py.File(filename2, "w")
#     file2.create_dataset("/CG/barcode3", data=data)
#     file2.create_dataset("/CG/barcode4", data=data)

#     fct.run_convert([], [], "tests/assets/barcode1_barcode3.txt", "", "gzip", 6, "tests/assets/temp/converted.h5", [filename1, filename2])
#     with h5py.File("tests/assets/temp/converted.h5") as file:
#         assert sorted(list(file["/"])) == ["CG", "metadata"], "Failed to create context CG and metadata string"
#         assert file["/metadata/version"][()].decode() == "amethyst2.0.0", f"Metadata string is incorrect: {file["/metadata/version"][()].decode()}"
#         assert sorted(list(file["/CG"])) == ["barcode2", "barcode4"]

# def test_convert_require(cleanup_temp):
#     dataset = pl.DataFrame({
#         "chr": ["1", "1", "2", "2"],
#         "pos": [10, 11, 10, 11],
#         "c": [1, 1, 1, 1],
#         "t": [1, 1, 1, 1]
#     }).to_numpy(structured=True)
#     data = dataset.astype(fct.cov_dtype)

#     filename1 = "tests/assets/temp/f1.h5"
#     filename2 = "tests/assets/temp/f2.h5"

#     file1 = h5py.File(filename1, "w")
#     file1.create_dataset("/CG/barcode1", data=data)
#     file1.create_dataset("/CG/barcode2", data=data)

#     file2 = h5py.File(filename2, "w")
#     file2.create_dataset("/CG/barcode3", data=data)
#     file2.create_dataset("/CG/barcode4", data=data)

#     fct.run_convert([], [], "", "tests/assets/barcode1_barcode3.txt", "gzip", 6, "tests/assets/temp/converted.h5", [filename1, filename2])
#     with h5py.File("tests/assets/temp/converted.h5") as file:
#         assert sorted(list(file["/"])) == ["CG", "metadata"], "Failed to create context CG and metadata string"
#         assert file["/metadata/version"][()].decode() == "amethyst2.0.0", f"Metadata string is incorrect: {file["/metadata/version"][()].decode()}"
#         assert sorted(list(file["/CG"])) == ["barcode1", "barcode3"]