import logging
import os
import shutil
import sys
import pytest

@pytest.fixture
def cleanup_temp():
    yield

    # Loop through all the items in the temp directory
    for filename in os.listdir("tests/assets/temp"):
        file_path = os.path.join("tests/assets/temp", filename)
        try:
            # If it's a file or a symlink, delete it
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            # If it's a directory, delete it and all its contents
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            # Handle potential errors, e.g., permission denied
            print(f'Failed to delete {file_path}. Reason: {e}')

def log(level):
    root = logging.getLogger()
    root.setLevel(level)