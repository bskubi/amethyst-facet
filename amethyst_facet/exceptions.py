import re
import pytest

def raises(excinfo, match: str):
    """
    Asserts that a regex pattern is found in the string representation
    of any exception within the captured exception chain.
    """
    current_exc = excinfo.value
    while current_exc:
        if re.search(match, str(current_exc)):
            return  # Success! Match found.
        current_exc = current_exc.__cause__

    # If the loop finishes without returning, no match was found.
    pytest.fail(f"Regex '{match}' not found in any exception in the chain.")