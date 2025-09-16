import numpy as np
from numpy.testing import assert_array_equal
import polars as pl
from polars.testing import assert_frame_equal
import pytest

import amethyst_facet as fct

df_bp = pl.DataFrame({
    "chr": ["1", "1", "2", "2"], 
    "pos": [1, 2, 3, 4], 
    "c": [0, 1, 0, 1],
    "t": [1, 0, 1, 0]
})

df_agg = df_bp.with_columns(c_nz = pl.Series([0, 1, 0, 1]), t_nz = pl.Series([1, 0, 1, 0]))

@pytest.mark.parametrize("df1, schema", [(df_bp, "bp"), (df_agg, "agg"),])
def test_roundtrip_convert(df1, schema):
    """Test round-trip conversions between bp-format DataFrame and recarray
    """
    # Run round-trip conversions
    arr1 = fct.Schema.as_numpy(df1, schema = schema)
    df2 = fct.Schema.as_polars(arr1, schema = schema)
    arr2 = fct.Schema.as_numpy(df2, schema = schema)

    # Check dtypes, i.e. for c vs. t column order mismatch
    assert df1.schema == df2.schema, f"DataFrame schemas do not match: {df1.schema}, {df2.schema}"
    assert arr1.dtype == arr2.dtype, f"Array dtypes do not match: {arr1.dtype}, {arr2.dtype}"

    # Check all values in the dataframe/array
    assert_frame_equal(df1, df2), f"Round-trip test from DataFrame -> ndarray -> DataFrame failed:\n{df1}\n{arr1}\n{df2}"
    assert_array_equal(arr1, arr2), f"Round-trip test from ndarray -> DataFrame -> ndarray failed:\n{arr1}\n{df2}\n{arr2}"