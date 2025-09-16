"""Create Amethyst H5 dataset schemas for numpy and polars
"""
from typing import *

import polars as pl
import numpy as np
from pydantic import validate_call

class Formats:
    """Library-specific formats for dtypes used in Amethyst schema
    """
    # Codes for specific libraries
    NP = "numpy"
    PL = "polars"

    # Map Python dtype to library dtypes
    np_types = {str: "S10", int: np.int64} 
    pl_types = {str: pl.String, int: pl.Int64}

    @classmethod
    @validate_call
    def as_type(cls, t: Type[str|int], types: dict[type,Any]) -> Any:
        """Convert Python type to library-specific dtype
        """
        if t in types:
            return types[t]
        raise NotImplementedError(f"No type {t} in {types}.")

    @classmethod
    @validate_call
    def as_numpy(cls, t: Type[str|int]) -> Any:
        """Convert Python type to numpy-specific dtype
        """
        return cls.as_type(t, cls.np_types)
    
    @classmethod
    @validate_call
    def as_polars(cls, t: Type[str|int]) -> Any:
        """Convert Python type to polars-specific dtype
        """
        return cls.as_type(t, cls.pl_types)


class Schema:
    """Column names and Python dtypes

    CHR -- chromosome/contig name
    POS -- position of individual base or 
    C -- methylated or c count after deamination
    T -- unmethylated or t count after deamination
    C_NZ -- number of nonzero c positions observed in window
    T_NZ -- number of nonzero t positions observed in window
    
    Unused:
    STRAND -- strand of observation ('+' or '-')
    """
    CHR = ("chr", str)
    POS = ("pos", int)
    C = ("c", int)
    T = ("t", int)
    C_NZ = ("c_nz", int)
    T_NZ = ("t_nz", int)
    

    # Unused
    STRAND = ("strand", Literal["+", "-"])

    @classmethod
    @validate_call
    def create(cls, schema: Literal["bp", "agg"], format: Literal["base", "numpy", "polars"]):
        """Obtain schema in library-specific format

        Arguments:
            schema ("bp", "agg"): Whether to use bp or agg schema
            format ("base", "numpy", "polars"): Which library to format the schema for
        """
        # Create base schema
        match schema:
            case "bp": base_schema = [cls.CHR, cls.POS, cls.C, cls.T]
            case "agg": base_schema = [cls.CHR, cls.POS, cls.C, cls.T, cls.C_NZ, cls.T_NZ] 
            case _: raise NotImplementedError(f"Schema {schema} not implemented.")
        
        # Convert base schema to desired format
        match format:
            case "base": formatter = lambda x: x
            case "numpy": formatter = Formats.as_numpy
            case "polars": formatter = Formats.as_polars
            case _: raise NotImplementedError(f"Format {format} not implemented.")

        # Convert the base schema to the desired format
        formatted_schema = [(col, formatter(t)) for col, t in base_schema]
        return formatted_schema

    @classmethod
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def as_polars(cls, data: np.ndarray, schema: Literal["bp", "agg"]) -> pl.DataFrame:
        """Autodetect input type and convert to polars DataFrame
        """
        schema = cls.create(schema=schema, format="polars")
        return pl.from_numpy(data, schema=schema)
    
    @classmethod
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def as_numpy(cls, data: pl.DataFrame, schema: Literal["bp", "agg"]) -> np.recarray:
        """Autodetect input type and convert to numpy recarray
        """
        schema = cls.create(schema=schema, format="numpy")
        return data.to_numpy(structured=True).astype(schema)


