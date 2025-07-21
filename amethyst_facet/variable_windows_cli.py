import dataclasses as dc
import typing
import warnings

import duckdb
import polars as pl
import pathlib

import parse

import amethyst_facet as fct

@dc.dataclass
class VariableWindows:
    windows: typing.Dict[str, pl.DataFrame]

    def __post_init__(self):
        for chrom, df in self.windows.items():
            expected_cols = {"start", "end"}
            actual_cols = set(df.columns)
            matching_cols = actual_cols.intersection(expected_cols)
            has_expected_cols = matching_cols == expected_cols
            assert has_expected_cols, f"VariableWindows must have columns 'start', 'end', but for chr='{chrom}' had {df.columns}"

    def aggregate(
        self, 
        values: pl.DataFrame,
        agg = {"c": pl.sum, "t": pl.sum, "c_nz": pl.sum, "t_nz": pl.sum},
        start = "start",
        end = "end",
        pos = "pos",
        positive_cols = {"c": "c_nz", "t": "t_nz"}
        ) -> pl.DataFrame:
        """Compute aggregations on all chromosomes with at least one window

        Returns --\n
        polars DataFrame starting with position columns 'chr', 'start', 'end',
        sorted in order of these position columns.
        Remaining columns are value columns with chosen aggregations.
        """

        try:
            values_chroms = set(values["chr"].unique().to_list())
        except:
            raise Exception(f"Failed to identify unique chromosomes for values:\n{values}")
        try:
            windows_chroms = set(self.windows.keys())
            common_chroms = values_chroms.intersection(windows_chroms)
        except:
            raise Exception(f"Failed to identify common chroms from windows and values dataframe. Windows:\n{self.windows}\nValues:\n{values}")

        try:
            values = values.partition_by("chr", as_dict = True)
            values = {k[0]: v for k, v in values.items()}
        except:
            raise Exception(f"Failed to partition values by column 'chr'. Values:\n{values}")
        
        if not common_chroms:
            warnings.warn(
                f"No common chromosomes identified for current dataset and windows. "
                f"This may not be an error, but is often caused by a mismatch in how chromosome names are formatted "
                f"in the Amethyst H5 file and window file.\n"
                f"H5 file chromosome names:\n"
                f"{sorted(list(values_chroms))}\n"
                f"Window file chromosome names:\n"
                f"{sorted(list(windows_chroms))}"
            )

        result = []
        for chrom in common_chroms:
            chrom_windows = self.windows[chrom]
            chrom_values = values[chrom]
            chrom_values = chrom_values.drop("chr")
            chrom_result = fct.aggregate_variable(
                chrom_windows,
                chrom_values,
                agg,
                start,
                end,
                pos,
                positive_cols
            )
            cols = ["chr"] + chrom_result.columns
            chrom_result = chrom_result.with_columns(chr = pl.lit(chrom))
            chrom_result = chrom_result.select(cols)
            result.append(chrom_result)
    
        if not result:
            # Handle lack of matching chromosome names by returning an empty dataframe
            schema = {
                "chr": pl.String,
                "start": pl.Int64,
                "end": pl.Int64,
            }
            for key in agg:
                schema[key] = pl.Int64
            result = [pl.DataFrame(schema=schema)]

        try:
            result = pl.concat(result)
        except:
            raise Exception(f"Failed to concatenate per-chrom variable window aggregation results with result:\n{result}\nWindows:\n{self.windows}\nValues:\n{values}")

        try:
            result = result.sort("chr", "start", "end")
        except:
            raise Exception(f"Failed to sort per-chrom variable window aggregation results with result:\n{result}\nWindows:\n{self.windows}\nValues:\n{values}")

        return result

def parse_variable_windows(arg: str) -> VariableWindows:
    valid_formats = ["{name}={filename}", "{filename}"]
    howto_msg = f"To specify variable windows, use one of the following formats: {valid_formats}"
    format = "{name}={filename}" if "=" in arg else "{filename}"

    try:
        parsed = parse.parse(format, arg).named
    except:
        raise ValueError(f"Failed to parse variable windows CLI argument '{arg}'. {howto_msg}")

    context = f"For {arg} parsed to {parsed}"

    try:
        filename = parsed["filename"]
    except:
        # Not sure if we can reach this but leaving in for safety.
        raise ValueError(f"{context}, failed to parse filename. {howto_msg}")

    path = pathlib.Path(filename)
    assert path.exists(), f"{context}, parsed filename as '{filename}', but this file does not exist."
    
    try:
        windows = duckdb.read_csv(filename, sample_size=-1).pl()
        windows = windows.cast({"chr": pl.String, "start": pl.Int64, "end": pl.Int64})
    except:
        raise ValueError(f"Unable to load windows from {filename} to polars DataFrame using DuckDB CSV sniffer.")
    
    try:
        expected_cols = {"chr", "start", "end"}
        actual_cols = set(windows.columns)
        matching_cols = actual_cols.intersection(expected_cols)
        has_expected_cols = matching_cols == expected_cols
    except:
        # Not sure if we can reach this but leaving in for safety.
        raise ValueError(f"{context}, check for expected column names failed for windows:\n{windows}")

    assert has_expected_cols, (
        f"{context}, column header names 'chr', 'start', 'end' are required but were not found."
        f"Column names were {windows.columns}.\n"
        f"{windows}"
    )

    try:
        default_filename = path.name.removesuffix(path.suffix)
        name = parsed.get("name", default_filename).strip()
        assert name
    except:
        raise ValueError(f"{context} with path {path}, failed to parse name or use default name.")

    try:
        windows = windows.partition_by("chr", as_dict = True)
        windows = {k[0]:v for k, v in windows.items()}
    except:
        raise ValueError(f"{context} with path {path}, failed to partition windows by 'chr':\n{windows}")
    
    assert name.strip, f"VariableWindows must have name with non-whitespace characters, but had {name}"

    windows = VariableWindows(windows)
    return name, windows

    
        
