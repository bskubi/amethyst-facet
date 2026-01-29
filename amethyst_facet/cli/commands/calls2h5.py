from dataclasses import dataclass
import itertools
from enum import Enum
from typing import *
from pathlib import Path
from contextlib import contextmanager
from abc import ABC, abstractmethod
import sys
from warnings import warn

import click
import polars as pl
import parse
import h5py
import numpy as np
from loguru import logger
from pydantic import BaseModel, FilePath, validate_call, model_validator, Field, BeforeValidator, PlainSerializer, ConfigDict, InstanceOf

import amethyst_facet.errors


AMETHYST_H5_DTYPE: Final = [('chr', 'S10'), ('pos', int), ('t', int), ('c', int)]
AMETHYST_H5_SORT_BY: Final = ["chr", "pos"]

class CovSchema(BaseModel):
    chr: int = Field(0, ge=0)
    pos: int = Field(1, ge=0)
    pct: int = Field(2, ge=0)
    t: int = Field(3, ge=0)
    c: int = Field(4, ge=0)
    delimiter: str = Field("\t", min_length=1)

@validate_call
def cov_to_amethyst_data( path_to_cov: FilePath, cov_schema: CovSchema = CovSchema()) -> np.ndarray:
    """Extract Amethyst data from .cov file

    Returns:
        numpy.ndarray: "chr", "pos", "t", "c" columns from .cov file sorted in ascending order,
        lexicographically by "chr", then numerically by "pos"
    """

    # 1. Load required data from .cov
    cov_data = np.loadtxt(
        fname = path_to_cov,
        delimiter = cov_schema.delimiter,
        usecols = [cov_schema.chr, cov_schema.pos, cov_schema.t, cov_schema.c],
        dtype = AMETHYST_H5_DTYPE
    )
    
    # 2. Sort .cov data by chr, then pos within chr
    sorted_cov_data = np.sort(
        a = cov_data, 
        order = AMETHYST_H5_SORT_BY
    )

    return sorted_cov_data

def to_numpy(v: Any) -> np.ndarray:
    if isinstance(v, np.ndarray):
        return v
    return np.array(v)

def serialize_numpy(v: np.ndarray) -> list:
    return v.tolist()

NumpyArray = Annotated[
    InstanceOf[np.ndarray],
    BeforeValidator(to_numpy),
    PlainSerializer(serialize_numpy, return_type=list)
]

class AmethystDatasetV2(BaseModel):
    """
    | **context (str)**: Methylation context, typically CG or CH.
    | **barcode (str)**: Unique cell barcode.
    | **name (str)**: Dataset name. By convention, '1' is used for bp-level (unaggregated) observations.
    | **data (numpy.ndarray):**
    columns "chr" (S10), "pos" (int), "t" (int), "c" (int)
    + t is the count of unmethylated calls at (chr, pos)
    + c is the count of methylated calls at (chr, pos)
    """

    context: str = Field(description = "Methylation context, typically CG or CH.")
    barcode: str = Field(description = "Unique cell barcode.")
    name: str = Field(default = "1", description = "Dataset name. By convention, '1' is used for bp-level (unaggregated) observations.")
    data: NumpyArray | None = Field(
        default = None,
        description = """
        **Columns:** columns "chr" (S10), "pos" (int), "t" (int), "c" (int)
        + t is the count of unmethylated calls at (chr, pos)
        + c is the count of methylated calls at (chr, pos)
        """
    )
    data_source: Optional[Any] = None

    @staticmethod
    def from_h5_dataset(dataset: h5py.Dataset, load_data: bool = True) -> "AmethystDatasetV2":
        """Load AmethystDatasetV2 object from h5py.Dataset object
        """
        try:
            _, context, barcode, name = dataset.name.split("/")
        except Exception as e:
            e.add_note(f"Dataset name should be formatted as /context/barcode/name")
            raise
        data = dataset[:] if load_data else None
        
        return AmethystDatasetV2(context = context, barcode = barcode, name = name, data = data, data_source = dataset.file.filename)

    @property
    def absolute_name(self) -> str:
        """Absolute dataset name in Amethyst H5 file: /context/barcode/name
        """
        return f"/{self.context}/{self.barcode}/{self.name}"


class BaseAmethystDataSource(BaseModel, ABC):
    data_source: Optional[Any] = None
    model_config = ConfigDict(str_strip_whitespace = True)

    @abstractmethod
    @validate_call
    def datasets(self, load_data: bool = True) -> Generator[AmethystDatasetV2, None, None]:
        """Extract datasets with data from file at input_path

        Arguments:
            load_data: If true, load data from file at input_path. If false, only return context, barcode, and name.
        """
        ...

class CovSource(BaseAmethystDataSource):
    path: FilePath
    context: str
    barcode: str
    name: str
    cov_schema: CovSchema = CovSchema()

    @validate_call
    def datasets(self, load_data: bool = True) -> Generator[AmethystDatasetV2, None, None]:
        """Extract Amethyst dataset from .cov file

        Returns:
            Generator[AmethystDatasetV2, None, None]: "chr", "pos", "t", "c" columns from .cov file sorted in ascending order,
            lexicographically by "chr", then numerically by "pos"
        """
        if load_data:
            schema = [None]*5
            schema[self.cov_schema.chr] = ("chr", pl.String)
            schema[self.cov_schema.pos] = ("pos", pl.Int64)
            schema[self.cov_schema.pct] = ("pct", pl.Float64)
            schema[self.cov_schema.t] = ("t", pl.Int64)
            schema[self.cov_schema.c] = ("c", pl.Int64)
            schema = {k: v for k, v in schema}

            data = (
                pl.read_csv(
                    source = self.path, 
                    separator = self.cov_schema.delimiter, 
                    schema = schema,
                    has_header = False
                )
                .drop("pct")
                .sort("chr", "pos")
                .to_numpy(structured=True)
                .astype(AMETHYST_H5_DTYPE)
            )

        else:
            data = None

        yield AmethystDatasetV2(
            context = self.context, 
            barcode = self.barcode, 
            name = self.name, 
            data = data, 
            data_source = self.data_source
        )
    
    def source_name(self) -> list[Path]:
        return [self.cov_path]

class ScaleMethylParquetSource(BaseAmethystDataSource):
    path: FilePath
    barcode: str
    name: str

    @validate_call
    def datasets(self, load_data: bool = True) -> Generator[AmethystDatasetV2, None, None]:
        """Extract Amethyst datasets from .parquet file generated by ScaleMethyl pipeline

        ScaleMethyl .parquet files are for individual cells, but include a 'context' column
        that might be CG, CH, etc. Each context results in a separate AmethystDatasetV2 object.

        Returns:
            Generator[AmethystDatasetV2, None, None]: "chr", "pos", "t", "c" columns from .cov file sorted in ascending order,
            lexicographically by "chr", then numerically by "pos"
        """
        if load_data:
            # Load from parquet file and convert to numpy recarray
            data = (
                pl.read_parquet(self.path)
                .rename({"methylated":"c", "unmethylated":"t"})
                .select("chr", "pos", "t", "c", "context")
            )

            # Get one dataframe per context
            context_dataset_dfs = data.partition_by("context", as_dict=True)
            for context, dataset_df in context_dataset_dfs.items():
                # Convert dataframe to sorted numpy array with columns 'chr', 'pos', 't', 'c'
                data = (
                    dataset_df
                    .drop("context")
                    .sort("chr", "pos")
                    .to_numpy(structured=True)
                    .astype(AMETHYST_H5_DTYPE)
                )

                # Yield one dataset per context
                yield AmethystDatasetV2(context = context, barcode = self.barcode, name = self.name, data = data)
        else:
            # Yield one dataset per context, but don't return the data
            data = (
                pl.read_parquet(self.path)
                .select("context")
            )
            for context in data["context"].unique():
                yield AmethystDatasetV2(
                    context = context, 
                    barcode = self.barcode, 
                    name = self.name,
                    data_source = self.data_source
                )


class AmethystH5Source(BaseAmethystDataSource):
    path: FilePath

    @validate_call
    def datasets(self, load_data: bool = True) -> Generator[AmethystDatasetV2, None, None]:
        # Helper function to recurse through the hierarchy
        def _recursive_yield(group_or_file):
            # Iterate over immediate children
            for name, obj in group_or_file.items():
                
                if isinstance(obj, h5py.Group):
                    # If Group: Recurse down
                    yield from _recursive_yield(obj)
                    
                elif isinstance(obj, h5py.Dataset):
                    # If Dataset: Check filter and yield
                    if obj.name != "/metadata/version":
                        yield AmethystDatasetV2.from_h5_dataset(obj, load_data)

        with h5py.File(self.path, "r") as h5_file:
            yield from _recursive_yield(h5_file)

class AmethystSourceCombiner(BaseAmethystDataSource):
    sources: list[BaseAmethystDataSource]
    
    @validate_call
    def datasets(self, load_data: bool = True) -> Generator[AmethystDatasetV2, None, None]:
        """Yield from the datasets method of all sources using common value of load_data.
        """
        for source in self.sources:
            yield from source.datasets(load_data = load_data)

class ConflictHandler(str, Enum):
    ERROR = "ERROR" # Raise error on conflict
    OVERWRITE = "OVERWRITE" # Overwrite previous dataset on conflict
    SKIP = "SKIP"   # Keep previous dataset on conflict


class AmethystH5Inserter(BaseModel):
    """Insert source files (.cov, ScaleMethyl .parquet, other Amethyst H5 objects) into target Amethyst H5 v2 file

    Attributes:
        source_combiner (AmethystSourceCombiner): A combination of multiple sources of input data for the Amethyst H5 v2 file.

    Example:
    ```
    # Build inserter with references to input source files
    inserter = AmethystH5Inserter(
        source_combiner = AmethystSourceCombiner(
            sources = [
                CovSource(path = "cell1.CG.cov", context = "CG", barcode = "cell1", name = "1"),
                CovSource(path = "cell1.CH.cov", context = "CH", barcode = "cell1", name = "1"),
            ]
        )
    )
    # Checks for dataset name collisions and inserts data from sources into cells.h5 in Amethyst v2.0.0 format
    # Adds /metadata/version="amethyst2.0.0" if it does not already exist.
    inserter.sources_to_h5(target_amethyst_h5_path = "cells.h5")
    ```
    """
    source_combiner: AmethystSourceCombiner

    @validate_call
    def insert_from_sources(
        self, 
        target_amethyst_h5_path: Path, 
        compression: str = "gzip", 
        compression_opts: Any = 6, mode: str = "a",
        source_target_dataset_name_conflict_handler: ConflictHandler = ConflictHandler.ERROR,
        dry_run = False

    ):
        """Extract data from sources and insert into the H5 file at amethyst_h5_path

        Arguments:
            target_amethyst_h5_path: Amethyst H5 file to be written or appended to.
            compression: 'compression' argument for h5py.create_dataset
            compression_opts: 'compression_opts' argument for h5py.create_dataset
            source_target_dataset_name_conflict_handler: Behavior when a source dataset has the same
                name as a dataset in the target Amethyst H5 file (only relevant if the target H5 file exists)
            dry_run: If true, simulates run without modifying files.

        Raises:
            ValueError: Duplicate absolute dataset names found across input sources, or
            input sources collide with datasets that already exits in target Amethyst H5 file.
        """
        log_prefix = "[dry run] " if dry_run else ""

        # Make sure that input sources do not conflict across input sources or with 
        # existing datasets in the output H5 file if appending to an existing file
        # and if dataset name conflicts with the target Amethyst H5 object should raise an error.
        self.detect_dataset_name_collisions(
            target_amethyst_h5_path = (
                target_amethyst_h5_path
                if (
                    source_target_dataset_name_conflict_handler == ConflictHandler.ERROR
                    and target_amethyst_h5_path.exists()
                )
                else None
            )
        )

        # If file already exists, require that /metadata/version == "amethyst2.0.0"
        version_dataset_name = "/metadata/version"
        required_version = "amethyst2.0.0"
        first_written = False
        if target_amethyst_h5_path.exists():
            with h5py.File(target_amethyst_h5_path, "r") as h5_file:
                if version_dataset_name not in h5_file:
                    raise ValueError(
                        f"{log_prefix}Target Amethyst H5 file at {target_amethyst_h5_path.absolute()} "
                        "exists but lacks /metadata/version, suggesting it is old V1 format. "
                        "First convert to V2 with facet convert (see facet convert --help for details). "
                    )
                elif version_string := h5_file[version_dataset_name].asstr()[()] != required_version:
                    # Load actual /metadata/version string from the target file
                    
                    raise ValueError(
                        f"{log_prefix}Target Amethyst H5 file at {target_amethyst_h5_path.absolute()} "
                        f"exists but its /metadata/version is {version_string}, which "
                        f"does not match the required {required_version}."
                    )

        logger.info("{}Writing source data to {}.", log_prefix, target_amethyst_h5_path)

        if dry_run:
            mode = "r"

        # Sequentially load and insert all datasets into the target H5 file.
        with h5py.File(name = target_amethyst_h5_path, mode = mode) as h5_file:

            # Add metadata with Amethyst format version if it doesn't already exist.
            if version_dataset_name not in h5_file and not dry_run:
                h5_file.create_dataset(version_dataset_name, data = required_version)

            # Iteratively load data from sources and write to the target as new datasets
            for dataset in self.source_combiner.datasets(load_data = True):
                if dataset.absolute_name in h5_file:
                    if source_target_dataset_name_conflict_handler == ConflictHandler.OVERWRITE:
                        logger.info("{}original dataset at {}", log_prefix, dataset.absolute_name)
                        if not dry_run:
                            del h5_file[dataset.absolute_name]
                    elif source_target_dataset_name_conflict_handler == ConflictHandler.SKIP:
                        logger.info("{}Skipping write of {} as it is already present in {}", log_prefix, dataset, target_amethyst_h5_path)
                        continue
                
                logger.info("{}Writing {} to {}", log_prefix, dataset, dataset.absolute_name)
                if not dry_run:
                    h5_file.create_dataset(
                        name = dataset.absolute_name,
                        data = dataset.data,
                        compression = compression,
                        compression_opts = compression_opts
                    )
                    if not first_written:
                        logger.info(
                            "First dataset written. Here is a sample of it as loaded from the H5 file:\n{}", 
                            pl.from_numpy(h5_file[dataset.absolute_name][:])
                        )
                        first_written = True

    def detect_dataset_name_collisions(self, target_amethyst_h5_path: Path | None = None):
        """Raise an exception if any dataset names collide across the input sources.

        Raises:
            ValueError: Duplicate absolute dataset names found across input sources.
        """
        # Store absolute h5 dataset names discovered over all input sources.
        absolute_names: dict[str, AmethystDatasetV2] = {}

        # Iterate through source datasets without loading data
        datasets = [self.source_combiner.datasets(load_data = False)]

        # If appending to existing Amethyst H5 file, check for collisions between
        # existing datasets and input sources.
        if check_output_path := target_amethyst_h5_path and target_amethyst_h5_path.exists():
            output_as_source = AmethystH5Source(path = target_amethyst_h5_path)
            datasets.append(output_as_source.datasets(load_data = False))

        for dataset in itertools.chain.from_iterable(datasets):
            # 1. Get current dataset absolute name
            absolute_name = dataset.absolute_name

            # 2. Check if it was already found
            source1 = absolute_names.get(absolute_name)
            source2 = dataset

            # 3. Add it to the stored names, along with the dataset object itself for logging,
            # or raise a ValueError if a duplicate was found.
            if source1 is None:
                absolute_names[absolute_name] = dataset
            else:
                raise ValueError(
                    f"{absolute_name} found in two places: {source1} (loaded first) "
                    f"and {source2} (loaded second, caused collision)."
                )              
        
        # No duplicates found -- success.

class ContextBarcode(BaseModel):
    context: str | None = Field(description = "Context for the dataset (i.e. CG, CH)")
    barcode: str | None = Field(description = "Complete barcode string for the dataset")
    model_config = ConfigDict(str_strip_whitespace = True)

@validate_call
def extract_amethyst_group_from_path(
        path: Path, 
        path_parse_formats: Optional[list[str]] = None,
        barcode_format: Optional[str] = None,
        default_context: Optional[str] = None, 
        default_barcode: Optional[str] = None,
        require_context: bool = False,
        require_barcode: bool = False
    ) -> ContextBarcode:
    """Extract context or barcode from path string.

    Arguments:
        path (Path): Path from which context or barcode are extracted. Will be converted to a string and parts extracted as specified
            at the terminal.
        path_parse_formats (list[str]): List of one or more strings used by the parse library
            to extract named parts of path_string based on curly-brace placeholders.
        barcode_format (str): String used to produce formatted barcode string based on named parts
            extracted from path_string by an element in path_parse_formats.
        default_context (str): Default value to use if unable to extract a context.
        default_barcode (str): Default value to use if unable to extract a barcode.

    Returns: 
        ContextBarcode: Stores final context and barcode.
    
    Raises:
        ValueError: When multiple values of path_parse_formats are passed in and generate a conflicting final value of 'context' or 'barcode'.
    """
    path_string: str = str(path)
    # For conflict detection and debugging, stores
    # the most recently detected context and barcode
    # so that in case of conflict, the next_path_format
    # strings that generated the conflict can both be displayed.
    context: str = None
    context_format: str | None
    barcode: str = None
    barcode_format: str | None
    
    # Iterate through next_path_format strings
    # Extract the named parts from the path string
    # Example:
    #   path = "./data/ACGT.CATA.CAAA.CG.cov",
    #   path_parse_formats = ["{ignore}/{barcode1}.{barcode2}.{barcode3}.{context}.cov"],
    #   barcode_format_pattern = "{barcode1}_{barcode2}_{barcode3}"
    # Returns ContextBarcode(context = "CG", barcode = "")
    # it will extract context = "CG" and barcode = "ACGT_CATA_CAAA".
    # Note that . in the path has been replaced by _ in the barcode.
    # Supports multiple path parsers as long as they do not deliver conflicting results.
    for next_path_format in (path_parse_formats or []):

        # 1. Extract named parts from string.
        parsed = parse.parse(format = next_path_format, string = path_string)
        if not parsed or not parsed.named:
            continue
        
        # 2. Attempt to extract context and ensure it does not conflict.
        if context is None:
            context = parsed.named.get("context")
        elif next_context := parsed.named.get("context") is not None and context != next_context:
            raise ValueError(
                f"Attempted to parse context from file at absolute path {path_string.absolute()} "
                f"using input path string {path_string}. However, --parse {context_format} extracted '{context}' "
                f"while --parse {next_path_format} extracted '{next_context}'. Choose non-conflicting values of --parse "
                f"or source paths that do not generate this conflict."
            )

        # Store the format string used to extract named parts for debugging
        context_format = next_path_format

        # 3. Attempt to format barcode and ensure it does not conflict.
        # Note that unneeded keys in parsed.named are ignored, but
        # missing keys in barcode_format_pattern raise an exception.
        next_barcode = barcode_format.format(**parsed.named)
        if barcode is None:
            barcode = next_barcode
        elif next_barcode is not None and barcode != next_barcode:
            raise ValueError(
                f"Attempted to parse barcode from file at absolute path {path.absolute()} "
                f"using input path string {path_string}. However, --parse {barcode_format} extracted '{barcode}' "
                f"while --parse {next_path_format} extracted '{next_barcode}'. Choose non-conflicting values of --parse "
                f"or source paths that do not generate this conflict."
            )
        
        # Store the format string used to generate the barcode.
        barcode_format = next_path_format
    
    barcode = barcode or default_barcode
    context = context or default_context
    
    if require_context and not context:
        raise ValueError("Context required but valid value could not be extracted from path or obtained from default.")
    
    if require_barcode and not barcode:
        raise ValueError("Barcode required but valid value could not be extracted from path or obtained from default.")

    return ContextBarcode(context = context, barcode = barcode)

@click.command
@click.option(
    "--parse",
    "path_parser_formats",
    multiple = True,
    help=(
"""Extract context or barcode from source paths using placeholders in curly brackets.
Placeholders should typically be 'context' or 'barcode', optionally followed by an integer. 'context' will always
be treated as the context, while others can be used arbitrarily in --barcode-format is passed. Can pass --parse multiple times.
Parsers are ignored if they do not match the input format. Raises an exception if two or more
parsers give different outputs for a given input file. If multiple context or barcode placeholders are passed,
they will be concatenated using the template given by the --barcode-format option. String extracted as {context} 
will be used without alteration. Note: You can supply arbitrary placeholders. Placeholders not needed by --barcode-format will be ignored,
but missing placeholders required by --barcode-format will raise an error.
"""
    )
)
@click.option(
    "--barcode-format",
    type = str,
    default = "{barcode}",
    show_default = True,
    help = (
"""How to format placeholders extracted from source paths using --parse as the barcode stored in the Amethyst H5 file.
Pass a string with the same placeholders used in --parse.
"""
    )
)
@click.option("--default-context", "--context", "default_context", help="Default context used if not extracted from file or filename")
@click.option("--default-barcode", "--barcode", "default_barcode", help="Default barcode used if not extracted from filename")
@click.option("--dataset", "--name", "dataset_name", default="1", show_default=True, help="Name of base-pair resolution dataset created for each context and barcode.")
@click.option(
    "--glob", 
    "globs", 
    multiple=True, 
    help= (
"""Specify files to ingest as globs, to be combined with INPUT_PATHS argument.

Example:
facet --glob ./dir1/*.cov --glob ./dir2/*.cov cells.h5
"""

    )
)
@click.option("--overwrite/--append", "overwrite", is_flag=True, default=True, show_default=True, help="""If HDF5_PATH exists, overwrite. Otherwise, new datasets are appended.""")
@click.option("--compression", default="gzip", show_default=True, help="""Compression algorithm applied to written HDF5 datsets.""")
@click.option("--compression_opts", default="6", show_default=True, help="Value of compression_opts argument for h5py create_dataset, specific to compression algorithm used.")
@click.option("--cov-chr-col", default=0, show_default=True, help="Index of 'chr' column in .cov source datasets")
@click.option("--cov-pos-col", default=1, show_default=True, help="Index of 'pos' column in .cov source datasets")
@click.option("--cov-pct-col", default=2, show_default=True, help="Index of 'pct' column in .cov source datasets")
@click.option("--cov-t-col", default=3, show_default=True, help="Index of 't' column in .cov source datasets")
@click.option("--cov-c-col", default=4, show_default=True, help="Index of 'c' column in .cov source datasets")
@click.option("--cov-delimiter", default="\t", show_default=True, help="Column delimiter character used in .cov source datasets")
@click.option(
    "--source-target-dataset-name-conflict-handler", 
    type=click.Choice(choices = [ConflictHandler.ERROR, ConflictHandler.OVERWRITE, ConflictHandler.SKIP]),
    default=ConflictHandler.ERROR,
    show_default=True,
    help = (
"""Behavior when dataset names conflict with existing names in the target Amethyst H5 file when appending.
No effect when overwriting the target. Note that source files are still not allowed to have conflicting dataset names.
Dataset name refers to the full /context/barcode/name path within the Amethyst H5 object.

Options:
    error: Raise an error. 
    overwrite: Replace the original Amethyst H5 dataset with the new one.
    skip: Keep the original Amethyst H5 dataset unchanged and ignore the new one.
"""
    )
)
@click.option("--dry-run", is_flag = True, default=False, help="Run calls2h5 as dry run (files will not be changed)")
@click.argument("target_amethyst_h5_path")
@click.argument("source_paths", nargs=-1)
def calls2h5(
    path_parser_formats,
    barcode_format,
    default_context,
    default_barcode,
    dataset_name,
    globs, 
    overwrite, 
    compression, 
    compression_opts,
    cov_chr_col,
    cov_pos_col,
    cov_pct_col,
    cov_t_col,
    cov_c_col,
    cov_delimiter,
    source_target_dataset_name_conflict_handler,
    dry_run,
    target_amethyst_h5_path, 
    source_paths):
    """Ingest ScaleMethyl pipeline parquet files, plaintext .cov files, or other Amethyst H5 v2.0.0 files to Amethyst v2.0.0 HDF5 format

    \b
    Arguments:
        TARGET_AMETHYST_H5_PATH: Path to an Amethyst HDF5 file that source data will be written to.
        SOURCE_PATHS: One or more paths (can be globs, i.e. *.parquet) for source files to ingest.

    \b
    Note: SOURCE_PATHS may include gzipped .cov files, and gzipping .cov files 
    may improve runtime and reduce disk space consumption considerably.

    \b
    Example:

    \b
    facet --parse {ignore}/{barcode1}.{barcode2}.{barcode3}.{context}.cov \\
        --parse {ignore}/{barcode1}.{barcode2}.{barcode3}.parquet \\
        --barcode-format {barcode1}_{barcode2}_{barcode3} 
        cells.h5 \\
        ./sources/cov/ACTG_CATA_TTAA.CG.cov \\
        ./sources/cov/ACTG_CATA_TTAA.CH.cov \\
        ./sources/scale_methyl_parquet/CAGG_GGAA_ACAA.parquet

    \b
    This will create the following datasets (assuming the parquet file has CG and CH contexts):
    /CG/ACTG_CATA_TTAA/1
    /CH/ACTG_CATA_TTAA/1
    /CG/CAGG_GGAA_ACAA/1
    /CH/CAGG_GGAA_ACAA/1
    """
    if dry_run:
        logger.info("-----------Calls2h5 DRY RUN-----------")
    else:
        logger.info("-----------Calls2h5-----------")

    if overwrite and source_target_dataset_name_conflict_handler == ConflictHandler.SKIP:
        raise ValueError(
            "Both --overwrite and --source-target-dataset-name-conflict-handler SKIP were passed to facet calls2h5, "
            "but SKIP is irrelevant if --overwrite is passed. This is because --overwrite causes the target H5 file "
            "to be deleted and replaced by the new data, while SKIP causes source datasets that have conflicts "
            "with the target dataset to be skipped. SKIP is relevant only on --append, not --overwrite. "
            "This suggests you may have meant to use a different conflict handler or to pass --append. "
            "Aborting facet calls2h5 out of caution."
        )

    # Unified list of filenames
    source_paths = [Path(path) for path in list(source_paths) + list(globs)]

    # Build schema for .cov files
    cov_schema = CovSchema(
        chr = cov_chr_col,
        pos = cov_pos_col,
        pct = cov_pct_col,
        t = cov_t_col,
        c = cov_c_col,
        delimiter = cov_delimiter
    )

    # Get context, barcode, dataset name for all source files
    sources = []
    for source_path in source_paths:
        handled_suffixes = set([".cov", ".parquet", ".h5"])
        found_suffixes = set(source_path.suffixes)
        suffix = handled_suffixes.intersection(found_suffixes)
        if len(suffix) != 1:
            raise ValueError(f"Found zero or multiple handled suffixes. Suffixes must include exactly one of {handled_suffixes}.")
        suffix = list(suffix)[0]

        match suffix:
            case ".cov":
                context_barcode = extract_amethyst_group_from_path(
                    source_path,
                    path_parser_formats,
                    barcode_format,
                    default_context,
                    default_barcode,
                    require_context = True,
                    require_barcode = True
                )
                # .cov files are an Adey lab tab-delimited tabular plaintext format.
                # Columns: chr (string), pos (int), pct (float), t (int), c (int)
                source = CovSource(
                    path = source_path, 
                    context = context_barcode.context, 
                    barcode = context_barcode.barcode, 
                    name = dataset_name, 
                    cov_schema = cov_schema,
                    data_source = source_path
                )
            case ".parquet":
                context_barcode = extract_amethyst_group_from_path(
                    source_path,
                    path_parser_formats,
                    barcode_format,
                    default_context,
                    default_barcode,
                    require_context = False,
                    require_barcode = True
                )
                # .parquet files are assumed to be from the ScaleMethyl pipeline.
                # They have the same columns as .cov files and also a context (str)
                # file that supplies the context. We therefore don't extract context
                # from the path.
                source = ScaleMethylParquetSource(
                    path = source_path,
                    barcode = context_barcode.barcode,
                    name = dataset_name,
                    data_source = source_path
                )
            case ".h5":
                # Extract from source Amethyst .h5 files to target.
                source = AmethystH5Source(
                    path = source_path,
                    data_source = source_path
                )
            case _:
                # Error on unhandled file extensions.
                raise NotImplementedError(
                    f"{source_path} has disallowed file extension. "
                    "Source files should be *.cov, *.parquet (ScaleMethyl pipeline output), or *.h5."
                )
        # Add the extracted source to the list of sources
        sources.append(source)

    # The inserter object facilitates extracting data for Amethyst h5 datasets
    # from one or more input sources of a variety of input files, checking for name conflicts.
    inserter = AmethystH5Inserter( source_combiner = AmethystSourceCombiner(sources = sources) )
    
    # Convert compression_opts to number if possible,
    # otherwise use as-is
    try:
        compression_opts = int(compression_opts)
    except:
        pass

    if not compression:
        compression = None
        compression_opts = None

    inserter.insert_from_sources(
        target_amethyst_h5_path, 
        mode = ("w" if overwrite else "a"), 
        compression = compression, 
        compression_opts = compression_opts,
        source_target_dataset_name_conflict_handler = source_target_dataset_name_conflict_handler,
        dry_run = dry_run
    )
