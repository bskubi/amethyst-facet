from dataclasses import dataclass
from typing import *

import click
import polars as pl

@click.command
@click.option("--parse", "parse_pattern",
              help="""Specify the filename format using placeholders for context and barcode components.

Format: {barcode1}_{barcode2}.{context}.parquet

- `{context}` represents the context of the file.
- `{barcode*}` represents one or more barcode components.

Example: `A1_B2.experiment.parquet`

This format allows the filename to be parsed using Python’s `parse` library.""")
@click.option("--barcode", help="Optional default barcode used if not extracted from filename")
@click.option("--context", help="Optional default context used if not extracted from filename")
@click.option("--format", help="""Optional Python code executed to return HDF5 group based on barcode list 'barcode' and context list 'context'.

              Example: --format \"f'/{context[0]}/{barcode[0]}{barcode[1]}'\"""")
@click.option("--dataset", default="1", show_default=True, help="Name of base-pair resolution dataset created for each context and barcode.")
@click.option("--glob", "globs", multiple=True, help="""Specify files to ingest as globs, to be combined with INPUT_PATHS argument.
              
              Example: --glob dir1/*.cov dir2/*.cov
              """)
@click.option("--overwrite", is_flag=True, default=True, show_default=True, help="""If HDF5_PATH exists, overwrite. Otherwise, new datasets are appended.""")
@click.option("--compression", default="gzip", show_default=True, help="""Compression algorithm applied to written HDF5 datsets.""")
@click.option("--compression_opts", default="6", show_default=True, help="Value of compression_opts argument for h5py create_dataset, specific to compression algorithm used.")
@click.option("--amethyst_version", default="amethyst2.0.0", show_default=True, help="Metadata string written in metadata/version dataset. Recommended to be left as default value.")
@click.option("--chr_col", default=0, show_default=True, help="Index of 'chr' column in source datasets")
@click.option("--pos_col", default=1, show_default=True, help="Index of 'pos' column in source datasets")
@click.option("--t_col", default=3, show_default=True, help="Index of 't' column in source datasets")
@click.option("--c_col", default=4, show_default=True, help="Index of 'c' column in source datasets")
@click.option("--delimiter", default="\t", show_default=True, help="Column delimiter character used in source datasets")
@click.argument("hdf5_path")
@click.argument("input_paths", nargs=-1)
def calls2h5(
    parse_pattern,
    barcode, 
    context, 
    format, 
    dataset, 
    globs, 
    overwrite, 
    compression, 
    compression_opts,
    amethyst_version,
    chr_col,
    pos_col,
    t_col,
    c_col,
    delimiter,
    hdf5_path, 
    input_paths):
    """Ingest parquet or plaintext .cov files to Amethyst v2.0.0 HDF5 format

    Arguments --
        HDF5_PATH: Path to destination Amethyst HDF5 file
        INPUT_PATHS: One or more paths (can be globs, i.e. *.parquet) for source files to ingest
    """
    # Unified list of filenames
    input_paths = list(input_paths) + list(globs)

    # Convert compression_opts to number if possible
    try:
        compression_opts = int(compression_opts)
    except:
        pass

    if not compression:
        compression = None
        compression_opts = None
    
    # Build list of source filenames and target HDF5 paths
    @dataclass
    class Map:
        source: str
        barcode: List[str]
        context: List[str]
        dataset: str
        target_func: Callable

        def target(self, context = None) -> str:
            """Construct an HDF5 dataset target"""
            context = context or self.context
            return self.target_func(context, self.barcode) + f"/{self.dataset}"

    maps = []

    # Build HDF5 group+dataset string based on filename and defaults 
    for filename in input_paths:
        # Set default values
        parsed_barcode = [barcode] if barcode else []
        parsed_context = [context] if context else []

        # Update using the parse library if a pattern is given
        if parse_pattern:
            named = parse.parse(parse_pattern, filename).named
            assert named, f"{filename} could not be parsed by {parse_pattern} -- check for typos"
            
            parsed_barcode = [v for k, v in named.items() if k.startswith("barcode")] or parsed_barcode
            parsed_context = [v for k, v in named.items() if k.startswith("context")] or parsed_context

        # While the .cov format had one file per context, the Scale Bio parquet format has a "context" column.
        # To deal with this, we still specify how to parse the barcode and context from the filename. However,
        # any "context" value parsed here for the parquet file will be replaced with the context value in the appropriate
        # parquet column when the dataset is actually written. 
        if format:
            target_func = lambda _context, _barcode: eval(format, {"barcode":_barcode, "context": _context})
        else:
            target_func = lambda _context, _barcode: f"/{''.join(_context)}/{''.join(_barcode)}"
        
        maps.append(Map(source=filename, barcode=parsed_barcode, context=parsed_context, dataset=dataset, target_func=target_func))
    
    # Ingest source files to HDF5 file
    mode = "w" if overwrite else "a"
    with h5py.File(hdf5_path, mode) as f:
        # Specify file format version
        metadata_group = f.create_group("metadata")
        metadata_group.create_dataset("version", data=amethyst_version)
        
        for map in maps:
            # Helper methods for loading data from parquet and .cov-style plaintext         
            def load_parquet(filename: str, dtype: np.dtype) -> np.recarray | None:
                try:
                    # Load from parquet file and convert to numpy recarray
                    dtype_names = [it[0] for it in dtype]
                    data = (
                        pl.read_parquet(filename)
                        .rename({"methylated":"c", "unmethylated":"t"})
                        .select(dtype_names + ["context"])
                        .partition_by("context", as_dict=True)
                    )

                    for context, df in data.items():
                        data[context] = (
                            df
                            .drop("context")
                            .sort("chr", "pos")
                            .to_numpy(structured=True)
                            .astype(dtype)
                        )
                except:
                    data = None
                return {context[0]: data for context, data in data.items()}

            def load_cov(filename: str, dtype: np.dtype, chr_col: int, pos_col: int, t_col: int, c_col: int, delimiter: str) -> np.recarray | None:
                try:
                    # Load from plaintext .cov format as numpy recarray and sort by chr, then pos
                    data = np.sort(
                        np.loadtxt(filename, delimiter=delimiter, usecols = [chr_col, pos_col, t_col, c_col], dtype=dtype),
                        order=['chr', 'pos']
                    )
                except:
                    data = None
                return data
        
            # Load data from parquet, and if that fails, try .cov-style plaintext
            dtype = [('chr', 'S10'), ('pos', int), ('t', int), ('c', int)]
            try:
                context_data = load_parquet(filename, dtype)
                
                # Write all contexts as individual datasets
                for context, data in context_data.items():
                    target = map.target([context])
                    f.create_dataset(target, data=data, compression=compression, compression_opts=compression_opts)
            except:
                data = load_cov(filename, dtype, chr_col, pos_col, t_col, c_col, delimiter)
                f.create_dataset(map.target(), data=data, compression=compression, compression_opts=compression_opts)
            assert data is not None, f"Failed to read data from {filename}. Parquet or .cov plaintext with dtype {dtype} expected."