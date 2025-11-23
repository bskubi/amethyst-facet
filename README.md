**Facet** is an efficient utility for computing window aggregations on Amethyst HDF5 files produced via the [premethyst](https://github.com/adeylab/premethyst) pipeline.


### Create environment

Install `facet.py` dependencies using mamba:
```
mamba create -n facet pip && mamba activate facet && pip install amethyst-facet
```

### Ingest base-pair-resolution .parquet and .cov files

`facet calls2h5` will ingest base-pair-resolution methylation observations in the Scale Bio .parquet format as well as the legacy plaintext .cov format to the HDF5 format used by Amethyst. This can then be used to compute window aggregations using `facet agg`. Context and barcode can be flexibly parsed from the filename. Run `facet calls2h5 --help` for examples.


### Compute Window Aggregations

`facet agg` will add window aggregations to an existing HDF5 file in version 2.0.0 (see below for information on file format conversion). 

Example:
```
facet agg -u 500 -u step_1000=1000:250 -w special_fancy_windows=windows.tsv -p 55 *.h5
```

This computes several types of windows.

+ `-u 500` computes uniform non-overlapping 500bp windows. These will be stored in `/[context]/[barcode]/[window_size]` by default. A custom name can be chosen by prepending `-u [dataset_name]=500`.
+ `-u step_1000=1000:250` computes 1000bp windows with a 250bp step, so intervals will be computed at $[0, 1000), [250, 1250), ...$.  This example uses a custom name of `step_1000`. The default is to use `[window_size]_by_[step_size]`, which in this case would have been `1000_by_250`.
+ `-w special_fancy_windows=windows.tsv` computes aggregations over custom windows defined in a CSV-like file. The headers `chr`, `start` and `end` are required but the file format is sniffed by DuckDB (csv, tsv etc are allowed). Intervals are left-closed right-open, i.e. $[start, end)$ and may be overlapping and gapped.

The `-p 55` option parallelizes the computation using 55 worker cores. All HDF5 files retrieved via `*.h5` will have windows computed in this case. Multiple globs can be specified, i.e. `-glob path1/*.h5 -glob path2/*.h5`.

Other options are described in `facet agg --help`.

### Help

The options for facet.py can be explored at the command line by appending `--help`.

Example:
```
$ facet --help
Usage: facet.py [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  agg      Compute window sums over methylation observations stored in...
  convert  Convert an old Amethyst HDF5 file format to v2.0.0 format
  delete   Delete contexts, barcodes, or datasets from an Amethyst 2.0.0...
  version
```

You can also call `--help` on subcommands. Example:

```facet agg --help```

### Convert old Amethyst HDF5 file format to version 2.0.0

File format conversion is necessary prior to computing window aggregations using `facet.py` for Amethyst HDF5 files produced using earlier scripts.

Example:
```
facet convert old_format.h5 new_format.h5
```

#### Explanation and schema comparison:

The old Amethyst HDF5 format stored datasets under a cell barcode under a context group:

```
/[context]/[barcode]
```

`context` values are typically CH and CG. The `barcode` values are unique identifiers attributed to single cells. Typically each value of `barcode` is found in both the CH and CG contexts.

The schema of `barcode` was `chr`, `pos`, `pct`, `c`, `t`, with `chr` the chromosome name, `pos` the bp position of the observation, `pct` equal to `c/(c+t)`, and `c` and `t` the methylated and unmethylated count at that position. 

This gave no clear way to store window aggregations alongside the bp-resolution observations. We therefore altered the schema to:

```
/[context]/[barcode]/[dataset]
```

The bp-resolution observations are stored under the dataset `1` by default. Window aggregations are stored under their context and barcode under other names. The schema for window aggregations is `chr`, `start`, `end`, `c`, `t`, `c_nz`, `t_nz`. The `start` and `end` values denote the interval $[start, end)$. The `c` and `t` values store the sum of `c` and `t` counts for observed positions on that interval. Intervals with no observations are not reported. The `c_nz` and `t_nz` fields store the count of positions where `c >= 1` or `t >= 1` respectively.

### Delete datasets

Examples:

```
facet delete context CH *.h5
facet delete barcode AGCGAGCGAGCAHHCAHH *.h5
facet delete dataset 1 *.h5
```
