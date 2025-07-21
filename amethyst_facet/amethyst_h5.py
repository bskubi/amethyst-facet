import dataclasses as dc
import polars as pl
from numpy.typing import NDArray
import time
from typing import *
import warnings

import h5py

import amethyst_facet as fct

cov_dtype = [("chr", "S10"), ("pos", int), ("c", int), ("t", int)]
amethyst_dtype = [("chr", "S10"), ("start", int), ("end", int), ("c", int), ("t", int), ("c_nz", int), ("t_nz", int)]

handles = dict()

@dc.dataclass
class AmethystH5Dataset:
    filename: str
    context: str
    barcode: str
    observations: str
    dataset: NDArray
    decode: bool = True

    def pl(self) -> pl.DataFrame:
        """Return a sorted polars DataFrame representation of data

        If self.decode is True (default), converts 'chr' column to type string,
        as it is a binary fixed-size string in the Amethyst H5 file.
        """
        df = pl.from_numpy(self.dataset)
        df = df.sort("chr", "pos")
        if self.decode:
            df = df.cast({"chr":pl.String})
        return df

@dc.dataclass
class AmethystH5ReaderV2:
    filenames: List[str]
    observations: str = "1"
    contexts: List[str] | None = None
    skip_bc: Set[str] = dc.field(default_factory=set)
    require_bc: Set[str] = dc.field(default_factory=set)

    def __iter__(self) -> Generator[AmethystH5Dataset]:
        for filename in self.filenames:
            try:
                if filename in handles:
                    file = handles[filename]
                else:
                    file = h5py.File(filename, "a")
                    handles[filename] = file
                
                try:
                    version = file['metadata']['version'][()].decode()
                except Exception as e:
                    raise ValueError(
                        f"Invalid or outdated Amethyst H5 file {filename} with no /metadata/version string. "
                        f"If using an old Amethyst h5 object, use facet convert to convert it to the updated format."
                    ) from e
                assert version == "amethyst2.0.0", f"Invalid amethyst version string: {version} != amethyst2.0.0"

                try:
                    if self.contexts:
                        contexts = self.contexts
                    else:
                        contexts = set(file["/"])
                        contexts.remove("metadata")
                except Exception as e:
                    raise ValueError(
                        f"Failed to read contexts from Amethyst H5 file {filename}"
                    ) from e
                
                for context in contexts:
                    try:
                        if self.require_bc:
                            barcodes = self.require_bc
                        else:
                            barcodes = set(file[context])
                            barcodes.difference_update(self.skip_bc)
                    except KeyError:
                        # If a given context isn't present in the file, skip it
                        continue
                    except Exception as e:
                        raise ValueError(f"Failed to obtain barcodes in {filename}/{context} using {self}") from e
                    
                    for barcode in barcodes:
                        try:
                            group = file[context][barcode]
                        except KeyError:
                            # If a given barcode isn't present in the given context, skip it.
                            continue
                        try:
                            data_array = group[self.observations][:]
                            dataset = AmethystH5Dataset(
                                filename,
                                context,
                                barcode,
                                self.observations,
                                data_array
                            )
                            yield dataset
                        except Exception as e:
                            raise ValueError(f"Failed to load dataset from {filename}/{context}/{barcode}/{self.observations} using {self}") from e
            except Exception as e:
                raise ValueError(f"Could not open {filename} as H5 file with h5py") from e


@dc.dataclass
class AmethystH5ReaderV1:
    filenames: List[str]
    contexts: List[str] | None = None
    skip_bc: Set[str] = dc.field(default_factory=set)
    require_bc: Set[str] = dc.field(default_factory=set)

    def __iter__(self) -> Generator[AmethystH5Dataset]:
        for filename in self.filenames:
            try:
                if filename in handles:
                    file = handles[filename]
                else:
                    file = h5py.File(filename, "a")
                    handles[filename] = file
            
                # We only convert from the original format to v2.0.0
                if "/metadata/version" in file:
                    warnings.warn("Input file contains /metadata/version, suggesting it is already converted.")
                
                try:
                    if self.contexts:
                        contexts = self.contexts
                    else:
                        contexts = set(file["/"])
                except Exception as e:
                    raise ValueError(
                        f"Failed to read contexts from Amethyst H5 file {filename}"
                    ) from e
                
                for context in contexts:
                    try:
                        if self.require_bc:
                            barcodes = self.require_bc
                        else:
                            barcodes = set(file[context])
                            barcodes.difference_update(self.skip_bc)
                    except KeyError:
                        # If a given context isn't present in the file, skip it
                        continue
                    except Exception as e:
                        raise ValueError(f"Failed to obtain barcodes in {filename}/{context} using {self}") from e
                    
                    for barcode in barcodes:

                        try:
                            data_array = file[context][barcode]
                        except KeyError:
                            # If a given barcode isn't present in the given context, skip it.
                            continue
                        try:
                            data_array = data_array[:]
                            dataset = AmethystH5Dataset(
                                filename,
                                context,
                                barcode,
                                barcode,
                                data_array
                            )
                            yield dataset
                        except Exception as e:
                            raise ValueError(f"Failed to load dataset from {filename}/{context}/{barcode}/{self.observations} using {self}") from e
            except Exception as e:
                raise ValueError(f"Could not open {filename} as H5 file with h5py") from e

AmethystH5Data: TypeAlias = NDArray

def pl_to_amethyst_recarray(df: pl.DataFrame) -> AmethystH5Data:
    recarray = df.to_numpy(structured=True)
    recarray = recarray.astype(amethyst_dtype)
    return recarray

def agg_to_amethyst_h5(dataset: AmethystH5Dataset, windows: fct.UniformWindows | fct.VariableWindows):
    """Use windows object to aggregate dataset and return as recarray to for writing to Amethyst H5 file
    """
    # 25% of time is spent on formatting data
    values = dataset.pl()
    values = values.fill_nan(0)
    values = values.fill_null(0)

    # 55% of time is spent on aggregation
    result = windows.aggregate(values = values)
    
    # 20% of time is spent on converting to recarray
    result = fct.pl_to_amethyst_recarray(result)

    return result

def write_amethyst_h5_data(data: "fct.AmethystH5Data", dataset: "fct.AmethystH5Dataset", output_filename: str, dataset_name: str, compression: str, compression_opts: Any):
    if output_filename in handles:
        file = handles[output_filename]
    else:
        file = h5py.File(output_filename, "a")
        handles[output_filename] = file
    
    context = (
        f"data\n{data}\n"
        f"dataset\n{dataset}\n"
        f"output filename\n{output_filename}\n"
        f"dataset name\n{dataset_name}\n"
        f"compression\n{compression}\n"
        f"compression_opts\n{compression_opts}"
    )
    try:
        dataset_path = "/" + "/".join([dataset.context, dataset.barcode])
    except Exception as e:
        raise ValueError(f"Failed to create dataset path with {context}") from e

    try:
        if dataset_path not in file:
            file.create_group(dataset_path)
    except Exception as e:
        raise ValueError(f"Failed to create group {dataset_path} with {context}") from e

    try:
        if dataset_name in file[dataset_path]:
            del file[dataset_path][dataset_name]
    except Exception as e:
        raise ValueError(f"Failed to delete pre-existing dataset {dataset_name} with {context}") from e
    
    try:
        file[dataset_path].create_dataset(dataset_name, data = data, compression=compression, compression_opts=compression_opts)
    except Exception as e:
        raise ValueError(f"Failed to create dataset {dataset_name} with {context}") from e