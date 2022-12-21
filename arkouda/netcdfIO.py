from __future__ import annotations

import glob
import json
import os
import warnings
from typing import Dict, List, Mapping, Optional, Union, cast

import pandas as pd  # type: ignore
from typeguard import typechecked

import arkouda.array_view
from arkouda.categorical import Categorical
from arkouda.client import generic_msg
from arkouda.pdarrayclass import create_pdarray, pdarray
from arkouda.strings import Strings

from arkouda.pdarray2dclass import create_pdarray2D
from arkouda.pdarray3dclass import create_pdarray3D
from arkouda.pdarray4dclass import create_pdarray4D


__all__ = [
    "lsnetcdf",
    "read_NetCDF",
    "get_datasets_NetCDF",
]


@typechecked
def lsnetcdf(filename: str) -> List[str]:
    """
    This function calls the h5ls utility on a HDF5 file visible to the
    arkouda server or calls a function that imitates the result of h5ls
    on a Parquet file.

    Parameters
    ----------
    filename : str
        The name of the file to pass to the server

    Returns
    -------
    str
        The string output of the datasets from the server

    Raises
    ------
    TypeError
        Raised if filename is not a str
    ValueError
        Raised if filename is empty or contains only whitespace
    RuntimeError
        Raised if error occurs in executing ls on an HDF5 file
    """
    if not (filename and filename.strip()):
        raise ValueError("filename cannot be an empty string")

    cmd = "lsany_NetCDF"
    return json.loads(
        cast(
            str,
            generic_msg(
                cmd=cmd,
                args={
                    "filename": filename,
                },
            ),
        )
    )

def read_NetCDF(filename: str, dataset: str, read_shape="normal") -> Union[pdarray, Strings, Mapping[str, Union[pdarray, Strings]]]:
    """
    Read datasets from HDF5 or Parquet files.

    Parameters
    ----------
    filename : The file to be opened
    dataset : The dataset to be opened from the file
    read_shape : Read dataset either as a multi-dimensional array ("normal") or a flattened 1D array ("flat")
 
    Returns
    -------
    For a single dataset returns an Arkouda pdarray or Arkouda Strings object
    and for multiple datasets returns a dictionary of Arkouda pdarrays or
    Arkouda Strings.
        Dictionary of {datasetName: pdarray or String}

    Raises
    ------
    ValueError
        Raised if all datasets are not present in all hdf5/parquet files or if one or
        more of the specified files do not exist
    RuntimeError
        Raised if one or more of the specified files cannot be opened.
        If `allow_errors` is true this may be raised if no values are returned
        from the server.
    TypeError
        Raised if we receive an unknown arkouda_type returned from the server

    See Also
    --------
    read_NetCDF, get_datasets_NetCDF, ls

    Notes
    -----
    If dataset is None, throw an error.

    Examples
    --------
    Read with file Extension
    >>> x = ak.read('path/name_prefix.nc') # load NetCDF 
    """
    if dataset is None:
        raise ValueError("No variable requested.")
    else:  # ensure dataset exists
        #nonexistent = set(dataset) - set(get_datasets_NetCDF(filename))
        exists = dataset in get_datasets_NetCDF(filename)
        #if len(nonexistent) > 0:
        if not exists:
            raise ValueError(f"Variable not found: {dataset}")

    cmd = "readNetCDF"
    rep_msg = generic_msg(
            cmd=cmd,
            args={
                "dset": dataset,
                "filename": filename,
                "read_shape": read_shape,
            },
    )

    rep = json.loads(rep_msg)  # See GenSymIO._buildReadAllHdfMsgJson for json structure
    items = rep["items"] if "items" in rep else []
    file_errors = rep["file_errors"] if "file_errors" in rep else []

    ## Return conditions: either it m
    if len(items) == 1:
        item = items[0]

        if "pdarray" == item["arkouda_type"]:
            return create_pdarray(item["created"])
        if "pdarray2D" == item["arkouda_type"]:
            return create_pdarray2D(item["created"])
        if "pdarray3D" == item["arkouda_type"]:
            return create_pdarray3D(item["created"])
        if "pdarray4D" == item["arkouda_type"]:
            return create_pdarray4D(item["created"])
    else:
        raise RuntimeError("No items were returned")



@typechecked
def get_datasets_NetCDF(filename: str) -> List[str]:
    """
    Get the names of datasets in a NetCDF file.

    Parameters
    ----------
    filename : str
        Name of a NetCDF file visible to the arkouda server

    Returns
    -------
    List[str]
        Names of the datasets in the file

    Raises
    ------
    TypeError
        Raised if filename is not a str
    ValueError
        Raised if filename is empty or contains only whitespace
    RuntimeError
        Raised if error occurs in executing ls on an HDF5 file

    See Also
    --------
    ls
    """
    datasets = lsnetcdf(filename)
    return datasets


@typechecked
def get_datasets_allow_errors(filenames: List[str]) -> List[str]:
    """
    Get the names of datasets in an HDF5 file
    Allow file read errors until success

    Parameters
    ----------
    filenames : List[str]
        A list of HDF5 files visible to the arkouda server

    Returns
    -------
    List[str]
        Names of the datasets in the file

    Raises
    ------
    TypeError
        Raised if filenames is not a List[str]
    FileNotFoundError
        If none of the files could be read successfully

    See Also
    --------
    get_datasets, ls
    """
    datasets = []
    for filename in filenames:
        try:
            datasets = get_datasets(filename)
            break
        except RuntimeError:
            pass
    if not datasets:  # empty
        raise FileNotFoundError("Could not read any of the requested files")
    return datasets



