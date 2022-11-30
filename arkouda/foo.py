from arkouda.pdarrayclass import *
from typing import cast, List, Sequence
import itertools
import numpy as np # type: ignore
import pandas as pd # type: ignore
from typing import cast, Iterable, Optional, Union
from typeguard import typechecked
from arkouda.client import generic_msg
from arkouda.dtypes import NUMBER_FORMAT_STRINGS, float64, int64, \
     DTypes, isSupportedInt, isSupportedNumber, NumericDTypes, SeriesDTypes,\
    int_scalars, numeric_scalars, get_byteorder, get_server_byteorder
from arkouda.dtypes import dtype as akdtype
from arkouda.pdarrayclass import pdarray, create_pdarray
from arkouda.strings import Strings
from arkouda.logger import getArkoudaLogger

from typeguard import typechecked
import json
import numpy as np # type: ignore
from arkouda.client import generic_msg
from arkouda.dtypes import dtype, DTypes, resolve_scalar_dtype, \
     translate_np_dtype, NUMBER_FORMAT_STRINGS, \
     int_scalars, numeric_scalars, numeric_and_bool_scalars, numpy_scalars, get_server_byteorder
from arkouda.dtypes import int64 as akint64
from arkouda.dtypes import str_ as akstr_
from arkouda.dtypes import bool as npbool
from arkouda.dtypes import isSupportedInt
from arkouda.logger import getArkoudaLogger
from arkouda.infoclass import list_registry, information, pretty_print_information

from arkouda.pdarray4dclass import create_pdarray4D

logger = getArkoudaLogger(name='foo')

__all__ = ['foo'] 



def foo(pda, pdb):
    """
    Return the foo() of the array.

    Parameters
    ----------
    pda : pdarray
        The first array to foo
    pdb : pdarray
        The second array to foo

    Returns
    -------
    pdarray
        The foo'd array
    """
    if isinstance(pda, pdarray):

        rep_msg = generic_msg(cmd='foo', args={"aname": pda.name, "bname": pdb.name})
        return create_pdarray4D(rep_msg)

    else:
        raise TypeError("must be pdarray {}".format(pda))



