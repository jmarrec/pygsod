import datetime
import struct
import sys
import warnings
from enum import Enum
from pathlib import Path
from typing import Union

import numpy as np
import pandas as pd

"""
* types:
    * text_type: unicode in Python 2, str in Python 3
    * binary_type: str in Python 2, bytes in Python 3
    * string_types: basestring in Python 2, str in Python 3
"""

if sys.version_info[0] < 3:
    raise ValueError("Python3 is only supported")


string_types = (str,)
integer_types = (int,)
class_types = (type,)
text_type = str
binary_type = bytes

string_and_binary_types = string_types + (binary_type,)


def is_platform_windows():
    return sys.platform == "win32" or sys.platform == "cygwin"


def is_platform_linux():
    return sys.platform == "linux2"


def is_platform_mac():
    return sys.platform == "darwin"


def is_platform_32bit():
    return struct.calcsize("P") * 8 < 64


def is_list_like(obj):
    """
    Check if the object is list-like.
    Objects that are considered list-like are for example Python
    lists, tuples, sets, NumPy arrays, and Pandas Series.
    Strings and datetime objects, however, are not considered list-like.
    Parameters
    ----------
    obj : The object to check.
    Returns
    -------
    is_list_like : bool
        Whether `obj` has list-like properties.
    Examples
    --------
    >>> is_list_like([1, 2, 3])
    True
    >>> is_list_like({1, 2, 3})
    True
    >>> is_list_like(datetime(2017, 1, 1))
    False
    >>> is_list_like("foo")
    False
    >>> is_list_like(1)
    False
    """

    return hasattr(obj, "__iter__") and not isinstance(obj, string_and_binary_types)


class ReturnCode(Enum):
    """
    A simple Enum class to represent return codes
    """

    success = 0
    missing = 1
    outdated = 2


class DataType(Enum):
    """
    A simple Enum class to represent NOAA data types codes
    """

    gsod = 0
    isd_full = 1
    isd_lite = 2


class FileType(Enum):
    """
    A simple Enum class to represent Output File Type
    """

    Historical = 0
    TMY = 1


class OutputType(Enum):
    """
    A simple Enum class to represent Output File Type
    """

    CSV = 0
    JSON = 1
    XLSX = 2
    EPW = 3


def sanitize_usaf_wban(usaf_wban):
    # Format USAF and WBAN as fixed-length numbers (strings)
    usaf, wban = usaf_wban.split("-")
    if (len(usaf) > 6) | (len(wban) > 5):
        raise ValueError("USAF must be len 6 and WBAN len 5, " "you provided {}".format(usaf_wban))
    if len(usaf) < 6:
        msg = "USAF must be len 6, " "adding leading zeros to '{}'".format(usaf)
        warnings.warn(msg, SyntaxWarning)
        usaf = usaf.zfill(6)

    if len(wban) < 5:
        msg = "WBAN must be len 5, " "adding leading zeros to '{}'".format(wban)
        warnings.warn(msg, SyntaxWarning)
        wban = wban.zfill(5)

    return "{}-{}".format(usaf, wban)


def get_valid_year(prompt):
    """
    Get a year to pull data for. Defaults to current year if user
    doesn't enter anything at the prompt

    Args:
    ------
        prompt (str): The prompt message

    Returns:
    --------
        year (int): the year

    Needs:
    -------------------------------
        import sys # For backward compatibility with Python 2.x
        import datetime

    """
    while True:
        year = input(prompt)

        # If nothing is provided, get current year
        if year == "":
            year = datetime.date.today().year
        try:
            year = int(year)
        except ValueError:
            print("Please enter an integer between 2000 and 2020")
            continue

        if year < 1950 or year > 2020:
            print("year needs to be between 2000 and 2020!")
            continue
        else:
            break
    return year


def clean_df(df):
    """clean raw data into hourly
    interpolate for missing data
    """
    # print("years downloaded:", set(df.index.year))
    # year = int(input("enter the year you want to convert:"))
    # df = df[df.index.year == year]
    print("Start parsing, length of original dataset:", len(df))

    df = df.groupby(pd.Grouper(freq="1H")).mean()
    print("length of data after groupby hour", len(df))

    current_year = datetime.datetime.now().year

    if df.index[0].year == current_year:
        start_date = df.index[0]
        end_date = df.index[-1]
    else:
        # to include 8760 hrs data if the year is not current data
        # otherwise it will missing some hrs because of the raw data
        start_date = "{}-01-01 00:00:00".format(df.index[0].year)
        end_date = "{}-12-31 23:00:00".format(df.index[0].year)

    date_range = pd.date_range(start_date, end_date, freq="1H")

    missing_hours = date_range[~date_range.isin(df.index)]
    for idx in missing_hours:
        df.loc[idx] = np.NaN  # make the missing rows filled with NaN

    print("length of processed dataset:", len(df), "\n")
    # sort to make new rows in place, otherwise the Nan rows are at the end
    df = df.sort_index()
    df = df.interpolate()  # interpolate values

    # fill with rest NaN with value of previous row
    df = df.fillna(method="ffill")
    df = df.fillna(method="backfill")  # fill first row value with second row

    return df


def as_path(path: Union[Path, str]) -> Path:
    """Asserts argument is a Path, or a str (convert to Path), or raises."""

    if not isinstance(path, Path):
        if isinstance(path, str):
            path = Path(path)
        else:
            raise ValueError("You must provide a pathlib.Path object or a string that can convert to one")

    return path
