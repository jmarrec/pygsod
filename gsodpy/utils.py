import sys
import struct
import warnings
import datetime
from enum import Enum

"""
* types:
    * text_type: unicode in Python 2, str in Python 3
    * binary_type: str in Python 2, bytes in Python 3
    * string_types: basestring in Python 2, str in Python 3
"""
PY3 = (sys.version_info[0] >= 3)

if PY3:
    string_types = str,
    integer_types = int,
    class_types = type,
    text_type = str
    binary_type = bytes
else:
    string_types = basestring,
    integer_types = (int, long)
    class_types = (type, types.ClassType)
    text_type = unicode
    binary_type = str

string_and_binary_types = string_types + (binary_type,)


def is_platform_windows():
    return sys.platform == 'win32' or sys.platform == 'cygwin'


def is_platform_linux():
    return sys.platform == 'linux2'


def is_platform_mac():
    return sys.platform == 'darwin'


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

    return (hasattr(obj, '__iter__') and
            not isinstance(obj, string_and_binary_types))


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


def sanitize_usaf_wban(usaf_wban):

    # Format USAF and WBAN as fixed-length numbers (strings)
    usaf, wban = usaf_wban.split('-')
    if (len(usaf) > 6) | (len(wban) > 5):
        raise ValueError("USAF must be len 6 and WBAN len 5, "
                         "you provided {}".format(usaf_wban))
    if len(usaf) < 6:
        msg = ("USAF must be len 6, "
               "adding leading zeros to '{}'".format(usaf))
        warnings.warn(msg, SyntaxWarning)
        usaf = usaf.zfill(6)

    if len(wban) < 5:
        msg = ("WBAN must be len 5, "
               "adding leading zeros to '{}'".format(wban))
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
        if sys.version_info[0] >= 3:
            year = input(prompt)
        else:
            year = raw_input(prompt)

        # If nothing is provided, get current year
        if year == '':
            year = datetime.date.today().year
        try:
            year = int(year)
        except ValueError:
            print("Please enter an integer between 2000 and 2020")
            continue

        if year < 2000 or year > 2020:
            print("year needs to be between 2000 and 2020!")
            continue
        else:
            break
    return year
