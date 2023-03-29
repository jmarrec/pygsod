"""

Author: Julien Marrec
Date: 2014-12-12
Updated: 2016-05-25
Updated: 2017-06-08

Download weather data for a year (user-specified), from NOAA
Extracts the gzip files and cleans up afterwards
Weather stations are stored into the file `weatherfiles.txt`
with the syntax "USAF-WBAN"

"""
import datetime
import os

import pandas as pd

from pygsod.constants import WEATHER_DIR
from pygsod.noaadata import NOAAData
from pygsod.utils import DataType, get_valid_year, is_list_like


def parse_isd_lite_op_file(op_path):
    """
    Parses the Wheater File downloaded from NOAA's ISD-Lite

    This '*.op' is a fixed-width file, which format is specified in
    '/pub/data/noaa/isd-lite/isd-lite-format.txt

    Will also convert the IP units to SI units used by E+

    Args:
    ------
        op_path (str, or list_like): Path to the *.op file, or a list of path

        If a list, will parse all the files and concat the result in a single
        DataFrame

    Returns:
    --------
        op (pd.DataFrame): a DataFrame of the parsed results

    Needs:
    -------------------------------
        import pandas as pd
        import numpy as np

    """

    # How to get it from the CSV - I chose to hardcode stuff to be faster
    # and not depend on a csv file

    # df = pd.read_csv(os.path.join(directory, 'gsod_format.csv'))
    # # Not needed anymore
    # # df.START_POS = df.START_POS.map(int)
    # # df.END_POS = df.END_POS.map(int)
    #
    # colspecs = df.apply(lambda x: (x['START_POS'], x['END_POS']),
    #                                axis=1).tolist()
    #
    #
    # dtypes = df.set_index('FIELD').TYPE.map({'Int.': np.int32,
    #                                          'Real': np.float64,
    #                                          'Char': str,
    #                                          'Bool': np.bool}).to_dict()
    #
    # df['Name'] = df.FIELD
    # df.loc[~df['IP_Units'].isnull(), 'Name'] = (
    #     df.loc[~df['IP_Units'].isnull(), 'FIELD'] + "_" + df.loc[~df['IP_Units'].isnull(), 'IP_Units']
    # )
    #
    # df['SI_Name'] = df.FIELD
    # df.loc[~df['SI_Units'].isnull(), 'SI_Name'] = (
    #     df.loc[~df['SI_Units'].isnull(), 'FIELD'] + "_" + df.loc[~df['SI_Units'].isnull(), 'SI_Units']
    # )
    #
    # names = df.Name.tolist()

    # TODO: not working
    # Define the [start,end[ for the fixed-width format
    # colspecs = [
    #     (0, 4),
    #     (5, 7),
    #     (8, 11),
    #     (11, 13),
    #     (13, 19),
    #     (19, 24),
    #     (25, 31),
    #     (31, 37),
    #     (37, 43),
    #     (43, 49),
    #     (49, 55),
    #     (55, 61),
    # ]

    # Define column names
    names = [
        "YEAR",
        "MONTH",
        "DAY",
        "HOUR",
        "TEMP_C",
        "DEWP_C",
        "SLP_HPa",
        "WIND_dir",
        "WDSP_ms",
        "SkyCond",
        "PRCP_mm_1hr",
        "PRCP_mm_6hr",
    ]

    scale_factors = {
        "YEAR": None,
        "MONTH": None,
        "DAY": None,
        "HOUR": None,
        "TEMP_C": 10.0,
        "DEWP_C": 10.0,
        "SLP_HPa": 10.0,
        "WIND_dir": 1.0,
        "WDSP_ms": 10.0,
        "SkyCond": None,
        "PRCP_mm_1hr": 10.0,
        "PRCP_mm_6hr": 10.0,
    }

    # TODO: use that!
    # Force dtypes
    # dtypes = {
    #     "YEAR": np.int32,
    #     "MONTH": np.int32,
    #     "DAY": np.int32,
    #     "HOUR": np.int32,
    #     "TEMP_C": np.float64,
    #     "DEWP_C": np.float64,
    #     "SLP_HPa": np.float64,
    #     "WIND_dir": np.float64,
    #     "WDSP_ms": np.float64,
    #     # 'SkyCond': np.float64,
    #     "PRCP_mm_1hr": np.float64,
    #     "PRCP_mm_6hr": np.float64,
    # }

    # Define NA values per column, based on gsod format description
    na_values = {
        "TEMP_C": -9999,
        "DEWP_C": -9999,
        "SLP_HPa": -9999,
        "WIND_dir": -9999,
        "WDSP_ms": -9999,
        "SkyCond": -9999,
        "PRCP_mm_1hr": -9999,
        "PRCP_mm_6hr": -9999,
    }

    # If a single path, put it in a list of one-element
    if not is_list_like(op_path):
        op_path = [op_path]

    all_ops = []
    for p in op_path:
        # TODO: NOT WORKING, there's an offset problem with the colspecs
        # I parsed from the isd-lite-format.txt
        # i_op = pd.read_fwf(p, index_col='Date',
        # parse_dates={'Date': ['YEAR', 'MONTH', 'DAY']},
        # colspecs=colspecs, header=None, names=names,
        # skiprows=1,
        # na_values=na_values, dtypes=dtypes)

        i_op = pd.read_csv(
            p,
            sep=r"\s+",
            index_col="Date",
            parse_dates={"Date": ["YEAR", "MONTH", "DAY"]},
            header=None,
            names=names,
            skiprows=1,
            na_values=na_values,
        )

        # Parse USAF-WBAN from the file
        fname = os.path.basename(p)
        usaf, wban, year = fname.split("-")
        i_op["USAF"] = usaf
        i_op["WBAN"] = wban
        i_op["StationID"] = "{}-{}".format(usaf, wban)

        all_ops.append(i_op)
    op = pd.concat(all_ops)

    for k, v in scale_factors.items():
        if v is not None:
            op[k] = op[k] / v

    return op


# Gets only run if calling "python pygsod.py" not if you import it
if __name__ == "__main__":
    isd_lite = NOAAData(data_type=DataType.isd_lite)

    # This is what's run
    start_year = get_valid_year(
        "Enter start year in YYYY format." "Leave blank for current year " "({}):\n".format(datetime.date.today().year)
    )

    end_year = get_valid_year(
        "Enter end year in YYYY format." "Leave blank for current year " "({}):\n".format(datetime.date.today().year)
    )
    # Download the data
    isd_lite.set_years_range(start_year=start_year, end_year=end_year)

    # cleanup empty files, extract the gzip files, and delete them afterwards
    isd_lite.get_stations_from_file(weather_stations_file=os.path.join(WEATHER_DIR, "weather_stations.txt"))

    print("Starting retrieving!")
    isd_lite.get_all_data()
    df = parse_isd_lite_op_file(isd_lite.ops_files)
    fname = os.path.join(isd_lite.weather_dir, "df_isd_lite.xlsx")
    df.to_excel(fname)
    print(df)
