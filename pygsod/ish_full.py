"""

Author: Julien Marrec， Hang Li
Date: 2020-06-18

Download weather data for a year (user-specified), from NOAA
Extracts the gzip files and cleans up afterwards
Weather stations are stored into the file `weatherfiles.txt`
with the syntax "USAF-WBAN"

Parameters can be added but not currently used in EnergyPlus calculations:
1. Ceiling height
2. Visibility

"""
import datetime
import os

import numpy as np
import pandas as pd

from pygsod.constants import WEATHER_DIR
from pygsod.noaadata import NOAAData
from pygsod.utils import DataType, get_valid_year, is_list_like


def parse_rh(data):
    if "RH1" in data:
        loc = data.find("RH3")
        rh = int(data[(loc + 7) : (loc + 10)])
        if rh == 999:
            return np.nan  # if using 999 therew will be issue in interpolating
        else:
            return rh
    else:
        return np.nan


def parse_total_sky_cover(data):
    # TODO: Sky cover is stored as an enum represented by an int
    # eg: (00 = clear, 19=dark overcast, 99 = missing)
    # Perhaps we should store this as an enum?
    if "GF1" in data:
        loc = data.find("GF1")
        total_sky_cover = int(data[(loc + 3) : (loc + 5)])
        if total_sky_cover == 99:
            return np.nan
        else:
            return total_sky_cover
    else:
        return np.nan


def parse_opaque_sky_cover(data):
    # TODO: Sky cover is stored as an enum represented by an int
    # eg: (00 = clear, 19=dark overcast, 99 = missing)
    # Perhaps we should store this as an enum?
    if "GF1" in data:
        loc = data.find("GF1")
        opaque_sky_cover = int(data[(loc + 5) : (loc + 7)])
        if opaque_sky_cover == 99:
            return np.nan
        else:
            return opaque_sky_cover
    else:
        return np.nan


def parse_zenith(data):
    if "GQ1" in data:
        loc = data.find("GQ1")
        zenith_angle = int(data[(loc + 7) : (loc + 11)])
        if zenith_angle == 9999:
            return np.nan
        else:
            return zenith_angle / 10  # scaling factor = 10
    else:
        return np.nan


def parse_azimuth(data):
    if "GQ1" in data:
        loc = data.find("GQ1")
        azimuth_angle = int(data[(loc + 12) : (loc + 16)])
        if azimuth_angle == 9999:
            return np.nan
        else:
            return azimuth_angle / 10  # scaling factor = 10
    else:
        return np.nan


def parse_ish_file(isd_full, create_excel_file=True):
    """
    Parses the Weather File downloaded from NOAA's Integrated Surface Data
    (ISD, formerly Integrated Surface Hourly (ISH))
    This file with no extension is a fixed-width file, which format
    is specified in '/pub/data/noaa/ish-format-document.pdf'
    Will also convert the IP units to SI units used by E+
    Args:
    ------
        isd_full object
        create_excel_file (bool): if True, it creates an excel file per year
    Returns:
    --------
        ish (pd.DataFrame): a DataFrame of the parsed results
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
    # Define the [start,end[ for the fixed-width format

    op_path = isd_full.ops_files
    years = isd_full.years
    # If a single path, put it in a list of one-element
    if not is_list_like(op_path):
        op_path = [op_path]
    all_ops = []

    oppath_year = list(zip(op_path, years))

    colspecs = [
        (4, 10),
        (10, 15),
        (15, 19),
        (19, 21),
        (21, 23),
        (23, 27),
        (87, 92),
        (92, 93),
        (93, 98),
        (98, 99),
        (99, 104),
        (65, 69),
        (60, 63),
        (105, -1),
    ]
    # Define column names
    names = [
        "USAF",
        "WBAN",
        "YEAR",
        "MONTH",
        "DAY",
        "TIME",
        "TEMP_C",
        "TEMP_Count",
        "DEWP_C",
        "DEWP_Count",
        "SLP_hPa",
        "WIND_SPEED",
        "WIND_DIRECTION",
        "ADD_DATA",
    ]
    # Force dtypes
    dtypes = {
        "DAY": np.int32,
        "TIME": np.int32,
        "DEWP_C": np.float64,
        "DEWP_Count": np.int32,
        "MONTH": np.int32,
        "SLP_hPa": np.float64,
        "TEMP_C": np.float64,
        "TEMP_Count": np.int32,
        "USAF": np.int32,
        "WBAN": np.int32,
        "YEAR": np.int32,
        "WIND_SPEED": np.float64,
        "WIND_DIRECTION": np.int32,
    }
    # Define NA values per column, based on gsod format description
    na_values = {
        "TEMP_C": 9999,
        "DEWP_C": 9999,
        "SLP_hPa": 99999,
        "STP_mbar": 9999.9,
        "VISIB_mi": 999.9,
        "WDSP_kn": 999.9,
        "MXSPD_kn": 999.9,
        "GUST_kn": 999.9,
        "MAX_F": 9999.9,
        "MIN_F": 9999.9,
        "PRCP_in": 99.9,
        "SNDP_in": 999.9,
        "WIND_SPEED": 9999,
        "WIND_DIRECTION": 999,
    }

    for p, year in oppath_year:
        i_op = pd.read_fwf(
            p,
            index_col="Date",
            parse_dates={"Date": ["YEAR", "MONTH", "DAY", "TIME"]},
            colspecs=colspecs,
            header=None,
            names=names,
            skiprows=1,
            na_values=na_values,
            dtypes=dtypes,
        )

        i_op["TEMP_C"] = i_op["TEMP_C"] / 10  # scaling factor: 10
        i_op["TEMP_F"] = i_op["TEMP_C"] * 1.8 + 32  # calculate C to F
        i_op["DEWP_C"] = i_op["DEWP_C"] / 10  # scaling factor: 10
        i_op["SLP_hPa"] = i_op["SLP_hPa"] / 10  # scaling factor: 10
        i_op["WIND_SPEED"] = i_op["WIND_SPEED"] / 10  # scaling factor: 10

        i_op["SLP_Pa"] = i_op["SLP_hPa"] * 100

        # ADDITIONAL DATA SECTION
        i_op["ADD_DATA"] = i_op["ADD_DATA"].fillna("")
        i_op["RELATIVE_HUMIDITY_PERCENTAGE"] = i_op["ADD_DATA"].apply(parse_rh)
        i_op["TOTAL_SKY_COVER"] = i_op["ADD_DATA"].apply(parse_total_sky_cover)
        i_op["OPAQUE_SKY_COVER"] = i_op["ADD_DATA"].apply(parse_opaque_sky_cover)
        i_op["AZIMUTH_ANGLE"] = i_op["ADD_DATA"].apply(parse_azimuth)
        i_op["ZENITH_ANGLE"] = i_op["ADD_DATA"].apply(parse_zenith)

        # filter the only data for the year we need
        i_op = i_op[i_op.index.year == year]

        if create_excel_file:
            fname = p.with_suffix(".xlsx")
            i_op.to_excel(fname)
        all_ops.append(i_op)

    if len(all_ops) > 0:
        op = pd.concat(all_ops)
        # Format USAF and WBAN as fixed-length numbers (strings)
        op.USAF = op.USAF.map(str).str.zfill(6)
        op.WBAN = op.WBAN.map(str).str.zfill(5)
        op["StationID"] = op.USAF + "-" + op.WBAN
    else:
        op = pd.DataFrame()

    # Convert from IP units to SI (used by E+)

    # Convert temperatures

    # op['TEMP_C'] = (op['TEMP_F'] - 32) * 5 / 9.0
    # op['DEWP_C'] = (op['DEWP_F'] - 32) * 5 / 9.0
    # op['MAX_C'] = (op['MAX_F'] - 32) * 5 / 9.0
    # op['MIN_C'] = (op['MIN_F'] - 32) * 5 / 9.0
    # Convert millibars to Pa (1 mbar = 100 Pa)
    # op['SLP_Pa'] = op['SLP_mbar'] * 0.01
    # op['STP_Pa'] = op['STP_mbar'] * 0.01
    # Convert knots to m/s (1 nautical mile = 1.852 km)
    # op['WDSP_m/s'] = op['WDSP_kn'] * 1852 / 3600.0
    # op['MXSPD_m/s'] = op['MXSPD_kn'] * 1852 / 3600.0
    # op['GUST_m/s'] = op['GUST_kn'] * 1852 / 3600.0
    # Convert inches to meter multiples (1 in = 2.54 cm)
    # op['SNDP_cm'] = op['SNDP_in'] * 2.54
    # op['PRCP_mm'] = op['PRCP_in'] * 25.4
    # Convert miles to km (1 mile = 1.60934 km)
    # op['VISIB_km'] = op['VISIB_mi'] * 1.60934

    # col_order = ['StationID', 'USAF', 'WBAN',
    #              'TEMP_C', 'TEMP_Count',
    #              'DEWP_C', 'DEWP_Count',
    #              'SLP_mbar', 'SLP_Pa', 'SLP_Count',
    #              'STP_mbar', 'STP_Pa', 'STP_Count',
    #              'VISIB_mi', 'VISIB_km', 'VISIB_Count',
    #              'WDSP_kn', 'WDSP_m/s', 'WDSP_Count',
    #              'MXSPD_kn', 'MXSPD_m/s',
    #              'GUST_kn', 'GUST_m/s',
    #              'MAX_F', 'MAX_C', 'MAX_Flag',
    #              'MIN_F', 'MIN_C', 'MIN_Flag',
    #              'PRCP_in', 'PRCP_mm', 'PRCP_Flag',
    #              'SNDP_in', 'SNDP_cm',
    #              'FRSHTT_Fog', 'FRSHTT_Rain_or_Drizzle',
    #              'FRSHTT_Snow_or_Ice_Pellets', 'FRSHTT_Hail',
    #              'FRSHTT_Thunder', 'FRSHTT_Tornado_or_Funnel_Cloud']
    # op = op[col_order]
    return op


if __name__ == "__main__":
    isd_full = NOAAData(data_type=DataType.isd_full)

    # This is what's run
    start_year = get_valid_year(
        "Enter start year in YYYY format." "Leave blank for current year " "({}):\n".format(datetime.date.today().year)
    )

    end_year = get_valid_year(
        "Enter end year in YYYY format." "Leave blank for current year " "({}):\n".format(datetime.date.today().year)
    )
    # Download the data
    isd_full.set_years_range(start_year=start_year, end_year=end_year)

    # cleanup empty files, extract the gzip files, and delete them afterwards
    isd_full.get_stations_from_file(weather_stations_file=os.path.join(WEATHER_DIR, "weather_stations.txt"))

    print("Starting retrieving!")
    isd_full.get_all_data()
    df = parse_ish_file(isd_full.ops_files)
    # fname = os.path.join(isd_full.weather_dir, 'df_isd_full.xlsx')
    # df.to_excel(fname)
    print(df)
