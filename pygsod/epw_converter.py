"""Convert to an EPW weather file."""

import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from pyepw.epw import EPW

from pygsod.constants import RESULT_DIR, SUPPORT_DIR, WEATHER_DIR
from pygsod.utils import as_path


def clean_df(df, file):
    """Clean raw data into hourly.

    Interpolates for missing data.
    """
    # print("years downloaded:", set(df.index.year))
    # year = int(input("enter the year you want to convert:"))
    # df = df[df.index.year == year]
    print("start parsing", file)
    print("length of original dataset:", len(df))
    df.index = pd.to_datetime(df.index)
    df = df.groupby(pd.Grouper(freq="1H")).mean(numeric_only=True)
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


def epw_convert(df, op_file_name):
    """Convert ish_full into EPW file."""
    epw = EPW()
    epw_file = SUPPORT_DIR / "EPW-template-file.epw"
    epw.read(epw_file)

    current_year = datetime.datetime.now().year

    if df.index[0].year == current_year:
        length = len(df.index)
    else:
        length = len(epw.weatherdata)

    for i, wd in enumerate(epw.weatherdata):
        if i < length:
            wd.year = df.index[i].year

            #    Temperature
            # ----------------
            value_temp = df["TEMP_C"][i]

            if value_temp >= 70:
                # condition of EPW package, value need to be smaller 70.0
                # for field dry_bulb_temperature
                value_temp = 69

            elif value_temp <= -70:
                value_temp = -69

            wd.dry_bulb_temperature = value_temp

            #        DEWP
            # ----------------

            value_dewp = df["DEWP_C"][i]

            if value_dewp >= 70:
                # condition of EPW package, value need to be smaller 70.0
                # for field dew_point_temperature
                value_dewp = 69

            elif value_dewp <= -70:
                value_dewp = -69

            wd.dew_point_temperature = value_dewp

            #      Pressure
            # ----------------

            value_pressure = df["SLP_Pa"][i]
            if value_pressure >= 120000:
                # condition of EPW package, value need to be smaller 120000 for
                # field atmosphere pressure
                value_pressure = 119999
            elif value_pressure <= 31000:
                value_pressure = 31001

            wd.atmospheric_station_pressure = value_pressure

            #      Wind Speed
            # ----------------

            value_windspeed = df["WIND_SPEED"][i]
            if value_windspeed >= 40:
                value_windspeed = 39.9  # value need to be smaller 40.0
            wd.wind_speed = value_windspeed

            #      Wind Direction
            # ----------------
            value_winddirection = df["WIND_DIRECTION"][i]
            wd.wind_direction = value_winddirection

            #      Relative Humidity Percentage
            # ----------------
            value_rh = df["RELATIVE_HUMIDITY_PERCENTAGE"][i]
            if np.isnan(value_rh):
                value_rh = 0
            wd.relative_humidity = value_rh

            #      Total Sky Cover
            # ----------------
            value_total_sky_cover = df["TOTAL_SKY_COVER"][i] / 2
            # divided by 2 because NOAA uses 20t0 scaling while EPW uses tenth
            # scaling
            wd.total_sky_cover = value_total_sky_cover

            #      Opaque Sky Cover
            # ----------------
            value_opaque_sky_cover = df["OPAQUE_SKY_COVER"][i] / 2
            # divided by 2 because NOAA uses 20t0 scaling while EPW uses tenth
            # scaling
            wd.opaque_sky_cover = value_opaque_sky_cover

    epw_file_new = RESULT_DIR / (op_file_name + ".epw")
    epw.save(epw_file_new)

    return epw_file_new

def convert_all_isd_full_files(directory: Optional[Path] = None):
    """Runs epw_convert for all the files in the isd_full folder.

    Arg :
        - directory (Path): should be the same type of folder than
        isd_full, e.g one folder for each year and in these folders
        you have weather file in .xlsx format
    """
    if directory is None:
        directory = WEATHER_DIR / "isd_full"
    else:
        directory = as_path(directory)

    for dirs in directory.iterdir():
        print(dirs)
        for file_path in dirs.iterdir():
            file_name = file_path.name
            if file_name.endswith("xlsx"):
                df = pd.read_excel(str(file_path), index_col=0)
                df = clean_df(df, file_name)
                epw_convert(df, file_name)
