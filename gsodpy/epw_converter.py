from gsodpy.constants import SUPPORT_DIR, WEATHER_DIR, RESULT_DIR
import os
from pyepw.epw import EPW
from gsodpy.ish_full import parse_ish_file
import pandas as pd
import numpy as np
import datetime

from gsodpy.utils import clean_df


def epw_convert(df, op_file_name):
    """Convert ish_full into EPW file """

    epw = EPW()
    epw_file = os.path.join(
        SUPPORT_DIR, 'EPW-template-file.epw')
    epw.read(epw_file)

    for i, wd in enumerate(epw.weatherdata):

        wd.year = df.index[i].year

        #    Temperature
        # ----------------
        value_temp = df['TEMP_C'][i]

        if value_temp >= 70:
            # condition of EPW package, value need to be smaller 70.0
            # for field dry_bulb_temperature
            value_temp = 69

        elif value_temp <= -70:
            value_temp = -69

        wd.dry_bulb_temperature = value_temp

        #        DEWP
        # ----------------

        value_dewp = df['DEWP_C'][i]

        if value_dewp >= 70:
            # condition of EPW package, value need to be smaller 70.0
            # for field dew_point_temperature
            value_dewp = 69

        elif value_dewp <= -70:
            value_dewp = -69

        wd.dew_point_temperature = value_dewp

        #      Pressure
        # ----------------

        value_pressure = df['SLP_Pa'][i]
        if value_pressure >= 120000:
            # condition of EPW package, value need to be smaller 120000 for
            # field atmosphere pressure
            value_pressure = 119999
        elif value_pressure <= 31000:
            value_pressure = 31001

        wd.atmospheric_station_pressure = value_pressure

        #      Wind Speed
        # ----------------

        value_windspeed = df['WIND_SPEED'][i]
        if value_windspeed >= 40:
            value_windspeed = 39.9  # value need to be smaller 40.0
        wd.wind_speed = value_windspeed

        #      Wind Direction
        # ----------------
        value_winddirection = df['WIND_DIRECTION'][i]
        wd.wind_direction = value_winddirection

        #	   Relative Humidity Percentage
        # ----------------
        value_rh = df['RELATIVE_HUMIDITY_PERCENTAGE'][i]
        wd.relative_humidity = value_rh

        #	   Total Sky Cover
        # ----------------
        value_total_sky_cover = df['TOTAL_SKY_COVER'][i] / 2
        # divided by 2 because NOAA uses 20t0 scaling while EPW uses tenth
        # scaling
        wd.total_sky_cover = value_total_sky_cover

        #	   Opaque Sky Cover
        # ----------------
        value_opaque_sky_cover = df['OPAQUE_SKY_COVER'][i] / 2
        # divided by 2 because NOAA uses 20t0 scaling while EPW uses tenth
        # scaling
        wd.opaque_sky_cover = value_opaque_sky_cover

    epw_file_new = os.path.join(
        RESULT_DIR, op_file_name + '.epw')
    epw.save(epw_file_new)


if __name__ == '__main__':

    for root, dirs, files in os.walk(WEATHER_DIR + '/isd_full'):
        for file in files:
            if file.endswith("xlsx"):
                df_path = os.path.join(root, file)
                df = pd.read_excel(df_path, index_col=0)
                df = clean_df(df)
                epw_convert(df, file)
