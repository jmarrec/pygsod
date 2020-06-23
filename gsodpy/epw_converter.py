from gsodpy.constants import (EPW_DIR, WEATHER_DIR)
import os
from pyepw.epw import EPW
from gsodpy.ish_full import parse_ish_file
import pandas as pd
import numpy as np


def clean_df(df):

    print("years downloaded:", set(df.index.year))
    year = int(input("enter the year you want to convert:"))
    df = df[df.index.year == year]
    print("length of original dataset:", len(df))

    start_date = '{}-01-01 00:00:00'.format(df.index[0].year)
    end_date = '{}-12-31 23:00:00'.format(df.index[0].year)
    date_range = pd.date_range(start_date, end_date, freq='1H')

    missing_hours = date_range[~date_range.isin(df.index)]
    for idx in missing_hours:
        df.loc[idx] = np.NaN  # make the missing rows filled with NaN

    print("length of processed dataset:", len(df))
    # sort to make new rows in place, otherwise the Nan rows are at the end
    df = df.sort_index()
    df = df.interpolate()  # interpolate values

    # fill with rest NaN with value of previous row
    df = df.fillna(method='ffill')
    df = df.fillna(method='backfill')  # fill first row value with second row

    return df

if __name__ == '__main__':

    df_path = os.path.join(WEATHER_DIR, 'isd_full/df_isd_full.xlsx')
    df = pd.read_excel(df_path, index_col=0)
    df = clean_df(df)

    epw = EPW()
    epw_file = os.path.join(
        EPW_DIR, 'USA_NY_New.York-J.F.Kennedy.Intl.AP.744860_TMY3.epw')
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
            value_dewp = 69  # condition of EPW package, value need to be smaller 70.0 for field dew_point_temperature

        elif value_dewp <= -70:
            value_dewp = -69

        wd.dew_point_temperature = value_dewp

        #      Pressure
        # ----------------

        value_pressure = df['SLP_Pa'][i] * 1000  # convert kPa to Pa
        if value_pressure >= 120000:
            # condition of EPW package, value need to be smaller 120000 for
            # field atmosphere pressure
            value_pressure = 119999
        wd.atmospheric_station_pressure = value_pressure

    epw_file_new = epw_file[:-4] + "_new" + ".epw"
    epw.save(epw_file_new)
