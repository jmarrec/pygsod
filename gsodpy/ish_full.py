
'''

Author: Julien Marrec
Date: 2020-06-18

Download weather data for a year (user-specified), from NOAA
Extracts the gzip files and cleans up afterwards
Weather stations are stored into the file `weatherfiles.txt`
with the syntax "USAF-WBAN"

'''
import os
import datetime
import pandas as pd
import numpy as np

from gsodpy.noaadata import NOAAData
from gsodpy.constants import WEATHER_DIR
from gsodpy.utils import (DataType, is_list_like, get_valid_year,
                          sanitize_usaf_wban)


def parse_ish_file(op_path):
    """
    Parses the Weather File downloaded from NOAA's Integrated Surface Data 
    (ISD, formerly Integrated Surface Hourly (ISH))
    This file with no extension is a fixed-width file, which format 
    is specified in '/pub/data/noaa/ish-format-document.pdf'
    Will also convert the IP units to SI units used by E+
    Args:
    ------
        ish_path (str, or list_like): Path to the weather file, 
        or a list of path
        If a list, will parse all the files and concat the result in a single
        DataFrame
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
    # df.loc[~df['IP_Units'].isnull(), 'Name'] = df.loc[~df['IP_Units'].isnull(), 'FIELD'] + "_" + df.loc[~df['IP_Units'].isnull(), 'IP_Units']
    #
    # df['SI_Name'] = df.FIELD
    # df.loc[~df['SI_Units'].isnull(), 'SI_Name'] = df.loc[~df['SI_Units'].isnull(), 'FIELD'] + "_" + df.loc[~df['SI_Units'].isnull(), 'SI_Units']
    #
    # names = df.Name.tolist()
    # Define the [start,end[ for the fixed-width format

    colspecs = [(4, 10),
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
                (104, 105),
                (57, 63),
                (64, 66),
                (68, 73),
                (74, 76),
                (78, 83),
                (84, 86),
                (88, 93),
                (95, 100),
                (102, 108),
                (108, 109),
                (110, 116),
                (116, 117),
                (118, 123),
                (123, 124),
                (125, 130),
                (132, 133),
                (133, 134),
                (134, 135),
                (135, 136),
                (136, 137),
                (137, 138),
                (65, 69),
                (60,63)]
    # Define column names
    names = ['USAF',
             'WBAN',
             'YEAR',
             'MONTH',
             'DAY',
             'TIME',
             'TEMP_C',
             'TEMP_Count',
             'DEWP_C',
             'DEWP_Count',
             'SLP_hPa',
             'SLP_Count',
             'STP_mbar',
             'STP_Count',
             'VISIB_mi',
             'VISIB_Count',
             'WDSP_kn',
             'WDSP_Count',
             'MXSPD_kn',
             'GUST_kn',
             'MAX_F',
             'MAX_Flag',
             'MIN_F',
             'MIN_Flag',
             'PRCP_in',
             'PRCP_Flag',
             'SNDP_in',
             'FRSHTT_Fog',
             'FRSHTT_Rain_or_Drizzle',
             'FRSHTT_Snow_or_Ice_Pellets',
             'FRSHTT_Hail',
             'FRSHTT_Thunder',
             'FRSHTT_Tornado_or_Funnel_Cloud',
             'WIND_SPEED',
             'WIND_DIRECTION']
    # Force dtypes
    dtypes = {'DAY': np.int32,
              'TIME': np.int32,
              'DEWP_C': np.float64,
              'DEWP_Count': np.int32,
              'FRSHTT_Fog': bool,
              'FRSHTT_Hail': bool,
              'FRSHTT_Rain_or_Drizzle': bool,
              'FRSHTT_Snow_or_Ice_Pellets': bool,
              'FRSHTT_Thunder': bool,
              'FRSHTT_Tornado_or_Funnel_Cloud': bool,
              'GUST': np.float64,
              'MAX': np.float64,
              'MAX_F': np.float64,
              'MAX_Flag': str,
              'MIN': np.float64,
              'MIN_Flag': str,
              'MONTH': np.int32,
              'MXSPD': np.float64,
              'PRCP': np.float64,
              'PRCP_Flag': str,
              'SLP': np.float64,
              'SLP_Count': np.int32,
              'SNDP': np.float64,
              'STP': np.float64,
              'STP_Count': np.int32,
              'TEMP': np.float64,
              'TEMP_Count': np.int32,
              'USAF': np.int32,
              'VISIB': np.float64,
              'VISIB_Count': np.int32,
              'WBAN': np.int32,
              'WDSP': np.float64,
              'WDSP_Count': np.int32,
              'YEAR': np.int32,
              'WIND_SPEED': np.float64,
              'WIND_DIRECTION': np.int32}
    # Define NA values per column, based on gsod format description
    na_values = {'TEMP_C': 9999,
                 'DEWP_C': 9999,
                 'SLP_hPa': 99999,
                 'STP_mbar': 9999.9,
                 'VISIB_mi': 999.9,
                 'WDSP_kn': 999.9,
                 'MXSPD_kn': 999.9,
                 'GUST_kn': 999.9,
                 'MAX_F': 9999.9,
                 'MIN_F': 9999.9,
                 'PRCP_in': 99.9,
                 'SNDP_in': 999.9,
                 'WIND_SPEED': 9999,
                 'WIND_DIRECTION': 999}
    # If a single path, put it in a list of one-element
    if not is_list_like(op_path):
        op_path = [op_path]
    all_ops = []
    for p in op_path:
        i_op = pd.read_fwf(p, index_col='Date',
                           parse_dates={
                               'Date': ['YEAR', 'MONTH', 'DAY', 'TIME']},
                           colspecs=colspecs, header=None, names=names,
                           skiprows=1,
                           na_values=na_values, dtypes=dtypes)

        i_op['TEMP_C'] = i_op['TEMP_C'] / 10 # scaling factor: 10
        i_op['DEWP_C'] = i_op['DEWP_C'] / 10 # scaling factor: 10
        i_op['SLP_hPa'] = i_op['SLP_hPa'] / 10 # scaling factor: 10
        i_op['WIND_SPEED'] = i_op['WIND_SPEED'] / 10 # scaling factor: 10

        i_op['SLP_Pa'] = i_op['SLP_hPa'] * 100

        fname = os.path.join(isd_full.weather_dir, p + '.xlsx')
        i_op.to_excel(fname)
        all_ops.append(i_op)
    op = pd.concat(all_ops)
    # Format USAF and WBAN as fixed-length numbers (strings)
    op.USAF = op.USAF.map(str).str.zfill(6)
    op.WBAN = op.WBAN.map(str).str.zfill(5)
    op['StationID'] = op.USAF + "-" + op.WBAN
    # Change these to bool, easier if you want to
    # filter by these columns directly
    op[['FRSHTT_Fog', 'FRSHTT_Rain_or_Drizzle',
        'FRSHTT_Snow_or_Ice_Pellets', 'FRSHTT_Hail', 'FRSHTT_Thunder',
        'FRSHTT_Tornado_or_Funnel_Cloud'
        ]] = op[['FRSHTT_Fog', 'FRSHTT_Rain_or_Drizzle',
                 'FRSHTT_Snow_or_Ice_Pellets', 'FRSHTT_Hail', 'FRSHTT_Thunder',
                 'FRSHTT_Tornado_or_Funnel_Cloud']].applymap(bool)
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

    col_order = ['StationID', 'USAF', 'WBAN',
                 'TEMP_C', 'TEMP_Count',
                 'DEWP_C', 'DEWP_Count',
                 'SLP_mbar', 'SLP_Pa', 'SLP_Count',
                 'STP_mbar', 'STP_Pa', 'STP_Count',
                 'VISIB_mi', 'VISIB_km', 'VISIB_Count',
                 'WDSP_kn', 'WDSP_m/s', 'WDSP_Count',
                 'MXSPD_kn', 'MXSPD_m/s',
                 'GUST_kn', 'GUST_m/s',
                 'MAX_F', 'MAX_C', 'MAX_Flag',
                 'MIN_F', 'MIN_C', 'MIN_Flag',
                 'PRCP_in', 'PRCP_mm', 'PRCP_Flag',
                 'SNDP_in', 'SNDP_cm',
                 'FRSHTT_Fog', 'FRSHTT_Rain_or_Drizzle',
                 'FRSHTT_Snow_or_Ice_Pellets', 'FRSHTT_Hail',
                 'FRSHTT_Thunder', 'FRSHTT_Tornado_or_Funnel_Cloud']
    # op = op[col_order]
    return op

if __name__ == '__main__':

    isd_full = NOAAData(data_type=DataType.isd_full)

    # This is what's run
    start_year = get_valid_year("Enter start year in YYYY format."
                                "Leave blank for current year "
                                "({}):\n".format(datetime.date.today().year))

    end_year = get_valid_year("Enter end year in YYYY format."
                              "Leave blank for current year "
                              "({}):\n".format(datetime.date.today().year))
    # Download the data
    isd_full.set_years_range(start_year=start_year, end_year=end_year)

    # cleanup empty files, extract the gzip files, and delete them afterwards
    isd_full.get_stations_from_file(
        weather_stations_file=os.path.join(WEATHER_DIR,
                                           'weather_stations.txt'))

    print("Starting retrieving!")
    isd_full.get_all_data()
    df = parse_ish_file(isd_full.ops_files)
    # fname = os.path.join(isd_full.weather_dir, 'df_isd_full.xlsx')
    # df.to_excel(fname)
    print(df)
