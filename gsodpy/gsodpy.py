'''

Author: Julien Marrec
Date: 2014-12-12
Updated: 2016-05-25
Updated: 2017-06-08

Download weather data for a year (user-specified), from NOAA
Extracts the gzip files and cleans up afterwards
Weather stations are stored into the file `weatherfiles.txt`
with the syntax "USAF-WBAN"

'''

# Make it backwards compatible with python 2
from __future__ import print_function, division
import sys

import os
from ftplib import FTP
import gzip
import pandas as pd
import numpy as np

import time
import datetime
import re

# For the Haversine Formula
from math import cos, asin, sqrt

from gsodpy.utils import is_list_like

# Change here the main folder where this py file is located.
gsod_dir = os.path.dirname(os.path.realpath(__file__))

# Create support and weather_files directories if don't exist
support_dir = os.path.join(gsod_dir, '../support/')
if not os.path.exists(support_dir):
    os.makedirs(support_dir)

weather_dir = os.path.join(gsod_dir, '../weather_files/')
if not os.path.exists(weather_dir):
    os.makedirs(weather_dir)


def distance(lat1, lon1, lat2, lon2):
    """
    Computes the Haversine distance between two positions

    """
    p = 0.017453292519943295
    a = 0.5 - cos((lat2-lat1)*p)/2 + \
        cos(lat1*p)*cos(lat2*p) * (1-cos((lon2-lon1)*p)) / 2
    return 12742 * asin(sqrt(a))


def closest_weather_station(isd, lat, lon):
    """
    Returns the USAF-WBAN of the closest weather station to the point specified
    by lat and lon as arguments

    """
    isd['distance'] = isd.apply(lambda x: distance(x['LAT'], x['LON'],
                                                   lat, lon), axis=1)
    print(isd.loc[isd['distance'].argmin()])
    return isd['distance'].argmin()


def update_isd_history():
    """
    Will download the `isd-history.csv` file if one of two conditions are true:
        * The `isd-history.csv` does not exist in the /support/ folder
        * the `isd-history.csv` is older than 1 month

    `isd-history.csv` is the list of weather stations, it includes start
        and end dates

    Args:
    ------
        None

    Returns:
    --------
        None, but will download the file

    Needs:
    -------------------------------
        import os
        from ftplib import FTP
        import time

    """
    isd = 'isd-history.csv'
    isd_local = os.path.join(support_dir, isd)

    update_needed = False

    if os.path.isfile(isd):
        print("isd-history.csv was last modified on: %s" %
              time.ctime(os.path.getmtime(isd)))
        # Check if the isd-history.csv is older than 1 month
        if time.time() - os.path.getmtime(isd) > (1 * 30 * 24 * 60 * 60):
            update_needed = True
    else:
        print("isd-history.csv not found, will download it")
        update_needed = True

    if update_needed:

        ftp = FTP('ftp.ncdc.noaa.gov')
        ftp.login()

        # Change current working directory on FTP
        # isd-history is now stored there
        ftp.cwd('/pub/data/noaa/')

        # Try to retrieve it
        try:
            ftp.retrbinary('RETR ' + isd, open(isd_local, 'wb').write)
        except:
            print(isd + " failed to download")

        print("Success: isd-history.csv loaded")
        ftp.quit()

    else:
        print("No updates necessary: isd-history.csv is not"
              " older than one month")


def parse_isd():
    """
    Loads the isd-history.csv into a pandas dataframe. Will serve to check
    if the station has data up to the year we want data from and get it's full
    name for reporting

    Args:
    ------
        None, will load the `isd-history.csv` that's found in the same folder

    Returns:
    --------
        df_isd (pd.DataFrame): the isd-history loaded into a dataframe,
            indexed by "USAF-WBAN"

    Needs:
    -------------------------------
        import os
        import pandas as pd

    """

    isd = os.path.join(support_dir, 'isd-history.csv')
    df_isd = pd.read_csv(isd, sep=",", parse_dates=[9, 10])

    # Need to format the USAF with leading zeros as needed
    # should always be len of 6, WBAN len 5

    df_isd['StationID'] = df_isd.USAF.map("{:06d}".format) + '-' + \
        df_isd.WBAN.map("{:05d}".format)

    df_isd = df_isd.set_index('StationID')

    return df_isd


def download_GSOD_multiple_years(start_year=2003):
    """
    Downloads data for multiple years,
    from start_year (included) to current year

    Args:
    ------
        start_year (int): Optional, defaults to 2003

    Returns:
    --------
        True, will just download everything

    """
    end_year = datetime.date.today().year + 1

    for year in range(start_year, end_year):

        # Download the data
        download_GSOD_year(year)

        # cleanup empty files, extract the gzip files,
        # and delete them afterwards
        cleanup_extract(year)

    print("\n\nDownloaded data for {} years".format(end_year - start_year))
    return True


def download_GSOD_year(year):
    """
    Downloads data from GSOD for a single year

    Will check all weather stations entered in weatherfiles.txt (same folder)
    Loads the isd-history.csv in a pandas dataframe to check the station name
    and make sure there is actually data for the year we want (otherwise would
    try to download a file that doesn't exist)

    Args:
    ------
        year (int): Year to download data for (format YYYY)

        Check weatherfiles.txt as well, where you enter USAF-WBAN
            for all the stations you want

    Returns:
    --------
        None, will just download everything

    Needs:
    -------------------------------
        from ftplib import FTP
        import os
        import pandas as pd

    """

    # Log to NOAA ftp
    ftp = FTP('ftp.ncdc.noaa.gov')
    ftp.login()

    # Weather stations are stored into the following file
    # with the syntax "USAF-WBAN"
    weatherfiles = os.path.join(weather_dir, 'weatherfiles.txt')

    if not os.path.isfile(weatherfiles):
        print("You need to have a file in the current folder that's called "
              "'weatherfiles.txt' and in which you should put the USAF-WBAN "
              "of stations you want")

    # Load weather stations into lists. Stations[i] returns the stations
    # Can get it's length by calling 'len(stations)'
    # Will ignore everything that's after the pound `#` sign
    stations = [line.split('#')[0].strip() for line in open(weatherfiles)
                if not line.startswith('#')]

    # Load dataframe of isd-history
    df = parse_isd()

    # Open an error log file.
    ferror = open(os.path.join(support_dir, 'errors.txt'), 'w')

    # c = done; r = doesn't exist; o = outdated, stopped before
    c = 0
    r = 0
    o = 0

    weatherfolder = os.path.join(weather_dir, str(year))

    # Test if folder doesn't exist
    if os.path.isdir(weatherfolder) is False:
        # If not, create folder
        os.makedirs(weatherfolder)

    # Change current working directory (CWD)
    os.chdir(weatherfolder)

    # Change current working directory on FTP
    # GSOD data is stored in there, organized per year
    ftp.cwd('/pub/data/gsod/' + str(year))

    # Get each station file
    for station in stations:

        end_year = df.ix[station, 'END'].year

        if year <= end_year:

            # Construct file name
            fgsod = str(station) + "-" + str(year) + ".op.gz"

            # Retrieve file: open(fgsod, 'wb') opens a local file to receive
            # the distant blocks of binary data, in binary write mode
            # retrbinary(command, callback): command is a 'RETR filename',
            # and callback function is called for each block of data received:
            # here we write it to the local file

            # Try to retrieve it
            try:
                ftp.retrbinary('RETR ' + fgsod, open(fgsod, 'wb').write)
                # Print current progress
                c += 1
                d = 100 * (r+c+o) / (len(stations))
                print("Station downloaded:" + df['STATION NAME'][station])
                print("progress: {:.0f}%".format(d))
            except:
                r += 1
                ferror.write(fgsod + " doesn't exist\r\n")

        else:
            o += 1
            print(df['STATION NAME'][station] + " doesn't have data up to this"
                  "year. It stopped on: " + str(df['END'][station].date()))

    print("Success: " + str(c) + " files have been stored. ")
    print(str(r) + " station IDs didn't exist. ")
    print(str(o) + " stations stopped recording data before "
          "the year " + str(year))
    ftp.quit()


###############################################################################
def cleanup_extract(year):
    """
    Extracts the GSOD *.op.gz files to *.op and deletes the original gzip file

    Args:
    ------
        year (int): Year to download data for

    Returns:
    --------
        None

    Needs:
    -------------------------------
        import gzip
        import os
        import re

    """
    # Input: year.
    # Import the os module, for the os.walk function

    # Set the directory you want to start from
    weatherfolder = os.path.join(weather_dir, str(year))

    # dirName: The directory it found.
    # subdirList: A list of sub-directories in the current directory
    # fileList: A list of files in the current directory
    for dirName, subdirList, fileList in os.walk(weatherfolder):
        for fname in fileList:
            # Get full path
            path = os.path.join(dirName, fname)
            path = os.path.normpath(path)

            # path[:-3] removes the ".gz" at the end
            outpath = path[:-3]

            # If the file is empty, we delete it
            if os.path.getsize(path) == 0:
                os.remove(path)
            else:
                # If not, we extract
                # Another way of getting the extension
                if os.path.splitext(fname)[1] == ".gz":
                    print("unzipping ", fname)
                    # Open the gzip file
                    in_file = gzip.open(path, 'rb')
                    # Open a second file to write the uncompressed stream
                    out_file = open(outpath, 'wb')
                    out_file.write(in_file.read())

                    # close both
                    in_file.close()
                    out_file.close()
                    os.remove(path)

                    # Print latest date
                    with open(outpath, 'rb') as out_file:
                        first = next(out_file).decode()
                        out_file.seek(-276, 2)
                        last = out_file.readlines()[-1].decode()

                        date_string = re.split('\s+', last)[2]
                        date = datetime.datetime \
                            .strptime(date_string, '%Y%m%d') \
                            .strftime('%d %b %Y')

                        print("Data up to {}".format(date))


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


def parse_gsod_op_file(op_path):
    """
    Parses the Wheater File downloaded from NOAA's GSOD

    This '*.op' is a fixed-width file, which format is specified in
    '/pub/data/gsod/readme.txt'

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
    # df.loc[~df['IP_Units'].isnull(), 'Name'] = df.loc[~df['IP_Units'].isnull(), 'FIELD'] + "_" + df.loc[~df['IP_Units'].isnull(), 'IP_Units']
    #
    # df['SI_Name'] = df.FIELD
    # df.loc[~df['SI_Units'].isnull(), 'SI_Name'] = df.loc[~df['SI_Units'].isnull(), 'FIELD'] + "_" + df.loc[~df['SI_Units'].isnull(), 'SI_Units']
    #
    # names = df.Name.tolist()

    # Define the [start,end[ for the fixed-width format
    colspecs = [(0, 6),
                (7, 12),
                (14, 18),
                (18, 20),
                (20, 22),
                (24, 30),
                (31, 33),
                (35, 41),
                (42, 44),
                (46, 52),
                (53, 55),
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
                (137, 138)]

    # Define column names
    names = ['USAF',
             'WBAN',
             'YEAR',
             'MONTH',
             'DAY',
             'TEMP_F',
             'TEMP_Count',
             'DEWP_F',
             'DEWP_Count',
             'SLP_mbar',
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
             'FRSHTT_Tornado_or_Funnel_Cloud']

    # Force dtypes
    dtypes = {'DAY': np.int32,
              'DEWP': np.float64,
              'DEWP_Count': np.int32,
              'FRSHTT_Fog': bool,
              'FRSHTT_Hail': bool,
              'FRSHTT_Rain_or_Drizzle': bool,
              'FRSHTT_Snow_or_Ice_Pellets': bool,
              'FRSHTT_Thunder': bool,
              'FRSHTT_Tornado_or_Funnel_Cloud': bool,
              'GUST': np.float64,
              'MAX': np.float64,
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
              'YEAR': np.int32}

    # Define NA values per column, based on gsod format description
    na_values = {'TEMP_F': 9999.9,
                 'DEWP_F': 9999.9,
                 'SLP_mbar': 9999.9,
                 'STP_mbar': 9999.9,
                 'VISIB_mi': 999.9,
                 'WDSP_kn': 999.9,
                 'MXSPD_kn': 999.9,
                 'GUST_kn': 999.9,
                 'MAX_F': 9999.9,
                 'MIN_F': 9999.9,
                 'PRCP_in': 99.9,
                 'SNDP_in': 999.9}

    # If a single path, put it in a list of one-element
    if not is_list_like(op_path):
        op_path = [op_path]

    all_ops = []
    for p in op_path:
        i_op = pd.read_fwf(p, index_col='Date',
                           parse_dates={'Date': ['YEAR', 'MONTH', 'DAY']},
                           colspecs=colspecs, header=None, names=names,
                           skiprows=1,
                           na_values=na_values, dtypes=dtypes)
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
    op['TEMP_C'] = (op['TEMP_F'] - 32) * 5 / 9.0
    op['DEWP_C'] = (op['DEWP_F'] - 32) * 5 / 9.0
    op['MAX_C'] = (op['MAX_F'] - 32) * 5 / 9.0
    op['MIN_C'] = (op['MIN_F'] - 32) * 5 / 9.0

    # Convert millibars to Pa (1 mbar = 100 Pa)
    op['SLP_Pa'] = op['SLP_mbar'] * 0.01
    op['STP_Pa'] = op['STP_mbar'] * 0.01

    # Convert knots to m/s (1 nautical mile = 1.852 km)
    op['WDSP_m/s'] = op['WDSP_kn'] * 1852 / 3600.0
    op['MXSPD_m/s'] = op['MXSPD_kn'] * 1852 / 3600.0
    op['GUST_m/s'] = op['GUST_kn'] * 1852 / 3600.0

    # Convert inches to meter multiples (1 in = 2.54 cm)
    op['SNDP_cm'] = op['SNDP_in'] * 2.54
    op['PRCP_mm'] = op['PRCP_in'] * 25.4

    # Convert miles to km (1 mile = 1.60934 km)
    op['VISIB_km'] = op['VISIB_mi'] * 1.60934

    col_order = ['StationID', 'USAF', 'WBAN',
                 'TEMP_F', 'TEMP_C', 'TEMP_Count',
                 'DEWP_F', 'DEWP_C', 'DEWP_Count',
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

    op = op[col_order]

    return op


# Gets only run if calling "python gsodpy.py" not if you import it
if __name__ == '__main__':

    # Update isd history (if needed)
    update_isd_history()

    # This is what's run
    year = get_valid_year("Enter a year in YYYY format. Leave blank for "
                          "current year ({}):\n".format(
                              datetime.date.today().year))

    # Download the data
    download_GSOD_year(year)

    # cleanup empty files, extract the gzip files, and delete them afterwards
    cleanup_extract(year)
