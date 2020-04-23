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

import progressbar

# For the Haversine Formula
from math import cos, asin, sqrt

from gsodpy.utils import (is_list_like, ReturnCode)

import warnings


# Global constants

# Where this py file is located.
GSOD_DIR = os.path.dirname(os.path.realpath(__file__))

# Create support and weather_files directories if don't exist
SUPPORT_DIR = os.path.realpath(os.path.join(GSOD_DIR, '../support/'))
if not os.path.exists(SUPPORT_DIR):
    os.makedirs(SUPPORT_DIR)

WEATHER_DIR = os.path.realpath(os.path.join(GSOD_DIR, '../weather_files/'))
if not os.path.exists(WEATHER_DIR):
    os.makedirs(WEATHER_DIR)


ISD_PATH = os.path.join(SUPPORT_DIR, 'isd-history.csv')


class ISD():
    """
    Class for the ISD file and methods
    """

    def __init__(self, isd_path=None):
        """
        Init the ISD. Checks if exists, if not downloads it, store the outpath

        Args:
        ------
            isd_path (str): path to `isd-history.csv`, optional, will default
            to ../support/isd-history.csv

        """
        # If isd_path isn't supplied, set it to the default (class) path
        if isd_path is None:
            self.isd_path = ISD_PATH
        else:
            self.isd_path = isd_path

        # See if the isd_history needs updating, otherwise just link it
        self.update_isd_history(isd_path=self.isd_path)
        self.df = self.parse_isd(isd_path=self.isd_path)

    def update_isd_history(self, isd_path=None, force=False, dry_run=False):
        """
        Will download the `isd-history.csv` file
        if one of two conditions are true:
            * The `isd-history.csv` does not exist in the ../support/ folder
            * the `isd-history.csv` is older than 1 month

        `isd-history.csv` is the list of weather stations, it includes start
            and end dates

        Args:
        ------
            isd_path (str, Optional): path to `isd-history.csv`.
                If None, will get the ISD instance's `isd_pat` that was
                set during initialization

            force (bool, optional): whether to force an update or not

        Returns:
        --------
            update_needed (bool): True if updated, False otherwise

        Needs:
        -------------------------------
            import os
            from ftplib import FTP
            import time

        """

        # Defaults the isd_path
        if isd_path is None:
            isd_path = self.isd_path
            print("Using ISD path: {}".format(isd_path))

        update_needed = False

        if os.path.isfile(isd_path):
            print("isd-history.csv was last modified on: %s" %
                  time.ctime(os.path.getmtime(self.isd_path)))
            # Check if the isd-history.csv is older than 1 month
            if time.time() - os.path.getmtime(self.isd_path) > (1 * 30 * 24
                                                                * 60 * 60):
                update_needed = True
            elif force:
                print("Forcing update anyways")
                update_needed = True
        else:
            print("isd-history.csv not found, will download it")
            update_needed = True

        if update_needed:
            if not dry_run:
                self.download_isd(isd_path)

        else:
            print("No updates necessary: isd-history.csv is not"
                  " older than one month")

        return update_needed

    def download_isd(self, isd_path):
        """
        Downloads the isd-history.csv from NOAA

        Args:
        -----
            isd_path (str): path on local disk to save the file

        Returns:
        --------
            success (bool): whether it worked or not

        Needs:
        -------------------------------
            from ftplib import FTP

        """

        ftp = FTP('ftp.ncdc.noaa.gov')
        ftp.login()

        # Change current working directory on FTP
        # isd-history is now stored there
        ftp.cwd('/pub/data/noaa/')
        success = False
        # Try to retrieve it
        try:
            ftp.retrbinary('RETR isd-history.csv',
                           open(isd_path, 'wb').write)
            success = True
        except Exception as err:
            print("'isd-history.csv' failed to download")
            print("  {}".format(err))
            success = False

        print("Success: isd-history.csv loaded")
        ftp.quit()

        return success

    def parse_isd(self, isd_path=None):
        """
        Loads the isd-history.csv into a pandas dataframe. Will serve to check
        if the station has data up to the year we want data from and get
        it's full name for reporting

        Args:
        ------
            isd_path (str, Optional): path to `isd-history.csv`.
                If None, will get the ISD instance's `isd_pat` that was
                set during initialization

        Returns:
        --------
            df_isd (pd.DataFrame): the isd-history loaded into a dataframe,
                indexed by "USAF-WBAN"

        Needs:
        -------------------------------
            import os
            import pandas as pd

        """

        # Defaults the isd_path
        if isd_path is None:
            isd_path = self.isd_path
            print("Using ISD path: {}".format(isd_path))

        df_isd = pd.read_csv(isd_path, sep=",", parse_dates=[9, 10])

        # Need to format the USAF with leading zeros as needed
        # should always be len of 6, WBAN len 5
        # USAF now is a string, and has len 6 so no problem

        df_isd['StationID'] = (df_isd['USAF'] + '-' +
                               df_isd['WBAN'].map("{:05d}".format))

        df_isd = df_isd.set_index('StationID')

        return df_isd

    def distance(self, lat1, lon1, lat2, lon2):
        """
        Computes the Haversine distance between two positions

        """
        p = 0.017453292519943295
        a = 0.5 - cos((lat2-lat1)*p)/2 + \
            cos(lat1*p)*cos(lat2*p) * (1-cos((lon2-lon1)*p)) / 2
        return 12742 * asin(sqrt(a))

    def closest_weather_station(self, lat, lon):
        """
        Returns the USAF-WBAN of the closest weather station to the point
        specified by latitude and longitude as arguments

        """
        self.df['distance'] = self.df.apply(lambda x: self.distance(x['LAT'],
                                                                    x['LON'],
                                                                    lat, lon),
                                            axis=1)
        print(self.df.loc[self.df['distance'].argmin()])
        return self.df['distance'].argmin()


class GSOD():

    def __init__(self, isd_path=None, weather_dir=None):
        """
        Init the GSOD main object, and attaches an `isd` (class ISD) to it

        Args:
        ------
            isd_path (str): path to `isd-history.csv`, optional, will default
            to ../support/isd-history.csv

            weather_dir (str): path to folder to download weather files,
            will default to ../weather_files/

        """
        # Initiates an instance of ISD
        if isd_path is None:
            self.isd = ISD(ISD_PATH)
        else:
            self.isd = ISD(isd_path)

        if weather_dir is None:
            self.weather_dir = WEATHER_DIR
        else:
            self.weather_dir = weather_dir

        # Instantiates a list of USAF-WBANs to download for
        self.stations = []

        # Defaults to current year
        self.years = [datetime.date.today().year]

    def set_years(self, years):
        """
        Sets the year to download data on the GSOD object

        Args:
        -----
            years (list of int): years to download data for

        Returns:
        --------
            years (list of int): All years to download data for

        """

        # Sanitize
        years = sorted([int(x) for x in years])

        self.years = years

        return years

    def set_years_range(self, start_year=2003, end_year=None):
        """
        Sets the year to download data on the GSOD object

        Args:
        -----
            start_year (int): start year, inclusive, defaults to 2003

            end_year (int): end year, inclusive, defaults to current

        Returns:
        --------
            years (list of int): All years to download data for

        """

        if end_year is None:
            end_year = datetime.date.today().year

        if end_year < start_year:
            raise ValueError("end_year cannot be lower than start_year")

        self.years = [x for x in range(start_year, end_year + 1)]

        return self.years

    def get_stations_from_file(self, weather_stations_file=None):
        """
        Loads in GSOD.stations the list of USAF-WBANs to download data for

        Will sanitize the USAF-WBAN provided to ensure that are of len 6 and
        len 5 respectively

        Args:
        -----
            weather_stations_file (str): Path to a file where Weather stations
                are stored, one on each line, with the syntax "USAF-WBAN"
                If None, looks in '{weather_dir}/weather_stations.txt'

        Returns:
        --------
            stations (list of str): sanitized

        """
        if weather_stations_file is None:
            weather_stations_file = os.path.join(self.weather_dir,
                                                 'weather_stations.txt')

        if not os.path.isfile(weather_stations_file):
            print("File '{}' doesn't exist."
                  "Place a file there in which you should put the USAF-WBAN "
                  "of stations you want, "
                  "one per line".format(weather_stations_file))

        # Load weather stations into lists. Stations[i] returns the stations
        # Can get it's length by calling 'len(stations)'
        # Will ignore everything that's after the pound `#` sign

        with open(weather_stations_file, 'r') as f:
            usaf_wbans = [line.split('#')[0].strip() for line in f.readlines()
                          if not line.startswith('#')]

        self.stations = [self.sanitize_usaf_wban(x) for x in usaf_wbans]

        return self.stations

    def set_stations(self, usaf_wbans):
        """
        Loads in GSOD.stations the list of USAF-WBANs to download data for

        Will sanitize the USAF-WBAN provided to ensure that are of len 6 and
        len 5 respectively

        Args:
        -----
            usaf_wbans (list of str): list of USAF-WBANs

        Returns:
        --------
            stations (list of str): sanitized

        """

        self.stations = [self.sanitize_usaf_wban(x) for x in usaf_wbans]

        return self.stations

    def sanitize_usaf_wban(self, usaf_wban):

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

    def get_all_data(self):
        """
        Downloads data from GSOD for all `years` and `stations`
        calls GSOD.download_GSOD_file()

        Args:
        ------
            None, `years`, `stations`, and `weather_dir`
                are stored as a GSOD attribute

        Returns:
        --------

            stats (3-uple): (n_success, n_doesnt_exists, n_outdated)

        Needs:
        -------------------------------
            from ftplib import FTP
            import os
            import pandas as pd

        """

        # c = done; r = doesn't exist; o = outdated, stopped before
        c = 0
        r = 0
        o = 0

        max_value = len(self.years) * len(self.stations)

        if max_value == 0:
            msg = ("Make sure you use GSOD.set_years or GSOD.set_years_range "
                   "AND GSOD.set_stations or GSOD.get_stations_from_file")

            raise ValueError(msg)

        with progressbar.ProgressBar(max_value=max_value) as bar:
            i = 0
            for year in self.years:
                for usaf_wban in self.stations:
                    i += 1

                    # Try downloading
                    (return_code,
                     op_path) = self.get_GSOD_file(year=year,
                                                   usaf_wban=usaf_wban)

                    print(op_path)
                    if return_code == ReturnCode.success:
                        c += 1
                    elif return_code == ReturnCode.missing:
                        r += 1
                    elif return_code == ReturnCode.outdated:
                        o += 1

                    bar.update(i)

        print("Success: {} files have been stored. ".format(c))
        print("{} station IDs didn't exist. ".format(r))
        print("{} stations stopped recording data before a year "
              "that was requested".format(o))

        return (c, r, o)

    def get_GSOD_file(self, year, usaf_wban, weather_dir=None, ftp=None):
        """
        Downloads and extracts data from GSOD from a single year for a single
        station.

        calls `GSOD.download_GSOD_file` followed by `GSOD.cleanup_extract

        Args:
        ------
            year (int): Year to download data for (format YYYY)

            usaf_wban (str): the USAF-WBAN (eg '064500-99999') to download data
            for

            ftp (ftplib.FTP): an instance of ftplib.FTP logged into
                ftp.ncdc.noaa.gov
                Will create one if None is passed

            weather_dir (str): path to the folder to store weather files, will
                default to self.weather_dir

        Returns:
        --------

            return_code (ReturnCode): an enum showing the return status
               ('success', 'missing', 'outdated')

            op_path (str): path to the uncompressed op_path,
                False if didn't work



        """

        op_path = False

        if weather_dir is None:
            weather_dir = self.weather_dir

        return_code, op_gz_path = self._download_GSOD_file(year=year,
                                                           usaf_wban=usaf_wban,
                                                           ftp=ftp)
        if return_code == ReturnCode.success:
            op_path = self._cleanup_extract_file(op_gz_path=op_gz_path,
                                                 delete_op_gz=True)

        return return_code, op_path

    def _download_GSOD_file(self, year, usaf_wban, weather_dir=None, ftp=None):
        """
        Downloads data from GSOD for a single year for a single station

        Loads the isd-history.csv in a pandas dataframe to check the station
        name and make sure there is actually data for the year we want
        (otherwise would try to download a file that doesn't exist)

        Args:
        ------
            year (int): Year to download data for (format YYYY)

            usaf_wban (str): the USAF-WBAN (eg '064500-99999') to download data
            for

            ftp (ftplib.FTP): an instance of ftplib.FTP logged into
                ftp.ncdc.noaa.gov
                Will create one if None is passed

            weather_dir (str): path to the folder to store weather files, will
                default to self.weather_dir

        Returns:
        --------
            return_code (ReturnCode): an enum showing the return status
               ('success', 'missing', 'outdated')

            local_path (str): the path to the downloaded *.op.gz file

        Needs:
        -------------------------------
            from ftplib import FTP
            import os
            import pandas as pd

        """

        if weather_dir is None:
            weather_dir = self.weather_dir

        # Test if folder doesn't exist
        if os.path.isdir(weather_dir) is False:
            # If not, create folder
            os.makedirs(weather_dir)

        # Whether you need to close the ftp connection or not
        to_close = False
        if ftp is None:
            to_close = True
            # Log to NOAA ftp
            ftp = FTP('ftp.ncdc.noaa.gov')
            ftp.login()
        else:
            if ftp.host != 'ftp.ncdc.noaa.gov':
                raise ValueError("ftp should be logged into "
                                 "'ftp.ncdc.noaa.gov' not into "
                                 "'{}'".format(ftp.host))

        # Load dataframe of isd-history
        df_isd = self.isd.df

        # Open an error log file.
        ferror = open(os.path.join(SUPPORT_DIR, 'errors.txt'), 'w')

        # Change current working directory (CWD)
        # os.chdir(weatherfolder)

        # Construct file names
        op_name = '{id}-{y}.op.gz'.format(id=usaf_wban, y=year)

        remote_folder = os.path.join('/pub/data/gsod/', str(year))
        local_folder = os.path.join(self.weather_dir, str(year))

        if not os.path.exists(local_folder):
            os.makedirs(local_folder)

        remote_path = os.path.join(remote_folder, op_name)
        local_path = os.path.join(local_folder, op_name)

        # Sanitize: should have been done already, but better safe... fast
        # anyways
        usaf_wban = self.sanitize_usaf_wban(usaf_wban)

        # Check if there's data or not
        end_year = df_isd.loc[usaf_wban, 'END'].year

        return_code = None
        if year <= end_year:

            # Retrieve file: open(fgsod, 'wb') opens a local file to receive
            # the distant blocks of binary data, in binary write mode
            # retrbinary(command, callback): command is a 'RETR filename',
            # and callback function is called for each block of data received:
            # here we write it to the local file

            # Try to retrieve it
            try:
                ftp.retrbinary('RETR ' + remote_path, open(local_path,
                                                           'wb').write)
                print("Station downloaded:" + df_isd.loc[usaf_wban,
                                                         'STATION NAME'])

                return_code = ReturnCode.success

            except Exception as err:
                return_code = ReturnCode.missing
                ferror.write(op_name + " doesn't exist\r\n")
                ferror.write("  {}".format(err))

        else:
            return_code = ReturnCode.outdated
            msg = ("{} doesn't have data up to this year. It stopped on:"
                   "{}".format(df_isd.loc[usaf_wban, 'STATION NAME'],
                               df_isd.loc[usaf_wban, 'END'].date()))
            warnings.warn(msg, UserWarning)

        if to_close:
            ftp.quit()

        return (return_code, local_path)

    def _cleanup_extract_file(self, op_gz_path, delete_op_gz=True):
        """
        Extracts the GSOD *.op.gz files to *.op and deletes the original gzip
        file. Also checks for empty files, and removes them

        Args:
        ------
            op_gz_path (str): the path to the op_gz_path file downloaded from
                GSOD

            delete_op_gz (bool): wether to delete the *.op.gz compressed file
                after decompressing
        Returns:
        --------
            op_path (str): path to the uncompressed *.op file

        Needs:
        -------------------------------
            import gzip
            import os
            import re

        """

        # If the file is empty, we delete it
        if os.path.getsize(op_gz_path) == 0:
            os.remove(op_gz_path)
        else:
            # If not, we extract
            #  Should return xxxx.op and .gz
            op_path, gz = os.path.splitext(op_gz_path)

            if gz == ".gz":
                print("unzipping '{}'".format(op_gz_path))
                # Open the gzip file
                with gzip.open(op_gz_path, 'rb') as in_file:
                    # Open a second file to write the uncompressed stream
                    with open(op_path, 'wb') as out_file:
                        out_file.write(in_file.read())

                # Deletes the op_gz_path
                if delete_op_gz:
                    os.remove(op_gz_path)
            else:
                raise ValueError("Was expecting an (.op).gz file to be passed,"
                                 " not: '{}'".format(op_gz_path))

        return op_path

    def cleanup_extract_all(year):
        """
        Extracts the GSOD *.op.gz files to *.op
        and deletes the original gzip file

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
        weatherfolder = os.path.join(WEATHER_DIR, str(year))

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

                            date_string = re.split(r'\s+', last)[2]
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

    gsod = GSOD()

    # This is what's run
    start_year = get_valid_year("Enter start year in YYYY format."
                                "Leave blank for current year "
                                "({}):\n".format(datetime.date.today().year))

    end_year = get_valid_year("Enter end year in YYYY format."
                              "Leave blank for current year "
                              "({}):\n".format(datetime.date.today().year))
    # Download the data
    gsod.set_years_range(start_year=start_year, end_year=end_year)

    # cleanup empty files, extract the gzip files, and delete them afterwards
    gsod.get_stations_from_file()

    print("Starting retrieving!")
    gsod.get_all_data()
