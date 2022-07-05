# Make it backwards compatible with python 2
from __future__ import print_function, division
import sys

import os
from ftplib import FTP
import gzip
import pandas as pd

import datetime
import re


from tqdm import tqdm

# import progressbar

from gsodpy.constants import SUPPORT_DIR, WEATHER_DIR
from gsodpy.utils import is_list_like, ReturnCode, DataType, sanitize_usaf_wban
from gsodpy.isdhistory import ISDHistory

import warnings


class NOAAData:
    def __init__(self, data_type, isd_path=None, weather_dir=None):
        """
        Init the NOAAData main object, and attaches an `isd` (class ISD) to it

        Args:
        ------
            isd_path (str): path to `isd-history.csv`, optional, will default
            to ../support/isd-history.csv

            weather_dir (str): path to folder to download weather files,
            will default to ../weather_files/

        """
        # Initiates an instance of ISD
        self.isd = ISDHistory(isd_path)

        if not (isinstance(data_type, DataType)):
            raise "Wrong data_type passed, expected DataType"

        _folder_name = None
        self.data_type = data_type
        self.gz_ext = "gz"
        if data_type == DataType.gsod:
            self.ftp_folder = "/pub/data/gsod/"
            _folder_name = "gsod"
            self.gz_ext = "op.gz"
        elif data_type == DataType.isd_full:
            self.ftp_folder = "/pub/data/noaa/"
            _folder_name = "isd_full"
        elif data_type == DataType.isd_lite:
            self.ftp_folder = "/pub/data/noaa/isd-lite"
            _folder_name = "isd_lite"
        else:
            raise NotImplementedError(
                "DataType not supported: " "{}".format(data_type)
            )

        if weather_dir is None:
            self.weather_dir = os.path.join(WEATHER_DIR, _folder_name)
        else:
            self.weather_dir = weather_dir

        # Instantiates a list of USAF-WBANs to download for
        self.stations = []

        # Defaults to current year
        self.years = [datetime.date.today().year]

        self.ops_files = []

    def set_years(self, years):
        """
        Sets the year to download data on the NOAAData object

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
        Sets the year to download data on the NOAAData object

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
        Loads in the list of USAF-WBANs to download data for from the file

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
            weather_stations_file = os.path.join(
                self.weather_dir, "weather_stations.txt"
            )

        if not os.path.isfile(weather_stations_file):
            print(
                "File '{}' doesn't exist."
                "Place a file there in which you should put the USAF-WBAN "
                "of stations you want, "
                "one per line".format(weather_stations_file)
            )

        # Load weather stations into lists. Stations[i] returns the stations
        # Can get it's length by calling 'len(stations)'
        # Will ignore everything that's after the pound `#` sign
        with open(weather_stations_file, "r") as f:
            usaf_wbans = [
                line.split("#")[0].strip()
                for line in f.readlines()
                if not line.startswith("#")
            ]

        self.stations = [sanitize_usaf_wban(x) for x in usaf_wbans]

        return self.stations

    def get_stations_from_user_input(
        self, country, state, station_name, latitude, longitude, year=None
    ):
        """
        convert country, state, station name input by user into USAF-WBANs

        Args:
        -----
            args: input by user with country, state and station_name info

        Returns:
        --------
            stations (list of str): sanitized

        """
        isd_history_file_name = os.path.join(SUPPORT_DIR, "isd-history.csv")
        df = pd.read_csv(isd_history_file_name)
        print("Station name", station_name)
        if (country is not None) and (station_name is not None):

            isd_history_file_name = os.path.join(SUPPORT_DIR, 'isd-history.csv')
            df = pd.read_csv(isd_history_file_name)

            if state is None:
                df_sub = df[
                    (df["CTRY"] == country)
                    & (df["STATION NAME"] == station_name)
                ]
            else:
                df_sub = df[
                    (df["CTRY"] == country)
                    & (df["STATE"] == state)
                    & (df["STATION NAME"] == station_name)
                ]
            if len(df_sub) == 0:
                raise ValueError(
                    "The input country, state and station name is not "
                    "found in isd-history."
                )
            else:
                self.stations = [
                    str(df_sub["USAF"].values[0])
                    + "-"
                    + str(df_sub["WBAN"].values[0])
                ]
                print(self.stations)
                return self.stations

        elif (usaf is not None) and (wban is not None):
            self.stations = [str(usaf) + '-' + str(wban)]
            return self.stations

        elif (latitude is not None) and (longitude is not None):
            self.stations = [
                self.isd.closest_weather_station(latitude, longitude, year)
            ]
            return self.stations
        else:
            raise ValueError(
                "You must provide usaf AND wban, "
                "OR country AND station_name, "
                "OR latitude AND longitude.")

    def set_stations(self, usaf_wbans):
        """
        Sets the list of USAF-WBANs to download data for

        Will sanitize the USAF-WBAN provided to ensure that are of len 6 and
        len 5 respectively

        Args:
        -----
            usaf_wbans (list of str): list of USAF-WBANs

        Returns:
        --------
            stations (list of str): sanitized

        """

        self.stations = [sanitize_usaf_wban(x) for x in usaf_wbans]

        return self.stations

    def get_all_data(self):
        """
        Downloads data from the appropriate source (GSOD, ISD, ISD_LITE)
        for all `years` and `stations`
        calls `get_year_file()`

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
            msg = (
                "Make sure you use `set_years` or `set_years_range` "
                "AND `set_stations` or `get_stations_from_file`"
            )

            raise ValueError(msg)

        # with progressbar.ProgressBar(max_value=max_value) as bar:
        i = 0
        for year in tqdm(self.years):
            for usaf_wban in self.stations:
                i += 1

                # Try downloading
                (return_code, op_path) = self.get_year_file(
                    year=year, usaf_wban=usaf_wban
                )

                print(op_path)
                if return_code == ReturnCode.success:
                    c += 1
                    self.ops_files.append(op_path)
                elif return_code == ReturnCode.missing:
                    r += 1
                elif return_code == ReturnCode.outdated:
                    o += 1

                # bar.update(i)

        print("Success: {} files have been stored. ".format(c))
        print("{} station IDs didn't exist. ".format(r))
        print(
            "{} stations stopped recording data before a year "
            "that was requested".format(o)
        )

        return (c, r, o)

    def get_year_file(self, year, usaf_wban, weather_dir=None, ftp=None):
        """
        Downloads and extracts data from the appropriate source (GSOD, ISD,
        etc) from a single year for a single station.

        calls `GSOD._get_year_file` followed by `GSOD._cleanup_extract_file`

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

        # TODO: CHECK IF FILE EXISTS BEFORE DOWNLOADING

        return_code, op_gz_path = self._get_year_file(
            year=year, usaf_wban=usaf_wban, ftp=ftp
        )
        if return_code == ReturnCode.success:
            op_path = self._cleanup_extract_file(
                op_gz_path=op_gz_path, delete_op_gz=True
            )

        return return_code, op_path

    def _get_year_file(self, year, usaf_wban, weather_dir=None, ftp=None):
        """
        Downloads data for a single year for a single station

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

            local_path (str): the path to the downloaded *(.op).gz file

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
            ftp = FTP("ftp.ncdc.noaa.gov")
            ftp.login()
        else:
            if ftp.host != "ftp.ncdc.noaa.gov":
                raise ValueError(
                    "ftp should be logged into "
                    "'ftp.ncdc.noaa.gov' not into "
                    "'{}'".format(ftp.host)
                )

        # Load dataframe of isd-history
        df_isd = self.isd.df

        # Open an error log file.
        ferror = open(os.path.join(SUPPORT_DIR, "errors.txt"), "w")

        # Change current working directory (CWD)
        # os.chdir(weatherfolder)

        # Sanitize: should have been done already, but better safe... fast
        # anyways
        usaf_wban = sanitize_usaf_wban(usaf_wban)

        # Construct file names
        remote_op_name = "{id}-{y}.{e}".format(
            id=usaf_wban, y=year, e=self.gz_ext
        )

        local_op_name = "{s}-{y}.{e}".format(
            # replace slash in the station name to not infer on the Path
            s=df_isd.loc[usaf_wban, "STATION NAME"].replace("/", " "),
            y=year,
            e=self.gz_ext,
        )

        remote_folder = os.path.join(self.ftp_folder, str(year))
        local_folder = os.path.join(self.weather_dir, str(year))

        if not os.path.exists(local_folder):
            os.makedirs(local_folder)

        remote_path = os.path.join(remote_folder, remote_op_name).replace(
            "\\", "/"
        )
        local_path = os.path.join(local_folder, local_op_name).replace(
            "\\", "/"
        )

        # Check if there's data or not
        end_year = df_isd.loc[usaf_wban, "END"].year

        return_code = None
        if year <= end_year:

            # Retrieve file: open(fgsod, 'wb') opens a local file to receive
            # the distant blocks of binary data, in binary write mode
            # retrbinary(command, callback): command is a 'RETR filename',
            # and callback function is called for each block of data received:
            # here we write it to the local file

            # Try to retrieve it
            try:
                ftp.retrbinary(
                    "RETR " + remote_path, open(local_path, "wb").write
                )
                print(
                    "Station downloaded:"
                    + df_isd.loc[usaf_wban, "STATION NAME"]
                )

                return_code = ReturnCode.success

            except Exception as err:
                return_code = ReturnCode.missing
                ferror.write(remote_op_name + " doesn't exist\r\n")
                ferror.write("  {}".format(err))

        else:
            return_code = ReturnCode.outdated
            msg = (
                "{} doesn't have data up to this year. It stopped on:"
                "{}".format(
                    df_isd.loc[usaf_wban, "STATION NAME"],
                    df_isd.loc[usaf_wban, "END"].date(),
                )
            )
            warnings.warn(msg, UserWarning)

        if to_close:
            ftp.quit()

        return (return_code, local_path)

    def _cleanup_extract_file(self, op_gz_path, delete_op_gz=True):
        """
        Extracts the individual *(.op).gz files to *.op and deletes the original
        gzip file. Also checks for empty files, and removes them

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
                with gzip.open(op_gz_path, "rb") as in_file:
                    # Open a second file to write the uncompressed stream
                    with open(op_path, "wb") as out_file:
                        out_file.write(in_file.read())

                # Deletes the op_gz_path
                if delete_op_gz:
                    os.remove(op_gz_path)
            else:
                raise ValueError(
                    "Was expecting an (.op).gz file to be passed,"
                    " not: '{}'".format(op_gz_path)
                )

        return op_path

    def cleanup_extract_all(self, year):
        """
        Extracts the GSOD *(.op).gz files to *.op
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
        weatherfolder = os.path.join(self.weather_dir, str(year))

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
                        in_file = gzip.open(path, "rb")
                        # Open a second file to write the uncompressed stream
                        out_file = open(outpath, "wb")
                        out_file.write(in_file.read())

                        # close both
                        in_file.close()
                        out_file.close()
                        os.remove(path)

                        # Print latest date
                        with open(outpath, "rb") as out_file:
                            first = next(out_file).decode()
                            out_file.seek(-276, 2)
                            last = out_file.readlines()[-1].decode()

                            date_string = re.split(r"\s+", last)[2]
                            date = datetime.datetime.strptime(
                                date_string, "%Y%m%d"
                            ).strftime("%d %b %Y")

                            print("Data up to {}".format(date))
