"""Downloading data from NOAA."""

# Make it backwards compatible with python 2
from __future__ import division, print_function

import datetime
import gzip
import os  # TODO: remove ASAP
import re
import warnings
from ftplib import FTP
from pathlib import Path
from typing import List, Optional, Tuple

from tqdm import tqdm

from pygsod.constants import SUPPORT_DIR, WEATHER_DIR
from pygsod.isdhistory import ISDHistory
from pygsod.utils import DataType, ReturnCode, as_path, sanitize_usaf_wban


class NOAAData:
    """Main class for downloading data from NOAA FTP."""

    def __init__(self, data_type, isd_path: Optional[Path] = None, weather_dir: Optional[Path] = None):
        """Init the NOAAData main object, and attaches an `isd` (class ISD) to it.

        Args:
        ------
        - data_type (DataType): the type of data to fetch
        - isd_path (Path): path to `isd-history.csv`, optional, will default
          to ../support/isd-history.csv

        - weather_dir (Path): path to folder to download weather files,
            will default to ../weather_files/

        """
        # Initiates an instance of ISD
        self.isd = ISDHistory(isd_path)

        if not (isinstance(data_type, DataType)):
            raise ValueError("Wrong data_type passed, expected DataType")

        _folder_name = None
        self.data_type = data_type
        self.gz_ext = "gz"
        if data_type == DataType.gsod:
            self.ftp_folder = Path("/pub/data/gsod/")
            _folder_name = "gsod"
            self.gz_ext = "op.gz"
        elif data_type == DataType.isd_full:
            self.ftp_folder = Path("/pub/data/noaa/")
            _folder_name = "isd_full"
        elif data_type == DataType.isd_lite:
            self.ftp_folder = Path("/pub/data/noaa/isd-lite")
            _folder_name = "isd_lite"
        else:
            raise NotImplementedError("DataType not supported: " "{}".format(data_type))

        if weather_dir is None:
            self.weather_dir = WEATHER_DIR / _folder_name
        else:
            self.weather_dir = as_path(weather_dir)

        # Instantiates a list of USAF-WBANs to download for
        self.stations: List[str] = []

        # Defaults to current year
        self.years = [datetime.date.today().year]

        self.ops_files: List[Path] = []
        self.ftp: Optional[FTP] = None

    def set_years(self, years: List[int]) -> None:
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

    def set_years_range(self, start_year: int = 2003, end_year: Optional[int] = None) -> List[int]:
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
            weather_stations_file = self.weather_dir / "weather_stations.txt"
        else:
            weather_stations_file = as_path(weather_stations_file)

        if not weather_stations_file.is_file():
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
            usaf_wbans = [line.split("#")[0].strip() for line in f.readlines() if not line.startswith("#")]

        self.stations = [sanitize_usaf_wban(x) for x in usaf_wbans]

        return self.stations

    def get_stations_from_user_input(self, country, state, station_name, latitude, longitude, year=None):
        """
        convert country, state, station name input by user into USAF-WBANs

        Args:
        -----
            args: input by user with country, state and station_name info

        Returns:
        --------
            stations (list of str): sanitized

        """
        if (country is not None) and (station_name is not None):
            df = self.isd.df

            if state is None:
                df_sub = df[(df["CTRY"] == country) & (df["STATION NAME"] == station_name)]
            else:
                df_sub = df[(df["CTRY"] == country) & (df["STATE"] == state) & (df["STATION NAME"] == station_name)]
            if len(df_sub) == 0:
                raise ValueError("The input country, state and station name is not " "found in isd-history.")
            else:
                self.stations = [str(df_sub["USAF"].values[0]) + "-" + str(df_sub["WBAN"].values[0])]
                print(self.stations)
                return self.stations

        elif (latitude is not None) and (longitude is not None):
            self.stations = [self.isd.closest_weather_station(lat=latitude, lon=longitude, year=year)]
            return self.stations
        else:
            raise ValueError(
                "You must provide usaf AND wban, " "OR country AND station_name, " "OR latitude AND longitude."
            )

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
        """

        # c = done; r = doesn't exist; o = outdated, stopped before
        c = 0
        r = 0
        o = 0

        max_value = len(self.years) * len(self.stations)

        if max_value == 0:
            msg = "Make sure you use `set_years` or `set_years_range` " "AND `set_stations` or `get_stations_from_file`"

            raise ValueError(msg)

        final_close = self.ftp is None
        i = 0
        for year in tqdm(self.years):
            for usaf_wban in self.stations:
                i += 1

                # Try downloading, force not closing the connection yet
                (return_code, op_path) = self.get_year_file(year=year, usaf_wban=usaf_wban, to_close=False)

                print(op_path)
                if return_code == ReturnCode.success:
                    c += 1
                    self.ops_files.append(op_path)
                elif return_code == ReturnCode.missing:
                    r += 1
                elif return_code == ReturnCode.outdated:
                    o += 1

                # bar.update(i)

        if self.ftp is not None and final_close:
            self.ftp.close()
            self.ftp = None

        print("Success: {} files have been stored. ".format(c))
        print("{} station IDs didn't exist. ".format(r))
        print("{} stations stopped recording data before a year " "that was requested".format(o))

        return (c, r, o)

    def get_year_file(self, year, usaf_wban, to_close=None):
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

            to_close (optional bool): whether to close the ftp connection after download
                True: force close
                False: force keep alive (even if it was first created during this call)
                None: if self.ftp is None originally, will close it, otherwise do nothing

        Returns:
        --------

            return_code (ReturnCode): an enum showing the return status
               ('success', 'missing', 'outdated')

            op_path (str): path to the uncompressed op_path,
                False if didn't work

        """

        op_path = False

        # TODO: CHECK IF FILE EXISTS BEFORE DOWNLOADING
        return_code, op_gz_path = self._get_year_file(year=year, usaf_wban=usaf_wban, to_close=to_close)
        if return_code == ReturnCode.success:
            op_path = self._cleanup_extract_file(op_gz_path=op_gz_path, delete_op_gz=True)

        return return_code, op_path

    def _get_year_file(self, year: int, usaf_wban: str, to_close=None) -> Tuple[ReturnCode, Path]:
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

            to_close (optional bool): whether to close the ftp connection after download

        Returns:
        --------
            return_code (ReturnCode): an enum showing the return status
               ('success', 'missing', 'outdated')

            local_path (str): the path to the downloaded *(.op).gz file

        Needs:
        -------------------------------
            from ftplib import FTP
            import pandas as pd

        """

        # Test if folder doesn't exist, create folder
        self.weather_dir.mkdir(parents=True, exist_ok=True)

        # Whether you need to close the ftp connection or not
        if self.ftp is None:
            # Log to NOAA ftp
            self.ftp = FTP("ftp.ncdc.noaa.gov")
            self.ftp.login()
            if to_close is None:
                to_close = True
        else:
            if self.ftp.host != "ftp.ncdc.noaa.gov":
                raise ValueError(
                    "ftp should be logged into " "'ftp.ncdc.noaa.gov' not into " "'{}'".format(self.ftp.host)
                )

        # Load dataframe of isd-history
        df_isd = self.isd.df

        # Open an error log file.
        ferror = open(SUPPORT_DIR / "errors.txt", "w")

        # Sanitize: should have been done already, but better safe... fast
        # anyways
        usaf_wban = sanitize_usaf_wban(usaf_wban)

        # Construct file names
        remote_op_name = "{id}-{y}.{e}".format(id=usaf_wban, y=year, e=self.gz_ext)

        local_op_name = "{s}-{y}.{e}".format(
            # replace slash in the station name to not infer on the Path
            s=df_isd.loc[usaf_wban, "STATION NAME"].replace("/", " "),
            y=year,
            e=self.gz_ext,
        )

        remote_folder = self.ftp_folder / str(year)
        local_folder = (self.weather_dir / str(year)).resolve()
        local_folder.mkdir(parents=True, exist_ok=True)

        remote_path = (remote_folder / remote_op_name).as_posix()
        local_path = local_folder / local_op_name

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
                self.ftp.retrbinary("RETR " + str(remote_path), open(local_path, "wb").write)
                print("Station downloaded:" + df_isd.loc[usaf_wban, "STATION NAME"])

                return_code = ReturnCode.success

            except Exception as err:
                return_code = ReturnCode.missing
                ferror.write(remote_op_name + " doesn't exist\r\n")
                ferror.write("  {}".format(err))

        else:
            return_code = ReturnCode.outdated
            msg = "{} doesn't have data up to this year. It stopped on:" "{}".format(
                df_isd.loc[usaf_wban, "STATION NAME"],
                df_isd.loc[usaf_wban, "END"].date(),
            )
            warnings.warn(msg, UserWarning)

        if to_close:
            self.ftp.close()
            self.ftp = None

        return (return_code, local_path)

    def _cleanup_extract_file(self, op_gz_path: Path, delete_op_gz: bool = True) -> Path:
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
        if op_gz_path.stat().st_size == 0:
            op_gz_path.unlink()
        else:
            # If not, we extract
            #  Should return xxxx.op and .gz
            op_path = op_gz_path.with_suffix("")  # or `op_gz_path.parent / op_gz_path.stem`
            gz = op_gz_path.suffix

            if gz == ".gz":
                print(f"unzipping '{op_gz_path}'")
                # Open the gzip file
                with gzip.open(op_gz_path, "rb") as in_file:
                    # Open a second file to write the uncompressed stream
                    with open(op_path, "wb") as out_file:
                        out_file.write(in_file.read())

                # Deletes the op_gz_path
                if delete_op_gz:
                    op_gz_path.unlink()
            else:
                raise ValueError("Was expecting an (.op).gz file to be passed," f" not: '{op_gz_path}'")

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
        weatherfolder = self.weather_dir / str(year)

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
                            next(out_file).decode()
                            out_file.seek(-276, 2)
                            last = out_file.readlines()[-1].decode()

                            date_string = re.split(r"\s+", last)[2]
                            date = datetime.datetime.strptime(date_string, "%Y%m%d").strftime("%d %b %Y")

                            print("Data up to {}".format(date))
