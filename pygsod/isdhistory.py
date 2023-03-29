import time
from ftplib import FTP

# For the Haversine Formula
from math import asin, cos, sqrt
from pathlib import Path
from typing import Optional

import pandas as pd

from pygsod.constants import ISDHISTORY_PATH
from pygsod.utils import as_path


class ISDHistory:
    """
    Class for the ISDHistory file and methods
    """

    def __init__(self, isd_history_path: Optional[Path] = None):
        """
        Init the ISDHistory. Checks if exists, if not downloads it,
        stores the outpath

        Args:
        ------
            isd_history_path (str): path to `isd-history.csv`, optional,
            will default to ../support/isd-history.csv

        """
        # If isd_history_path isn't supplied, set it to the default path
        if isd_history_path is None:
            self.isd_history_path = ISDHISTORY_PATH
        else:
            self.isd_history_path = as_path(isd_history_path)

        # See if the isd_history needs updating, otherwise just link it
        self.update_isd_history()
        self._parse_isd()

    def update_isd_history(self, force=False, dry_run=False):
        """
        Will download the `isd-history.csv` file
        if one of two conditions are true:
            * The `isd-history.csv` does not exist in the ../support/ folder
            * the `isd-history.csv` is older than 1 month

        `isd-history.csv` is the list of weather stations, it includes start
            and end dates

        Args:
        ------
            isd_history_path (str, Optional): path to `isd-history.csv`.
                If None, will get the ISDHistory instance's `isd_pat` that was
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

        update_needed = False

        if self.isd_history_path.is_file():
            tm_time = self.isd_history_path.lstat().st_mtime
            print("isd-history.csv was last modified on: %s" % time.ctime(tm_time))
            # Check if the isd-history.csv is older than 1 month
            _d = 1 * 30 * 24 * 60 * 60
            if time.time() - tm_time > _d:
                update_needed = True
            elif force:
                print("Forcing update anyways")
                update_needed = True
        else:
            print("isd-history.csv not found, will download it")
            update_needed = True

        if update_needed:
            if not dry_run:
                ret = self.download_isd(isd_history_path=self.isd_history_path)
                if not ret:
                    raise ValueError(
                        f"Something went wrong when downloading isd-history.csv to {self.isd_history_path}"
                    )

        else:
            print("No updates necessary: isd-history.csv is not" " older than one month")

        return update_needed

    @staticmethod
    def download_isd(isd_history_path: Path):
        """
        Downloads the isd-history.csv from NOAA

        Args:
        -----
            isd_history_path (str): path on local disk to save the file

        Returns:
        --------
            success (bool): whether it worked or not

        Needs:
        -------------------------------
            from ftplib import FTP

        """

        ftp = FTP("ftp.ncdc.noaa.gov")
        ftp.login()

        # Change current working directory on FTP
        # isd-history is now stored there
        ftp.cwd("/pub/data/noaa/")
        success = False
        # Try to retrieve it
        try:
            ftp.retrbinary("RETR isd-history.csv", open(isd_history_path, "wb").write)
            success = True
        except Exception as err:
            print("'isd-history.csv' failed to download")
            print("  {}".format(err))
            return False

        print("Success: isd-history.csv loaded")
        ftp.quit()

        return success

    def _parse_isd(self):
        """
        Loads the isd-history.csv into a pandas dataframe.

        Will serve to check if the station has data up to the year we want data from and get
        its full name for reporting
        """

        self.df = pd.read_csv(self.isd_history_path, sep=",", parse_dates=[9, 10])

        # Need to format the USAF with leading zeros as needed
        # should always be len of 6, WBAN len 5
        # USAF now is a string, and has len 6 so no problem

        self.df["StationID"] = self.df["USAF"] + "-" + self.df["WBAN"].map("{:05d}".format)

        self.df = self.df.set_index("StationID")

    @staticmethod
    def distance(lat1, lon1, lat2, lon2):
        """
        Computes the Haversine distance between two positions

        """
        p = 0.017453292519943295
        a = 0.5 - cos((lat2 - lat1) * p) / 2 + cos(lat1 * p) * cos(lat2 * p) * (1 - cos((lon2 - lon1) * p)) / 2
        return 12742 * asin(sqrt(a))

    def closest_weather_station(self, lat, lon, year=None):
        """
        Returns the USAF-WBAN of the closest weather station to the point
        specified by latitude and longitude as arguments

        """
        self.df["distance"] = self.df.apply(
            lambda x: ISDHistory.distance(lat1=x["LAT"], lon1=x["LON"], lat2=lat, lon2=lon),
            axis=1,
        )
        # print(self.df.loc[self.df['distance'].argmin()])

        df = self.df.copy()
        if year is not None:
            df = df[df["END"].dt.year >= year]
        d = df["distance"].argmin()
        return df.index[d]
