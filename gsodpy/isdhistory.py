import os
import time
from ftplib import FTP
import pandas as pd

# For the Haversine Formula
from math import cos, asin, sqrt

from gsodpy.constants import SUPPORT_DIR

ISDHISTORY_PATH = os.path.join(SUPPORT_DIR, 'isd-history.csv')


class ISDHistory():
    """
    Class for the ISDHistory file and methods
    """

    def __init__(self, isd_history_path=None):
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
            self.isd_history_path = isd_history_path

        # See if the isd_history needs updating, otherwise just link it
        self.update_isd_history(isd_history_path=self.isd_history_path)
        self.df = self.parse_isd(isd_history_path=self.isd_history_path)

    def update_isd_history(self, isd_history_path=None, force=False,
                           dry_run=False):
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

        # Defaults the isd_history_path
        if isd_history_path is None:
            isd_history_path = self.isd_history_path
            print("Using ISDHistory path: {}".format(isd_history_path))

        update_needed = False

        if os.path.isfile(isd_history_path):
            print("isd-history.csv was last modified on: %s" %
                  time.ctime(os.path.getmtime(self.isd_history_path)))
            # Check if the isd-history.csv is older than 1 month
            _d = (1 * 30 * 24 * 60 * 60)
            if (time.time() - os.path.getmtime(self.isd_history_path) > _d):
                update_needed = True
            elif force:
                print("Forcing update anyways")
                update_needed = True
        else:
            print("isd-history.csv not found, will download it")
            update_needed = True

        if update_needed:
            if not dry_run:
                self.download_isd(isd_history_path)

        else:
            print("No updates necessary: isd-history.csv is not"
                  " older than one month")

        return update_needed

    def download_isd(self, isd_history_path):
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

        ftp = FTP('ftp.ncdc.noaa.gov')
        ftp.login()

        # Change current working directory on FTP
        # isd-history is now stored there
        ftp.cwd('/pub/data/noaa/')
        success = False
        # Try to retrieve it
        try:
            ftp.retrbinary('RETR isd-history.csv',
                           open(isd_history_path, 'wb').write)
            success = True
        except Exception as err:
            print("'isd-history.csv' failed to download")
            print("  {}".format(err))
            success = False

        print("Success: isd-history.csv loaded")
        ftp.quit()

        return success

    def parse_isd(self, isd_history_path=None):
        """
        Loads the isd-history.csv into a pandas dataframe. Will serve to check
        if the station has data up to the year we want data from and get
        it's full name for reporting

        Args:
        ------
            isd_history_path (str, Optional): path to `isd-history.csv`.
                If None, will get the ISDHistory instance's `isd_pat` that was
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

        # Defaults the isd_history_path
        if isd_history_path is None:
            isd_history_path = self.isd_history_path
            print("Using ISDHistory path: {}".format(isd_history_path))

        df_isd = pd.read_csv(isd_history_path, sep=",", parse_dates=[9, 10])

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
