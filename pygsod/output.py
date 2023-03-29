from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

from pygsod.constants import RESULT_DIR
from pygsod.epw_converter import clean_df, epw_convert
from pygsod.ish_full import parse_ish_file
from pygsod.noaadata import NOAAData
from pygsod.tmy_download import TMY
from pygsod.utils import DataType, FileType, OutputType


class GetOneStation(object):
    """Call the API to download the data for the selected weather station
    and save the data in the selected format"""

    def __init__(
        self,
        type_of_file: FileType,
        type_of_output: OutputType,
        start_year: int,
        end_year: int,
        hdd_threshold: float = 65.0,
        cdd_threshold: float = 65.0,
        # Either this
        country: Optional[str] = None,
        station_name: Optional[str] = None,
        state: Optional[str] = None,  # Optional
        # Or that
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
    ):
        """Constructor for GetOneStation

        Inputs:
        --------

        - type_of_file (FileType)
        - type_of_ouptut (OutputType)
        - start_year (int)
        - end_year (int)
        - hdd_threshold (int, Optional): CDD threshold tmeperature in degree F
        - cdd_threshold (int, Optional): CDD theshold temperature in degree F

        Provide either
        - country (str)
        - station_name (str)
        - state (str, Optional)

        Or:
        - latitude (str or None)
        - longitude (str or None)
        """
        self.type_of_file = type_of_file
        self.type_output = type_of_output
        self.hdd_threshold = hdd_threshold
        self.cdd_threshold = cdd_threshold

        self.country = country
        self.state = state
        self.station_name = station_name

        self.latitude = latitude
        self.longitude = longitude

        self.start_year = start_year
        self.end_year = end_year

        if (self.country is None or self.station_name is None) and (self.latitude is None or self.latitude is None):
            raise ValueError("You must provide at least station_name and country, or latitude and lontigude")

        if self.country is not None:
            self.filenamestub = f"{self.country}-{self.station_name}"
        else:
            self.filenamestub = f"{self.latitude}-{self.longitude}"
        if self.type_of_file == FileType.Historical:
            self.filenamestub += f"-Historical-{self.start_year}-{self.end_year}"
        elif self.type_of_file == FileType.TMY:
            self.filenamestub += "-TMY3"
        else:
            raise NotImplementedError(f"Could not understand type_of_file={self.type_of_file}")

    def run(self):
        self.list_files = self._get_data()

        # output files
        for file in self.list_files:
            o = Output(file, self.type_output, self.hdd_threshold, self.cdd_threshold)
            o.output_files()

    def get_one_dataframe(self):
        self.list_files = self._get_data()

        df_hourly = pd.DataFrame()
        df_daily = pd.DataFrame()
        df_monthly = pd.DataFrame()
        for file in self.list_files:
            o = Output(file, self.type_output, self.hdd_threshold, self.cdd_threshold)
            df_h, df_d, df_m = o.create_dataframe()

            df_hourly = pd.concat([df_hourly, df_h])
            df_daily = pd.concat([df_daily, df_d])
            df_monthly = pd.concat([df_monthly, df_m])

        self.df_hourly = df_hourly
        self.df_daily = df_daily
        self.df_monthly = df_monthly

    def _get_data(self):
        if self.type_of_file == FileType.Historical:
            # download isd_full
            list_files = self._download_historical_data()

        elif self.type_of_file == FileType.TMY:
            # download weather data from EP+ website
            tmy_data = TMY(self.country, self.station_name, self.state)
            self.tmy = tmy_data
            list_files = [tmy_data.fname]

        else:
            raise ValueError(
                "The type of file is not correct, it should be" f" TMY or historical not {self.type_of_file}"
            )

        return list_files

    def _download_historical_data(self):
        self.isd_full = NOAAData(data_type=DataType.isd_full)

        self.isd_full.set_years_range(start_year=self.start_year, end_year=self.end_year)

        self.isd_full.get_stations_from_user_input(
            self.country,
            self.state,
            self.station_name,
            self.latitude,
            self.longitude,
            self.end_year,
        )

        self.isd_full.get_all_data()
        parse_ish_file(self.isd_full)

        list_ops_files = self.isd_full.ops_files

        return list_ops_files


class Output(object):
    """Class for output weather data into specific type"""

    def __init__(
        self,
        file: Path,
        type_of_output: OutputType,
        hdd_threshold: float = 65.0,
        cdd_threshold: float = 65.0,
    ):
        """Constructs an Output object."""

        self.file = Path(file)
        self.op_file_name = self.file.name

        self._file_names()

        self.type_of_output = type_of_output
        self.hdd_threshold = hdd_threshold
        self.cdd_threshold = cdd_threshold

    def calculate_hdd(self, temp: float) -> float:
        """Calculates Heating Degree Days for a temperature."""
        if temp <= self.hdd_threshold:
            return self.hdd_threshold - temp
        else:
            return 0.0

    def calculate_cdd(self, temp: float) -> float:
        """function to calculate cdd"""
        if temp >= self.cdd_threshold:
            return temp - self.cdd_threshold
        else:
            return 0.0

    def get_hourly_data(self) -> pd.DataFrame:
        full_path = self.file
        df_path = "{f}.{ext}".format(f=full_path, ext="xlsx")
        df = pd.read_excel(df_path, index_col=0)
        df = clean_df(df, self.op_file_name)

        return df

    def _file_names(self):
        self.hourly_file_name = RESULT_DIR / (self.op_file_name + "-hourly")

        # daily
        self.daily_file_name = RESULT_DIR / (self.op_file_name + "-daily")

        # monthly
        self.monthly_file_name = RESULT_DIR / (self.op_file_name + "-monthly")

    def output_daily(self, df_hourly: pd.DataFrame) -> pd.DataFrame:
        """output daily data by grouping by daily
        used in output_files()
        """
        df_daily = df_hourly.groupby(by=df_hourly.index.date).mean()  # type: ignore
        df_daily.index = pd.to_datetime(df_daily.index)  # reset index to datetime

        # remove unnecessary columns for daily
        for col in ["AZIMUTH_ANGLE", "ZENITH_ANGLE", "WIND_DIRECTION"]:
            if col in df_daily.columns:
                df_daily.drop(columns=[col], inplace=True)

        df_daily["HDD_F"] = df_daily["TEMP_F"].apply(self.calculate_hdd)
        df_daily["CDD_F"] = df_daily["TEMP_F"].apply(self.calculate_cdd)

        return df_daily

    def output_monthly(self, df_hourly: pd.DataFrame, df_daily: pd.DataFrame) -> pd.DataFrame:
        """output monthly data
        used in output_files()
        """
        df_monthly = df_hourly.groupby(pd.Grouper(freq="1M")).mean()  # type: ignore

        # remove unnecessary columns for daily
        for col in ["AZIMUTH_ANGLE", "ZENITH_ANGLE", "WIND_DIRECTION"]:
            if col in df_monthly.columns:
                df_monthly.drop(columns=[col], inplace=True)

        # monthly_hdd = []
        # monthly_cdd = []
        # for month in range(1, df_hourly.index.month[-1] + 1):
        #     monthly_hdd.append(
        #         df_daily[df_daily.index.month == month]['HDD_F'].sum())
        #     monthly_cdd.append(
        #         df_daily[df_daily.index.month == month]['CDD_F'].sum())
        # df_monthly['HDD_F'] = monthly_hdd
        # df_monthly['CDD_F'] = monthly_cdd

        df_hdd_cdd = df_daily.groupby(pd.Grouper(freq="1M")).sum()[["HDD_F", "CDD_F"]]  # type: ignore
        df_monthly = df_hdd_cdd.merge(df_monthly, how="left", right_index=True, left_index=True)

        return df_monthly

    def output_files(self):
        """output epw, csv or json file for each weather data for the op_file.

        epw: hourly

        csv: hourly, daily, monthly

        json: daily, monthly
        """
        # for root, dirs, files in os.walk(WEATHER_DIR + '/isd_full'):
        #     for file in files:
        #         if file.endswith("xlsx"):

        # full_path = self.file
        # op_file_name = self.file.split('/')[-1]

        df = self.get_hourly_data()
        df.to_csv(str(self.hourly_file_name) + ".csv")
        self.df_hourly = df

        # epw
        if self.type_of_output == OutputType.EPW:
            epw_convert(df, self.op_file_name)

        else:
            # daily
            df_daily = self.output_daily(df_hourly=df)
            self.df_daily = df_daily
            # monthly
            df_monthly = self.output_monthly(df_hourly=df, df_daily=df_daily)
            self.df_monthly = df_monthly

            # csv
            if self.type_of_output == OutputType.CSV:
                df_daily.to_csv(str(self.daily_file_name) + ".csv")
                df_monthly.to_csv(str(self.monthly_file_name) + ".csv")

            # json
            elif self.type_of_output == OutputType.JSON:
                df_daily.to_json(str(self.daily_file_name) + ".json")
                df_monthly.to_json(str(self.monthly_file_name) + ".json")

            elif self.type_of_output == OutputType.XLSX:
                df_daily.to_excel(str(self.daily_file_name) + ".xlsx")
                df_monthly.to_excel(str(self.monthly_file_name) + ".xlsx")

            else:
                raise NotImplementedError(f"OutputType={self.type_of_output} is not implemented.")

    def create_dataframe(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        df = self.get_hourly_data()
        self.df = df
        df_daily = self.output_daily(df_hourly=df)
        self.df_daily = df_daily
        df_monthly = self.output_monthly(df_hourly=df, df_daily=df_daily)

        return df, df_daily, df_monthly

    # def output_files_from_epw(self):
    #     """output csv or json file from epw file downloaded from EP+ website.
    #
    #        csv: hourly, daily, monthly
    #
    #        json: daily, monthly
    #
    #     """
    #     # export df_hourly
    #     # file_name = self.file.split('/')[-1]
    #     # hourly_file_name = os.path.join(
    #     #     RESULT_DIR, self.op_file_name[:-4] + '-hourly')
    #
    #     # daily
    #     df_daily = self.output_daily(df_hourly=df_hourly)
    #     # monthly
    #     df_monthly = self.output_monthly(
    #         df_hourly=df_hourly, df_daily=df_daily)
    #     # output files
    #
    #     # daily
    #     daily_file_name = os.path.join(
    #         RESULT_DIR, file_name[:-4] + '-daily')
    #
    #     # monthly
    #     monthly_file_name = os.path.join(
    #         RESULT_DIR, file_name[:-4] + '-monthly')
    #
    #     # csv
    #     if self.type_of_output == 'CSV':
    #         df_hourly.to_csv(hourly_file_name + '.csv')
    #         df_daily.to_csv(daily_file_name + '.csv')
    #         df_monthly.to_csv(monthly_file_name + '.csv')
    #
    #     # json
    #     if self.type_of_output == 'JSON':
    #         df_hourly.to_json(hourly_file_name + '.json')
    #         df_daily.to_json(daily_file_name + '.json')
    #         df_monthly.to_json(monthly_file_name + '.json')
