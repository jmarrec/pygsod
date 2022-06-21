from gsodpy.epw_converter import clean_df, epw_convert
from gsodpy.noaadata import NOAAData
from gsodpy.utils import DataType
from gsodpy.ish_full import parse_ish_file
from gsodpy.tmy_download import TMY
from pathlib import Path

from gsodpy.constants import WEATHER_DIR, RESULT_DIR
import os
import pandas as pd
from pyepw.epw import EPW
import datetime


class GetOneStation(object):
    """docstring for GetOneStation."""

    def __init__(self, args):

        self.type_of_file = args['type_of_file']
        self.type_output = args['type_of_output']
        self.hdd_threshold = args['hdd_threshold']
        self.cdd_threshold = args['cdd_threshold']

        self.country = args['country']
        self.state = args['state']
        self.station_name = args['station_name']

        self.latitude = args['latitude']
        self.longitude = args['longitude']

        self.start_year = args['start_year']
        self.end_year = args['end_year']

    def run(self):
        self.list_files = self.get_data()

        # output files
        for file in self.list_files:
            self.o = Output(file, self.type_output, self.hdd_threshold,
                       self.cdd_threshold)
            self.o.output_files()

    def get_one_dataframe(self):

        self.list_files = self.get_data()

        df_hourly = pd.DataFrame()
        df_daily = pd.DataFrame()
        df_monthly = pd.DataFrame()
        for file in self.list_files:
            o = Output(file, self.type_output, self.hdd_threshold,
                       self.cdd_threshold)
            df_h, df_d, df_m = o.create_dataframe()

            df_hourly = pd.concat([df_hourly, df_h])
            df_daily = pd.concat([df_daily, df_d])
            df_monthly = pd.concat([df_monthly, df_m])

        self.df_hourly = df_hourly
        self.df_daily = df_daily
        self.df_monthly = df_monthly

    def get_data(self):
        if self.type_of_file == 'historical':
            # download isd_full
            list_files = self.download_historical_data()

        elif self.type_of_file == 'TMY':
            # download weather data from EP+ website
            tmy_data = TMY(self.country, self.station_name, self.state)
            self.tmy =tmy_data
            list_files = [tmy_data.fname]

        else:
            raise ValueError("The type of file is not correct, it should be"
                             " TMY or historical not {}."
                             .format(self.type_of_file))

        return list_files

    def download_historical_data(self):

        self.isd_full = NOAAData(data_type=DataType.isd_full)
        self.isd_full.set_years_range(
            start_year=self.start_year, end_year=self.end_year)

        self.isd_full.get_stations_from_user_input(
            self.country, self.state, self.station_name,
            self.latitude, self.longitude, self.end_year)

        self.isd_full.get_all_data()
        parse_ish_file(self.isd_full)

        list_ops_files = self.isd_full.ops_files

        return list_ops_files


class Output(object):
    """Class for output weather data into specific type """

    def __init__(self, file, type_of_output, hdd_threshold, cdd_threshold):

        self.file = Path(file)
        self.op_file_name = self.file.name
        print(self.op_file_name)

        self._file_names()

        self.type_of_output = type_of_output
        self.hdd_threshold = hdd_threshold
        self.cdd_threshold = cdd_threshold

    def calculate_hdd(self, temp):
        """function to calculate hdd"""
        if temp <= self.hdd_threshold:
            return self.hdd_threshold - temp
        else:
            return 0

    def calculate_cdd(self, temp):
        """function to calculate cdd"""
        if temp >= self.cdd_threshold:
            return temp - self.cdd_threshold
        else:
            return 0

    def get_hourly_data(self):
        full_path = self.file
        df_path = '{f}.{ext}'.format(f=full_path, ext='xlsx')
        df = pd.read_excel(df_path, index_col=0)
        df = clean_df(df, self.op_file_name)

        return df

    def _file_names(self):
        self.hourly_file_name = os.path.join(
            RESULT_DIR, self.op_file_name + '-hourly')

        # daily
        self.daily_file_name = os.path.join(
            RESULT_DIR, self.op_file_name + '-daily')

        # monthly
        self.monthly_file_name = os.path.join(
            RESULT_DIR, self.op_file_name + '-monthly')

    def output_daily(self, df_hourly):
        """output daily data by grouping by daily
           used in output_files()
        """
        df_daily = df_hourly.groupby(by=df_hourly.index.date).mean()
        df_daily.index = pd.to_datetime(
            df_daily.index)  # reset index to datetime

        # remove unnecessary columns for daily
        for col in ['AZIMUTH_ANGLE', 'ZENITH_ANGLE', 'WIND_DIRECTION']:
            if col in df_daily.columns:
                df_daily.drop(
                    columns=[col], inplace=True)

        df_daily['HDD_F'] = df_daily['TEMP_F'].apply(self.calculate_hdd)
        df_daily['CDD_F'] = df_daily['TEMP_F'].apply(self.calculate_cdd)

        return df_daily

    def output_monthly(self, df_hourly, df_daily):
        """output monthly data
           used in output_files()
        """
        df_monthly = df_hourly.groupby(pd.Grouper(freq='1M')).mean()

        # remove unnecessary columns for daily
        for col in ['AZIMUTH_ANGLE', 'ZENITH_ANGLE', 'WIND_DIRECTION']:
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

        df_hdd_cdd = df_daily.groupby(pd.Grouper(freq='1M')).sum()[[
            'HDD_F', 'CDD_F']]
        df_monthly = df_hdd_cdd.merge(df_monthly, how='left',
                                      right_index=True, left_index=True)

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
        df.to_csv(self.hourly_file_name + '.csv')
        self.df_hourly = df

        # epw
        if self.type_of_output == 'EPW':
            epw_convert(df, self.op_file_name)

        else:
            # daily
            df_daily = self.output_daily(df_hourly=df)
            self.df_daily = df_daily
            # monthly
            df_monthly = self.output_monthly(
                df_hourly=df, df_daily=df_daily)
            self.df_monthly = df_monthly

            # csv
            if self.type_of_output == 'CSV':
                df_daily.to_csv(self.daily_file_name + '.csv')
                df_monthly.to_csv(self.monthly_file_name + '.csv')

            # json
            if self.type_of_output == 'JSON':
                df_daily.to_json(self.daily_file_name + '.json')
                df_monthly.to_json(self.monthly_file_name + '.json')

    def create_dataframe(self):
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
