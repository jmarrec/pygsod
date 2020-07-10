from gsodpy.epw_converter import clean_df, epw_convert
from gsodpy.constants import WEATHER_DIR
import os
import pandas as pd


class Output(object):

    def __init__(self, args):

        self.type_of_output = args['type_of_output']
        self.hdd_threshold = args['hdd_threshold']
        self.cdd_threshold = args['cdd_threshold']

    def calculate_hdd(self, temp):
        if temp <= self.hdd_threshold:
            return self.hdd_threshold - temp
        else:
            return 0

    def calculate_cdd(self, temp):
        if temp >= self.cdd_threshold:
            return temp - self.cdd_threshold
        else:
            return 0

    def output_files(self):

        for root, dirs, files in os.walk(WEATHER_DIR + '/isd_full'):
            for file in files:
                if file.endswith("xlsx"):
                    df_path = os.path.join(root, file)
                    df = pd.read_excel(df_path, index_col=0)
                    df = clean_df(df, file)

                    # hourly
                    hourly_file_name = os.path.join(
                        root, file[:-5] + '-hourly' + '.csv')
                    df.to_csv(hourly_file_name)

                    # daily
                    df_daily = df.groupby(by=df.index.date).mean()
                    df_daily.index = pd.to_datetime(
                        df_daily.index)  # reset index to datetime
                    # remove unnecessary columns for daily
                    df_daily.drop(
                        columns=['AZIMUTH_ANGLE', 'ZENITH_ANGLE', 'WIND_DIRECTION'], inplace=True)

                    df_daily['HDD_F'] = df_daily[
                        'TEMP_F'].apply(self.calculate_hdd)
                    df_daily['CDD_F'] = df_daily[
                        'TEMP_F'].apply(self.calculate_cdd)

                    # monthly
                    df_monthly = df.groupby(by=df.index.month).mean()
                    # remove unnecessary columns
                    df_monthly.drop(
                        columns=['AZIMUTH_ANGLE', 'ZENITH_ANGLE', 'WIND_DIRECTION'], inplace=True)

                    monthly_hdd = []
                    monthly_cdd = []
                    for month in range(1, 13):
                        monthly_hdd.append(
                            df_daily[df_daily.index.month == month]['HDD_F'].sum())
                        monthly_cdd.append(
                            df_daily[df_daily.index.month == month]['CDD_F'].sum())
                    df_monthly['HDD_F'] = monthly_hdd
                    df_monthly['CDD_F'] = monthly_cdd

                    # output files

                    # daily
                    daily_file_name = os.path.join(
                        root, file[:-5] + '-daily')

                    # monthly
                    monthly_file_name = os.path.join(
                        root, file[:-5] + '-monthly')

                    # epw
                    if self.type_of_output == 'EPW':
                        epw_convert(df, root, file)

                    # csv
                    if self.type_of_output == 'CSV':
                        df_daily.to_csv(daily_file_name + '.csv')
                        df_monthly.to_csv(monthly_file_name + '.csv')

                    # json
                    if self.type_of_output == 'JSON':
                        df_daily.to_json(daily_file_name + '.json')
                        df_monthly.to_json(monthly_file_name + '.json')
