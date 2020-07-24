from gsodpy.epw_converter import clean_df, epw_convert
from gsodpy.constants import WEATHER_DIR, RESULT_DIR
import os
import pandas as pd
from pyepw.epw import EPW
import datetime


class Output(object):
    """Class for output weather data into specific type """

    def __init__(self, file, type_of_output, hdd_threshold, cdd_threshold):

        self.file = file
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

    def output_daily(self, df_hourly):
        """output daily data by grouping by daily
           used in output_files()
        """
        df_daily = df_hourly.groupby(by=df_hourly.index.date).mean()
        df_daily.index = pd.to_datetime(
            df_daily.index)  # reset index to datetime
        # remove unnecessary columns for daily
        # df_daily.drop(
        # columns=['AZIMUTH_ANGLE', 'ZENITH_ANGLE', 'WIND_DIRECTION'],
        # inplace=True)
        for col in ['AZIMUTH_ANGLE', 'ZENITH_ANGLE', 'WIND_DIRECTION']:
            if col in df_daily.columns:
                df_daily.drop(
                    columns=[col], inplace=True)

        df_daily['HDD_F'] = df_daily[
            'TEMP_F'].apply(self.calculate_hdd)
        df_daily['CDD_F'] = df_daily[
            'TEMP_F'].apply(self.calculate_cdd)

        return df_daily

    def output_monthly(self, df_hourly, df_daily):
        """output monthly data
           used in output_files()
        """
        df_monthly = df_hourly.groupby(by=df_hourly.index.month).mean()
        # remove unnecessary columns for daily
        # df_monthly.drop(
        # columns=['AZIMUTH_ANGLE', 'ZENITH_ANGLE', 'WIND_DIRECTION'],
        # inplace=True)
        for col in ['AZIMUTH_ANGLE', 'ZENITH_ANGLE', 'WIND_DIRECTION']:
            if col in df_monthly.columns:
                df_monthly.drop(
                    columns=[col], inplace=True)

        monthly_hdd = []
        monthly_cdd = []
        for month in range(1, 13):
            monthly_hdd.append(
                df_daily[df_daily.index.month == month]['HDD_F'].sum())
            monthly_cdd.append(
                df_daily[df_daily.index.month == month]['CDD_F'].sum())
        df_monthly['HDD_F'] = monthly_hdd
        df_monthly['CDD_F'] = monthly_cdd

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
        full_path = self.file
        op_file_name = self.file.split('/')[-1]

        df_path = '{f}.{ext}'.format(f=full_path, ext='xlsx')
        df = pd.read_excel(df_path, index_col=0)
        df = clean_df(df, op_file_name)

        # hourly
        hourly_file_name = os.path.join(
            RESULT_DIR, op_file_name + '-hourly' + '.csv')
        df.to_csv(hourly_file_name)
        # df.to_json

        # daily
        df_daily = self.output_daily(df_hourly=df)
        # monthly
        df_monthly = self.output_monthly(
            df_hourly=df, df_daily=df_daily)
        # output files

        # daily
        daily_file_name = os.path.join(
            RESULT_DIR, op_file_name + '-daily')

        # monthly
        monthly_file_name = os.path.join(
            RESULT_DIR, op_file_name + '-monthly')

        # epw
        if self.type_of_output == 'EPW':
            epw_convert(df, op_file_name)

        # csv
        if self.type_of_output == 'CSV':
            df_daily.to_csv(daily_file_name + '.csv')
            df_monthly.to_csv(monthly_file_name + '.csv')

        # json
        if self.type_of_output == 'JSON':
            df_daily.to_json(daily_file_name + '.json')
            df_monthly.to_json(monthly_file_name + '.json')

    def output_files_from_epw(self):
        """output csv or json file from epw file downloaded from EP+ website.

           csv: hourly, daily, monthly

           json: daily, monthly

        """
        epw = EPW()
        epw.read(self.file)

        dic = {'year': [i.year for i in epw.weatherdata],
               'month': [i.month for i in epw.weatherdata],
               'day': [i.day for i in epw.weatherdata],
               'hour': [i.hour for i in epw.weatherdata],
               'minute': [i.minute for i in epw.weatherdata],
               'aerosol_optical_depth': [i.aerosol_optical_depth for i in epw.weatherdata],
               'albedo': [i.albedo for i in epw.weatherdata],
               'atmospheric_station_pressure': [i.atmospheric_station_pressure for i in epw.weatherdata],
               'ceiling_height': [i.ceiling_height for i in epw.weatherdata],
               'data_source_and_uncertainty_flags': [i.data_source_and_uncertainty_flags for i in epw.weatherdata],
               'days_since_last_snowfall': [i.days_since_last_snowfall for i in epw.weatherdata],
               'dew_point_temperature': [i.dew_point_temperature for i in epw.weatherdata],
               'diffuse_horizontal_illuminance': [i.diffuse_horizontal_illuminance for i in epw.weatherdata],
               'diffuse_horizontal_radiation': [i.diffuse_horizontal_radiation for i in epw.weatherdata],
               'direct_normal_illuminance': [i.direct_normal_illuminance for i in epw.weatherdata],
               'direct_normal_radiation': [i.direct_normal_radiation for i in epw.weatherdata],
               'dry_bulb_temperature': [i.dry_bulb_temperature for i in epw.weatherdata],
               'extraterrestrial_direct_normal_radiation': [i.extraterrestrial_direct_normal_radiation for i in epw.weatherdata],
               'extraterrestrial_horizontal_radiation': [i.extraterrestrial_horizontal_radiation for i in epw.weatherdata],
               'field_count': [i.field_count for i in epw.weatherdata],
               'global_horizontal_illuminance': [i.global_horizontal_illuminance for i in epw.weatherdata],
               'global_horizontal_radiation': [i.global_horizontal_radiation for i in epw.weatherdata],
               'horizontal_infrared_radiation_intensity': [i.horizontal_infrared_radiation_intensity for i in epw.weatherdata],
               'liquid_precipitation_depth': [i.liquid_precipitation_depth for i in epw.weatherdata],
               'liquid_precipitation_quantity': [i.liquid_precipitation_quantity for i in epw.weatherdata],
               'opaque_sky_cover': [i.opaque_sky_cover for i in epw.weatherdata],
               'precipitable_water': [i.precipitable_water for i in epw.weatherdata],
               'present_weather_codes': [i.present_weather_codes for i in epw.weatherdata],
               'present_weather_observation': [i.present_weather_observation for i in epw.weatherdata],
               'relative_humidity': [i.relative_humidity for i in epw.weatherdata],
               'snow_depth': [i.snow_depth for i in epw.weatherdata],
               'total_sky_cover': [i.total_sky_cover for i in epw.weatherdata],
               'visibility': [i.visibility for i in epw.weatherdata],
               'wind_direction': [i.wind_direction for i in epw.weatherdata],
               'wind_speed': [i.wind_speed for i in epw.weatherdata],
               'wind_speed': [i.wind_speed for i in epw.weatherdata],
               'zenith_luminance': [i.zenith_luminance for i in epw.weatherdata]
               }

        df_hourly = pd.DataFrame(dic)

        index = pd.date_range(freq='1H',
                              start=datetime.datetime(
                                  df_hourly['year'][0], df_hourly['month'][0],
                                  df_hourly['day'][0], df_hourly['hour'][0] - 1),
                              end=datetime.datetime(
                                  df_hourly['year'][0], df_hourly[
                                      'month'][8759],
                                  df_hourly['day'][8759], df_hourly['hour'][8759] - 1))
        df_hourly = df_hourly.set_index(index)
        df_hourly['TEMP_F'] = df_hourly['dry_bulb_temperature'] * 1.8 + 32

        # export df_hourly
        file_name = self.file.split('/')[-1]
        hourly_file_name = os.path.join(
            RESULT_DIR, file_name[:-4] + '-hourly')

        # daily
        df_daily = self.output_daily(df_hourly=df_hourly)
        # monthly
        df_monthly = self.output_monthly(
            df_hourly=df_hourly, df_daily=df_daily)
        # output files

        # daily
        daily_file_name = os.path.join(
            RESULT_DIR, file_name[:-4] + '-daily')

        # monthly
        monthly_file_name = os.path.join(
            RESULT_DIR, file_name[:-4] + '-monthly')

        # csv
        if self.type_of_output == 'CSV':
            df_hourly.to_csv(hourly_file_name + '.csv')
            df_daily.to_csv(daily_file_name + '.csv')
            df_monthly.to_csv(monthly_file_name + '.csv')
        # json
        if self.type_of_output == 'JSON':
            df_hourly.to_json(hourly_file_name + '.json')
            df_daily.to_json(daily_file_name + '.json')
            df_monthly.to_json(monthly_file_name + '.json')
