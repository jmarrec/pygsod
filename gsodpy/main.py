from gsodpy.epw_converter import epw_convert
from gsodpy.noaadata import NOAAData
from gsodpy.utils import DataType, clean_df
from gsodpy.ish_full import parse_ish_file
from gsodpy.tmy_download import TMY

from gsodpy.constants import WEATHER_DIR, RESULT_DIR
import os
import pandas as pd
from pyepw.epw import EPW
import datetime

import psycopg2

class Station(object):
    """docstring for GetOneStation."""

    def __init__(self, type_of_file, type_output=None,
                 country=None, state=None, station_name=None, usaf=None,
                 wban=None, latitude=None, longitude=None, start_year=None,
                 end_year=None, db_login=None,
                 hdd_threshold=None, cdd_threshold=None):

        self.type_of_file = type_of_file
        self.type_output = type_output

        if hdd_threshold is None:
            self.hdd_threshold = 65
        else:
            self.hdd_threshold = hdd_threshold

        if cdd_threshold is None:
            self.cdd_threshold = 65
        else:
            self.cdd_threshold = cdd_threshold

        self.country = country
        self.state = state
        self.station_name = station_name

        self.usaf = usaf
        self.wban = wban

        self.latitude = latitude
        self.longitude = longitude

        if self.type_of_file == 'TMY':
            self.start_year = 2019
            self.end_year = 2019

        else:
            if (start_year is None) or (end_year is None):
                raise ValueError(
                    "You must pass start_year and end_year, "
                    "for historical data")
            self.start_year = start_year
            self.end_year = end_year

        if db_login is not None:
            self.db = Database(
                db_login=db_login,
                type_of_file=self.type_of_file,
                country=self.country, state=self.state,
                station_name=self.station_name,
                usaf=self.usaf, wban=self.wban,
                latitude=self.latitude, longitude=self.longitude,
                start_year=self.start_year, end_year=self.end_year,
                hdd_threshold=self.hdd_threshold,
                cdd_threshold=self.cdd_threshold)
        else:
            self.db = None

        if self.latitude != None:
            # Function to get country, state, station_name
            pass

    def run(self):

        if self.db != None:

            if self.type_of_file == 'historical':
                years_to_update = self.db.get_years_to_update()
                self.list_files = []
                for year in years_to_update:
                    df_new_hourly, list_files = self.historical_data(
                        start_year=year, end_year=year,
                        create_excel_file=False)
                    self.db.update_missing_years(df_new_hourly)

                    self.list_files.extend(list_files)

            elif self.type_of_file == 'TMY':
                # +++++++++++++++++++++++++
                # TO DO
                raise ValueError("Not done yet for TMY and DB.")

            else:
                raise ValueError("The type of file is not correct, it should be"
                                 " TMY or historical not {}."
                                 .format(self.type_of_file))

            # +++++++++++++++++++++++++++++++++++++++++++++++++++++
            # we could use calculation below instead of the DB
            self.df_hourly = self.db.get_hourly_data()
            self.df_daily, self.df_monthly = self.db.get_daily_monthly_data()

        else:
            self.df_hourly, self.list_files = self.get_data()

            self.df_daily = self.compute_daily(df_hourly=self.df_hourly)
            self.df_monthly = self.compute_monthly(df_hourly=self.df_hourly,
                                                  df_daily=self.df_daily)

        # output files
        for file in self.list_files:
            o = Output(file, self.type_output,
                       self.df_hourly, self.df_daily, self.df_monthly)
            o.to_file()

    def get_data(self):
        if self.type_of_file == 'historical':
            # download isd_full
            df, list_files = self.historical_data(
                self.start_year, self.end_year, create_excel_file=True
            )

        elif self.type_of_file == 'TMY':
            # +++++++++++++++++++++++++
            # TO REVIEW
            df = None
            # download weather data from EP+ website
            tmy_data = TMY(self.country, self.state, self.station_name)
            list_files = [tmy_data.fname]

        else:
            raise ValueError("The type of file is not correct, it should be"
                             " TMY or historical not {}."
                             .format(self.type_of_file))

        return df, list_files

    def historical_data(self, start_year, end_year, create_excel_file):
        isd_full = NOAAData(data_type=DataType.isd_full)
        isd_full.set_years_range(
            start_year=start_year, end_year=end_year)

        # ++++++++++++++++++++++++++++++++++
        # need to update the part with latitude longitude
        isd_full.get_stations_from_user_input(
            usaf=self.usaf, wban=self.wban,
            country=self.country, state=self.state,
            station_name=self.station_name,
            latitude=self.latitude, longitude=self.longitude)

        isd_full.get_all_data()

        df = parse_ish_file(isd_full, create_excel_file=create_excel_file)
        df = clean_df(df)
        df.index = pd.to_datetime(df.index)

        list_ops_files = isd_full.ops_files

        return df, list_ops_files

    def compute_daily(self, df_hourly):
        """output daily data by grouping by daily
           used in output_files()
        """
        df_daily = df_hourly.groupby(pd.Grouper(freq='1D')).mean()

        # remove unnecessary columns for daily
        for col in ['AZIMUTH_ANGLE', 'ZENITH_ANGLE', 'WIND_DIRECTION']:
            if col in df_daily.columns:
                df_daily.drop(
                    columns=[col], inplace=True)

        df_daily['hdd_65_f'] = df_daily['TEMP_F'].apply(
            self.calculate_hdd, args=(65,))
        df_daily['cdd_65_f'] = df_daily['TEMP_F'].apply(
            self.calculate_cdd, args=(65,))

        return df_daily

    def compute_monthly(self, df_hourly, df_daily):
        """output monthly data
           used in output_files()
        """
        df_monthly = df_hourly.groupby(pd.Grouper(freq='1M')).mean()

        # remove unnecessary columns for daily
        for col in ['AZIMUTH_ANGLE', 'ZENITH_ANGLE', 'WIND_DIRECTION']:
            if col in df_monthly.columns:
                df_monthly.drop(columns=[col], inplace=True)

        df_hdd_cdd = df_daily.groupby(pd.Grouper(freq='1M')).sum()[[
            'hdd_65_f', 'cdd_65_f']]
        df_monthly = df_hdd_cdd.merge(df_monthly, how='left',
                                      right_index=True, left_index=True)

        return df_monthly

    def calculate_hdd(self, temp, threshold):
        """function to calculate hdd"""
        if temp <= threshold:
            return threshold - temp
        else:
            return 0

    def calculate_cdd(self, temp, threshold):
        """function to calculate cdd"""
        if temp >= threshold:
            return temp - threshold
        else:
            return 0


class Database(object):
    """docstring for Database."""

    def __init__(self, db_login, type_of_file,
                 hdd_threshold, cdd_threshold,
                 start_year, end_year,
                 country=None, state=None, station_name=None,
                 usaf=None, wban=None,
                 latitude=None, longitude=None):

        self.db_login = db_login
        self.db_params()

        self.type_of_file = type_of_file

        self.hdd_threshold = hdd_threshold
        self.cdd_threshold = cdd_threshold

        self.country = country
        self.state = state
        self.station_name = station_name

        self.usaf = usaf
        self.wban = wban

        self.latitude = latitude
        self.longitude = longitude

        self.start_year = start_year
        self.end_year = end_year

        self.get_weather_station_id()

    def db_params(self):
        with open(self.db_login, 'r') as file:
            HOST, DATABASE, USER, PASSWORD, _ = file.read().split('\n')

        self.connection_args = {
            'host': HOST,
            'database': DATABASE,
            'user': USER,
            'password': PASSWORD,
        }
        self.schema = "altanova-data-warehouse"

    def get_weather_station_id(self):

        if (self.usaf is not None) and (self.wban is not None):

            sql = f'''
            select id
            from "altanova-data-warehouse".weather_station
            where (
            usaf='{self.usaf}'
            and wban='{self.wban}')
            '''

        elif (self.country is not None) and (self.station_name is not None):

            if state is None:
                sql = f'''
                select id
                from "altanova-data-warehouse".weather_station
                where (
                country='{self.country}'
                and name='{self.station_name}'
                and state IS NULL)
                '''
            else:
                sql = f'''
                select id
                from "altanova-data-warehouse".weather_station
                where (
                country='{self.country}'
                and name='{self.station_name}'
                and state='{self.state}')
                '''

        elif (self.latitude is not None) and (self.longitude is not None):
            raise ValueError(
                "latitude and longitude are not done yet when using the DB.")

        else:
            raise ValueError(
                "You must provide usaf AND wban, "
                "OR country AND station_name, "
                "OR latitude AND longitude.")

        r, columns = self.select_all(sql)

        if len(r) == 0:
            self._add_weather_station()

            r, columns = self.select_all(sql)
            if len(r) == 1:
                self.weather_station_id = r[0][0]
            else:
                raise ValueError("Problem to insert station name.")

        elif len(r) == 1:
            self.weather_station_id = r[0][0]

        else:
            raise ValueError("Multiple weather stations with the same name.")

    def _add_weather_station(self):
        # +++++++++++++++++++++++++++++++++
        # TO IMPROVE
        sql = f'''
        insert into "{self.schema}".weather_station(
        name, country, state, usaf, wban, latitude, longitude)
        values ('{self.name}', '{self.country}', '{self.state}',
        '{self.usaf}', '{self.wban}', '{self.latitude}', '{self.longitude}'
        )
        '''
        self.insert_one(sql)

    def get_years_to_update(self):
        years_to_update = []

        df = self.get_index_hourly_data()

        if len(df) == 0:
            years_to_update.extend(
                        list(range(self.start_year, self.end_year+1)))
        else:

            today = datetime.date.today()
            if today.year == self.end_year:
                if df.index[-1] < today - datetime.timedelta(days=1):
                    years_to_update.append(self.end_year)

            # we consider only full years
            min_year = min(list(df.index.year.unique()))

            if self.start_year < min_year:
                years_to_update.extend(list(range(self.start_year, min_year)))

        self.years_to_update = years_to_update
        return years_to_update

    def get_hourly_data(self):
        start_date = f"{self.start_year}-01-01 00:00:00"
        end_date = f"{self.end_year}-12-31 23:00:00"
        sql = f'''
        select *
        from "altanova-data-warehouse".weather_data
        where (
        frequency='hourly'
        and weather_station_id={self.weather_station_id}
        and time>='{start_date}'
        and time<='{end_date}'
        )
        '''
        r, columns = self.select_all(sql)
        df = pd.DataFrame(r, columns=columns)
        df.set_index('time', inplace=True)
        df.sort_index(inplace=True)
        return df

    def get_index_hourly_data(self):
        start_date = f"{self.start_year}-01-01 00:00:00"
        end_date = f"{self.end_year}-12-31 23:00:00"
        sql = f'''
        select time
        from "altanova-data-warehouse".weather_data
        where (
        frequency='hourly'
        and weather_station_id={self.weather_station_id}
        and time>='{start_date}'
        and time<='{end_date}'
        )
        '''
        r, columns = self.select_all(sql)
        df = pd.DataFrame(r, columns=columns)
        df.set_index('time', inplace=True)
        return df

    def update_missing_years(self, df_new_hourly):

        df_hourly = self.get_hourly_data()
        df_new_hourly = df_new_hourly.loc[
            set(df_new_hourly.index) - set(df_hourly.index)]

        self.insert_hourly_weather_data(df_new_hourly)

    def insert_hourly_weather_data(self, df):
        sql = f'''
        insert into "{self.schema}".weather_data(
        weather_station_id,
        frequency,
        time)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        names = [
            ('DEWP_C', 'dewp_c'),
            ('TEMP_C', 'air_temperature_c'),
            ('TEMP_F', 'air_temperature_f'),
            ('RELATIVE_HUMIDITY_PERCENTAGE', 'relative_humidity'),
            ('WIND_DIRECTION', 'wind_direction'),
            ('WIND_SPEED', 'wind_speed'),
            ('OPAQUE_SKY_COVER', 'opaque_sky_cover'),
            ('TOTAL_SKY_COVER', 'total_sky_cover'),
            ('SLP_Pa', 'sea_level_pressure_hpa'),
            ('AZIMUTH_ANGLE', 'azimuth_angle'),
            ('ZENITH_ANGLE', 'zenith_ange')
        ]

        sql_list = []
        for datetime, row in df.iterrows():
            sql = f'''
            insert into "{self.schema}".weather_data(
            weather_station_id,
            frequency,
            time
            '''
            sql_values = f"""
            values ('{self.weather_station_id}', 'hourly', '{datetime}'
            """

            for name_df, name_sql in names:
                value = row[name_df]
                if not pd.isna(value):
                    sql += f''', {name_sql}'''
                    sql_values += f""", '{value}'"""

            sql_values += ''')'''
            sql += f'''
            )
            {sql_values}
            '''
            sql_list.append(sql)

        self.insert_many_loop(sql_list)

    def get_daily_monthly_data(self):

        sql_daily_hdd_cdd = f"""
        with avg_table as (
        	SELECT
        	date_trunc('day', time) as gb_day,
        	AVG(air_temperature_c) as avg_temp_c,
        	AVG(air_temperature_f) as avg_temp_f,
        	AVG(dewp_c) as avg_dewp_c
        	FROM "altanova-data-warehouse".weather_data
        	WHERE (frequency = 'hourly'
                  and weather_station_id={self.weather_station_id})
        	GROUP BY gb_day
        	ORDER BY gb_day
        ),

        daily_table as (
        select
        gb_day,
        avg_temp_c as air_temperature_c,
        avg_temp_f as air_temperature_f,
        avg_dewp_c as dewp_c,
        case
        	when avg_temp_f - {self.cdd_threshold} > 0
            then avg_temp_f - {self.cdd_threshold}
            else 0
        end as cdd,
        case
        	when {self.hdd_threshold} - avg_temp_f > 0
            then {self.hdd_threshold} - avg_temp_f
            else 0
        end as hdd
        from avg_table
        )
        """

        sql_daily_table = sql_daily_hdd_cdd + """
        select gb_day as datetime,
        ROUND(cdd, 2) as cdd_f,
        ROUND(hdd, 2) as hdd_f,
        ROUND(air_temperature_c, 2) as air_temperature_c,
        ROUND(air_temperature_f, 2) as air_temperature_f,
        ROUND(dewp_c, 2) as dewp_c
        from daily_table
        """

        sql_monthly_table = sql_daily_hdd_cdd + f"""
        ,
        monthly_table as (
        	SELECT
        	date_trunc('month', gb_day) as gb_month,
        	SUM(cdd) as cdd,
        	SUM(hdd) as hdd,
        	AVG(air_temperature_c) as air_temperature_c,
        	AVG(air_temperature_f) as air_temperature_f,
        	AVG(dewp_c) as dewp_c
        	FROM daily_table
        	GROUP BY gb_month
        	ORDER BY gb_month

        )
        select gb_month as datetime, ROUND(cdd, 2) as cdd_f,
        ROUND(hdd, 2) as hdd_f,
        ROUND(air_temperature_c, 2) as air_temperature_c,
        ROUND(air_temperature_f, 2) as air_temperature_f,
        ROUND(dewp_c, 2) as dewp_c
        from monthly_table
        """

        r, columns = self.select_all(sql_daily_table)
        df_daily = pd.DataFrame(r, columns=columns)
        df_daily.set_index('datetime', inplace=True)

        r, columns = self.select_all(sql_monthly_table)
        df_monthly = pd.DataFrame(r, columns=columns)
        df_monthly.set_index('datetime', inplace=True)

        return df_daily, df_monthly

    def select_all(self, sql_command):

        conn = psycopg2.connect(**self.connection_args)

        cur = conn.cursor()
        cur.execute(sql_command)
        r = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]

        conn.commit()
        conn.close()
        return r, col_names

    def insert_one(self, sql_command):

        conn = psycopg2.connect(**self.connection_args)

        cur = conn.cursor()
        cur.execute(sql_command)

        conn.commit()
        conn.close()

    def insert_many(self, sql_command, list_data):

        conn = psycopg2.connect(**self.connection_args)

        cur = conn.cursor()
        cur.executemany(sql_command, list_data)

        conn.commit()
        conn.close()

    def insert_many_loop(self, sql_list):

        conn = psycopg2.connect(**self.connection_args)

        cur = conn.cursor()
        for sql in sql_list:
            cur.execute(sql)

        conn.commit()
        conn.close()


class Output(object):
    """Class for output weather data into specific type """

    def __init__(self, file, type_output, df_hourly, df_daily, df_monthly):

        self.file = file
        self.op_file_name = self.file.split('/')[-1]

        self._file_names()

        self.type_output = type_output
        self.df_hourly = df_hourly
        self.df_daily = df_daily
        self.df_monthly = df_monthly

    def _file_names(self):
        self.hourly_file_name = os.path.join(
            RESULT_DIR, self.op_file_name + '-hourly')

        # daily
        self.daily_file_name = os.path.join(
            RESULT_DIR, self.op_file_name + '-daily')

        # monthly
        self.monthly_file_name = os.path.join(
            RESULT_DIR, self.op_file_name + '-monthly')

    def to_file(self):
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

        # df = self.get_hourly_data()
        # df.to_csv(self.hourly_file_name + '.csv')

        # epw
        if self.type_output == 'EPW':
            epw_convert(self.df_hourly, self.op_file_name)

        # else:
        # daily
        # df_daily = self.output_daily(df_hourly=df)
        # # monthly
        # df_monthly = self.output_monthly(
        #     df_hourly=df, df_daily=df_daily)

        # csv
        elif self.type_output == 'CSV':
            self.df_hourly.to_csv(self.hourly_file_name + '.csv')
            self.df_daily.to_csv(self.daily_file_name + '.csv')
            self.df_monthly.to_csv(self.monthly_file_name + '.csv')

        # json
        elif self.type_output == 'JSON':
            self.df_hourly.to_json(self.hourly_file_name + '.json')
            self.df_daily.to_json(self.daily_file_name + '.json')
            self.df_monthly.to_json(self.monthly_file_name + '.json')

# def get_hourly_data(self):
#     full_path = self.file
#     df_path = '{f}.{ext}'.format(f=full_path, ext='xlsx')
#     df = pd.read_excel(df_path, index_col=0)
#     df = clean_df(df)
#
#     return df

# def get_one_dataframe(self):
#
#     self.list_files = self.get_data()
#
#     df_hourly = pd.DataFrame()
#     df_daily = pd.DataFrame()
#     df_monthly = pd.DataFrame()
#     for file in self.list_files:
#         o = Output(file, self.type_output, self.hdd_threshold,
#                    self.cdd_threshold)
#         df_h, df_d, df_m = o.create_dataframe()
#
#         df_hourly = pd.concat([df_hourly, df_h])
#         df_daily = pd.concat([df_daily, df_d])
#         df_monthly = pd.concat([df_monthly, df_m])
#
#     self.df_hourly = df_hourly
#     self.df_daily = df_daily
#     self.df_monthly = df_monthly

# def download_historical_data(self):
#
#     isd_full = NOAAData(data_type=DataType.isd_full)
#     isd_full.set_years_range(
#         start_year=self.start_year, end_year=self.end_year)
#
#     isd_full.get_stations_from_user_input(
#         self.country, self.state, self.station_name,
#         self.latitude, self.longitude)
#
#     isd_full.get_all_data()
#
#     parse_ish_file(isd_full, create_excel_file=True)
#
#     list_ops_files = isd_full.ops_files
#
#     return list_ops_files


# def calculate_hdd(self, temp, threshold):
#     """function to calculate hdd"""
#     if temp <= threshold:
#         return threshold - temp
#     else:
#         return 0
# def calculate_cdd(self, temp, threshold):
#     """function to calculate cdd"""
#     if temp >= threshold:
#         return temp - threshold
#     else:
#         return 0
#
# def output_daily(self, df_hourly):
#     """output daily data by grouping by daily
#        used in output_files()
#     """
#     df_daily = df_hourly.groupby(by=df_hourly.index.date).mean()
#     df_daily.index = pd.to_datetime(
#         df_daily.index)  # reset index to datetime
#
#     # remove unnecessary columns for daily
#     for col in ['AZIMUTH_ANGLE', 'ZENITH_ANGLE', 'WIND_DIRECTION']:
#         if col in df_daily.columns:
#             df_daily.drop(
#                 columns=[col], inplace=True)
#
#     df_daily['HDD_F'] = df_daily['TEMP_F'].apply(self.calculate_hdd)
#     df_daily['CDD_F'] = df_daily['TEMP_F'].apply(self.calculate_cdd)
#
#     return df_daily
#
# def output_monthly(self, df_hourly, df_daily):
#     """output monthly data
#        used in output_files()
#     """
#     df_monthly = df_hourly.groupby(pd.Grouper(freq='1M')).mean()
#
#     # remove unnecessary columns for daily
#     for col in ['AZIMUTH_ANGLE', 'ZENITH_ANGLE', 'WIND_DIRECTION']:
#         if col in df_monthly.columns:
#             df_monthly.drop(columns=[col], inplace=True)
#
#     # monthly_hdd = []
#     # monthly_cdd = []
#     # for month in range(1, df_hourly.index.month[-1] + 1):
#     #     monthly_hdd.append(
#     #         df_daily[df_daily.index.month == month]['HDD_F'].sum())
#     #     monthly_cdd.append(
#     #         df_daily[df_daily.index.month == month]['CDD_F'].sum())
#     # df_monthly['HDD_F'] = monthly_hdd
#     # df_monthly['CDD_F'] = monthly_cdd
#
#     df_hdd_cdd = df_daily.groupby(pd.Grouper(freq='1M')).sum()[[
#         'HDD_F', 'CDD_F']]
#     df_monthly = df_hdd_cdd.merge(df_monthly, how='left',
#                                   right_index=True, left_index=True)
#
#     return df_monthly

# def insert_update_weather_data(self, df, df_inplace, frequency):
#     sql_insert = f'''
#     insert into "{self.schema}".weather_data(weather_station_id,
#     frequency, time,
#     dewp_c, wind_speed, air_temperature_f,
#     total_sky_cover, hdd_65_f, cdd_65_f)
#     values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#     '''
#
#     sql_update = f'''
#     update "{self.schema}".weather_data
#     set dewp_c=%s, wind_speed=%s, air_temperature_f=%s, total_sky_cover=%s, hdd_65_f=%s, cdd_65_F=%s
#     where weather_station_id=%s and frequency=%s and time=%s
#     '''
#     data_update = []
#     data_insert = []
#
#     index_inplace = df_inplace.index.unique()
#
#     for datetime, row in df.iterrows():
#         if datetime in index_inplace:
#             t = (
#
#                 row['DEWP_C'],
#                 row['WIND_SPEED'],
#                 row['TEMP_F'],
#                 row['TOTAL_SKY_COVER'],
#                 row['hdd_65_f'],
#                 row['cdd_65_f'],
#                 self.weather_station_id,
#                 frequency,
#                 datetime,
#             )
#             data_update.append(t)
#         else:
#             t = (
#                 self.weather_station_id,
#                 frequency,
#                 datetime,
#                 row['DEWP_C'],
#                 row['WIND_SPEED'],
#                 row['TEMP_F'],
#                 row['TOTAL_SKY_COVER'],
#                 row['hdd_65_f'],
#                 row['cdd_65_f'],
#             )
#             data_insert.append(t)
#
#     print(len(data_insert))
#     print(len(data_update))
#     self.insert_many(sql_insert, data_insert)
#     self.insert_many(sql_update, data_update)

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
#     if self.type_output == 'CSV':
#         df_hourly.to_csv(hourly_file_name + '.csv')
#         df_daily.to_csv(daily_file_name + '.csv')
#         df_monthly.to_csv(monthly_file_name + '.csv')
#
#     # json
#     if self.type_output == 'JSON':
#         df_hourly.to_json(hourly_file_name + '.json')
#         df_daily.to_json(daily_file_name + '.json')
#         df_monthly.to_json(monthly_file_name + '.json')
