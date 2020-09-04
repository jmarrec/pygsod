# from pkg_resources import resource_filename
import os
import pandas as pd
import requests
import datetime
from bs4 import BeautifulSoup
from pyepw.epw import EPW
from gsodpy.constants import RESULT_DIR


class TMY(object):
    """Provide temperature data for selected location."""

    def __init__(self, country, state, temperature_file):
        # self.building_args = building_args
        # self.country = building_args['location']['country']
        # self.state = building_args['location']['state']
        # self.temperature_file = building_args[
        #     'location']['temperature location']

        self.country = country
        self.state = state
        self.temperature_file = temperature_file

        file_exist, fname = self.check_exist()
        if file_exist:
            print("File already exist!")
            self._file_name(fname)

        else:
            # print('Download weather file from EP+ website.')
            self.url_epw = self.fetch_url()

            fname = self.url_epw.split('/')[-1]
            self._file_name(fname)

            self.download_data()

        # # self.url_epw = self.fetch_url() #
        # self.url_epw = self.fetch_url_2(temperature_file)
        # fname = self.url_epw.split('/')[-1]
        # self._file_name(fname)

        self.download_data()

        self.create_dataframe()

    def _file_name(self, fname):
        fname = os.path.splitext(fname)[0]
        self.fname = os.path.join(RESULT_DIR, fname)

        self.fname_epw = self.fname + '.epw'
        self.fname_xlsx = self.fname + '.xlsx'

    def fetch_url_2(self, site):
        url = 'https://energyplus.net/weather-search/'

        result = requests.get(url + site).content
        soup = BeautifulSoup(result, "html.parser")

        url_city = ''
        for i in soup.find_all(
                "a", {"class": "btn btn-default left-justify blue-btn"}):
            text = i.text.lower()
            if (site in text) and (('iwec' in text) or ('tmy'in text) or ('rmy' in text)):
                # url_city = 'https://energyplus.net/{}'.format(i['href'])
                print(i['href'])
                print(i['href'].split('/'))
                b, w, r, c, n = i['href'].split('/')
                url_city = 'https://energyplus.net/{}/{}/{}//{}'.format(
                    w, r, c, n)

        if len(url_city) == 0:
            print("This site is not working: {}".format(site))

        print('city', url_city)
        soup = BeautifulSoup(requests.get(url_city).content, "html.parser")
        for i in soup.find_all('a'):
            if 'epw' in i.text:
                url_epw = 'https://energyplus.net/{}'.format(i['href'])

        print('epw', url_epw)

        return url_city

    def fetch_url(self):
        """
        Fetch url for the energy plus TMY3 weather data.

        It is based on location provided.
        """
        prefix = 'https://energyplus.net/weather-region'
        country = self.country
        state = self.state
        temperature_file = self.temperature_file

        # retrieve region
        if country in ['CAN', 'USA', 'BLZ', 'CUB', 'GTM', 'HND', 'MEX', 'MTQ',
                       'NIC', 'PRI', 'SLV', 'VIR']:
            region = 'north_and_central_america_wmo_region_4'
        elif country in ['AUT', 'BEL', 'BGR', 'BIH', 'BLR', 'CHE', 'CYP',
                         'CZE', 'DEU', 'DNK', 'ESP', 'FIN', 'FRA', 'GBR',
                         'GRC', 'HUN', 'IRL', 'ISL', 'ISR', 'ITA', 'LTU',
                         'NLD', 'NOR', 'POL', 'PRT', 'ROU', 'RUS', 'SRB',
                         'SVK', 'SVN', 'SWE', 'SYR', 'TUR', 'UKR']:
            region = 'europe_wmo_region_6'
        elif country in ['AUS', 'BRN', 'FJI', 'GUM', 'MHL', 'MYS', 'NZL',
                         'PHL', 'PLW', 'SGP', 'UMI']:
            region = 'southwest_pacific_wmo_region_5'
        elif country in ['ARG', 'BOL', 'BRA', 'CHL', 'COL', 'ECU', 'PER',
                         'PRY', 'URY', 'VEN']:
            region = 'south_america_wmo_region_3'
        elif country in ['ARE', 'BGD', 'CHN', 'IND', 'IRN', 'JPN', 'KAZ',
                         'KOR', 'KWT', 'LKA', 'MAC', 'MDV', 'MNG', 'NPL',
                         'PAK', 'PRK', 'RUS', 'SAU', 'THA', 'TWN', 'UZB',
                         'VNM']:
            region = 'asia_wmo_region_2'
        elif country in ['DZA', 'EGY', 'ETH', 'GHA', 'KEN', 'LBY', 'MAR',
                         'MDG', 'SEN', 'TUN', 'ZAF', 'ZWE']:
            region = 'africa_wmo_region_1'

        elif country == 'Not In List':
            region = 'north_and_central_america_wmo_region_4'
            country = 'USA'
            # default USA
        else:
            raise ValueError('Your provided country is not applicable.')
        ###################################
        # add other region in the future
        ###################################
        # get all temperature files under the state

        if state == 'N/A' or pd.isna(state):
            state = ''
        elif state == 'Not In List':
            state = 'NY'

        # print(prefix, region, country, state)
        url_state = self.slash_join(prefix, region, country, state)
        url_city = self.get_url_city(
            region, country, url_state, temperature_file)
        # print('city', url_city)
        # fetch epw file
        soup = BeautifulSoup(requests.get(url_city).content, "html.parser")
        for i in soup.find_all('a'):
            if 'epw' in i.text:
                url_epw = 'https://energyplus.net/{}'.format(i['href'])

        # print('epw', url_epw)
        return url_epw

    def download_data(self):
        """Download epw weather file from website."""
        # If we don't have the file
        if not os.path.isfile(self.fname_epw):
            # fetch the zipfile
            data = requests.get(self.url_epw).content
            with open(self.fname_epw, 'wb') as f:
                print("Saving file to {}".format(self.fname_epw))
                f.write(data)

    def create_dataframe(self):
        epw = EPW()
        epw.read(self.fname_epw)

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
                                  2019, df_hourly['month'][0],
                                  df_hourly['day'][0], df_hourly['hour'][0] - 1),
                              end=datetime.datetime(
                                  2019,
                                  df_hourly['month'].iloc[-1],
                                  df_hourly['day'].iloc[-1],
                                  df_hourly['hour'].iloc[-1] - 1))
        df_hourly = df_hourly.set_index(index)
        df_hourly['TEMP_F'] = df_hourly['dry_bulb_temperature'] * 1.8 + 32

        df_hourly.to_excel(self.fname_xlsx)
        self.df_hourly = df_hourly

    def slash_join(self, *args):
        return "/".join(arg.strip("/") for arg in args)

    def get_url_city(self, region, country, url_state, temperature_file):
        url_city = None
        # print(url_state)
        result = requests.get(url_state).content
        soup = BeautifulSoup(result, "html.parser")
        for i in soup.find_all(
                "a", {"class": "btn btn-default left-justify blue-btn"}):
            # print(i.text)
            if temperature_file in i.text and 'TMY3' in i.text:
                url_city = 'https://energyplus.net/{}'.format(i['href'])

            elif temperature_file in i.text:
                url_city = 'https://energyplus.net/{}'.format(i['href'])

        if url_city is None:
            raise ValueError(
                "There is no temperature file which matches "
                "the location of the country ({}), the state ({}) and "
                "the city ({}) on energy plus."
                .format(self.country, self.state, self.temperature_file))
        else:
            return url_city

    def check_exist(self):

        file_exist = False
        fname = None

        f = RESULT_DIR
        for file_name in os.listdir(f):
            if file_name.endswith(".epw"):
                _temmp_file = self.temperature_file.replace(" ", ".")
                if self.state is None:
                    state = ''
                else:
                    state = self.state
                if ((self.country in file_name) and
                    (state in file_name) and
                        (_temmp_file in file_name)):

                    file_exist = True
                    fname = file_name
        return file_exist, fname
