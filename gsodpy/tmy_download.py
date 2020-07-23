# from pkg_resources import resource_filename
import os
import requests
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
        # self.start_date = start_date

        exist_file_name = self.check_exist()
        if exist_file_name:
            print("file already exist!")
            self.filename = os.path.join(RESULT_DIR, exist_file_name)
            # self.filename = resource_filename(__name__,
            #                                   '/specifications/' +
            #                                   exist_file_name)

        else:
            print('Download weather file from EP+ website.')
            self.url_epw = self.fetch_url()
            self.filename = os.path.join(RESULT_DIR, self.url_epw.split('/')[-1])
            # self.filename = resource_filename(__name__,
            #                                   '/specifications/' +
            #                                   self.url_epw.split('/')[-1])
            self.download_data()

        # self.fetch_temperature_data(file_name=self.filename)

        # Convert the list to a Series
        # self.data = from_fixed_yearly_load_to_actual_load(
        #     self.start_date, self.data)

        # Convert to Fahrenheit
        # self.data = self.data * 1.8 + 32

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
        elif country == 'Not In List':
            region = 'north_and_central_america_wmo_region_4'
            country = 'USA'
            # default USA
        else:
            print('Your provided country is not applicable.')
        ###################################
        # add other region in the future
        ###################################
        # get all temperature files under the state

        if state == 'N/A':
            state = ''
        elif state == 'Not In List':
            state = 'NY'

        url_state = self.slash_join(prefix, region, country, state)
        url_city = self.get_url_city(
            region, country, url_state, temperature_file)

        # fetch epw file
        soup = BeautifulSoup(requests.get(url_city).content, "html.parser")
        for i in soup.find_all('a'):
            if 'epw' in i.text:
                url_epw = 'https://energyplus.net/{}'.format(i['href'])

        return url_epw

    def download_data(self):
        """Download epw weather file from website."""
        if os.path.isfile(self.filename):  # we already have this file
            pass

        else:
            # fetch the zipfile
            data = requests.get(self.url_epw).content
            with open(self.filename, 'wb') as f:
                print("Saving file to {}".format(self.filename))
                f.write(data)

    def fetch_temperature_data(self, file_name):
        epw = EPW()
        epw.read(file_name)
        self.data = [
            i.dry_bulb_temperature for i in epw.weatherdata]

    def slash_join(self, *args):
        return "/".join(arg.strip("/") for arg in args)

    def get_url_city(self, region, country, url_state, temperature_file):
        url_city = None

        result = requests.get(url_state).content
        soup = BeautifulSoup(result, "html.parser")
        for i in soup.find_all(
                "a", {"class": "btn btn-default left-justify blue-btn"}):

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
        f = RESULT_DIR
        # f = resource_filename(__name__, "/specifications/")
        for file_name in os.listdir(f):
            if file_name.endswith(".epw"):
                _temmp_file = self.temperature_file.replace(" ", ".")
                if ((self.country in file_name) and
                    (self.state in file_name) and
                        (_temmp_file in file_name)):
                    return file_name
