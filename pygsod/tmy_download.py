# from pkg_resources import resource_filename
import datetime
import warnings
from pathlib import Path
from typing import List, Optional

import pandas as pd
import requests
from pyepw.epw import EPW

from pygsod.constants import RESULT_DIR, WEATHER_DIR


class TMY(object):
    """Provide Typical Meteorological Year data for selected location."""

    geojson_url = "https://github.com/NREL/EnergyPlus/" "raw/develop/weather/master.geojson"

    download_dir_path = Path(WEATHER_DIR)

    def __init__(self, country: str, temperature_file: str, state: Optional[str] = None):
        self.country = country
        self.state = state
        self.temperature_file = temperature_file

        # create the str to lookup in the EP API
        self.lookup_str = self.make_lookup_str(
            country=self.country,
            temperature_file=self.temperature_file,
            state=self.state,
        )

        # check if the file is already downloaded
        self.filepath = self.locate_local_epw(lookup_str=self.lookup_str)
        if not self.filepath:
            self.filepath = self.download_data(lookup_str=self.lookup_str)

        self.fname_epw = self.filepath.name
        self.fname_xlsx = self.filepath.stem + ".xlsx"
        self.filepath_xlsx = Path(RESULT_DIR) / self.fname_xlsx
        # format to be called by output.py
        self.fname = str(Path(RESULT_DIR) / self.filepath.stem)

        self.create_dataframe()

    @classmethod
    def download_data(cls, lookup_str: str) -> Path:
        """Download epw weather file from website."""

        r = requests.get(cls.geojson_url)
        r.raise_for_status()

        data = r.json()
        print(lookup_str)
        matches = [m for x in data["features"] if lookup_str in (m := x["properties"])["title"]]
        if not matches:
            raise ValueError(f"Cannot find {lookup_str}")
        if len(matches) > 1:
            print(f"More than one match for {lookup_str}, returning the first")

        m = matches[0]

        epw_url = m["epw"].split("href=")[1].split(">")[0]
        # TODO: this means that you download to the site-packages folder of
        # your python install (unless you did pip install -e .) which is not a
        # good thing (and may very well not work if say python is installed in
        # C:\Program Files\ or any write protected folder)
        filepath = cls.download_dir_path / Path(epw_url).name

        if filepath.exists():  # we already have this file
            pass

        else:
            r = requests.get(epw_url)
            r.raise_for_status()
            data = r.content
            with open(filepath, "wb") as f:
                print(f"Saving file to {filepath}")
                f.write(data)

        return filepath

    @staticmethod
    def read_temperature_data(filepath: Path) -> List[float]:
        epw = EPW()
        try:
            epw.read(filepath)
        except Exception as e:
            raise Exception(f"Failed to read EPW at {filepath}, " f"exists? {filepath.exists()}").with_traceback(
                e.__traceback__
            )

        data = [i.dry_bulb_temperature for i in epw.weatherdata]

        return data

    @classmethod
    def locate_local_epw(cls, lookup_str: str) -> Optional[Path]:
        matches = list(cls.download_dir_path.glob(f"**/*{lookup_str}*.epw"))
        if not matches:
            return None

        if len(matches) > 1:
            warnings.warn(
                f"Found more than one EPW that matches {lookup_str}, " f"returning the first of: {matches}",
                UserWarning,
            )

        return matches[0]

    @staticmethod
    def make_lookup_str(country: str, temperature_file: str, state: Optional[str] = None) -> str:
        lookup_str = f"{country}_"
        if state is not None and state != "":
            lookup_str += f"{state}_"

        lookup_str += temperature_file.replace(" ", ".")
        return lookup_str

    def create_dataframe(self):
        epw = EPW()
        epw.read(self.filepath)

        dic = {
            "year": [i.year for i in epw.weatherdata],
            "month": [i.month for i in epw.weatherdata],
            "day": [i.day for i in epw.weatherdata],
            "hour": [i.hour for i in epw.weatherdata],
            "minute": [i.minute for i in epw.weatherdata],
            "aerosol_optical_depth": [i.aerosol_optical_depth for i in epw.weatherdata],
            "albedo": [i.albedo for i in epw.weatherdata],
            "atmospheric_station_pressure": [i.atmospheric_station_pressure for i in epw.weatherdata],
            "ceiling_height": [i.ceiling_height for i in epw.weatherdata],
            "data_source_and_uncertainty_flags": [i.data_source_and_uncertainty_flags for i in epw.weatherdata],
            "days_since_last_snowfall": [i.days_since_last_snowfall for i in epw.weatherdata],
            "dew_point_temperature": [i.dew_point_temperature for i in epw.weatherdata],
            "diffuse_horizontal_illuminance": [i.diffuse_horizontal_illuminance for i in epw.weatherdata],
            "diffuse_horizontal_radiation": [i.diffuse_horizontal_radiation for i in epw.weatherdata],
            "direct_normal_illuminance": [i.direct_normal_illuminance for i in epw.weatherdata],
            "direct_normal_radiation": [i.direct_normal_radiation for i in epw.weatherdata],
            "dry_bulb_temperature": [i.dry_bulb_temperature for i in epw.weatherdata],
            "extraterrestrial_direct_normal_radiation": [
                i.extraterrestrial_direct_normal_radiation for i in epw.weatherdata
            ],
            "extraterrestrial_horizontal_radiation": [i.extraterrestrial_horizontal_radiation for i in epw.weatherdata],
            "field_count": [i.field_count for i in epw.weatherdata],
            "global_horizontal_illuminance": [i.global_horizontal_illuminance for i in epw.weatherdata],
            "global_horizontal_radiation": [i.global_horizontal_radiation for i in epw.weatherdata],
            "horizontal_infrared_radiation_intensity": [
                i.horizontal_infrared_radiation_intensity for i in epw.weatherdata
            ],
            "liquid_precipitation_depth": [i.liquid_precipitation_depth for i in epw.weatherdata],
            "liquid_precipitation_quantity": [i.liquid_precipitation_quantity for i in epw.weatherdata],
            "opaque_sky_cover": [i.opaque_sky_cover for i in epw.weatherdata],
            "precipitable_water": [i.precipitable_water for i in epw.weatherdata],
            "present_weather_codes": [i.present_weather_codes for i in epw.weatherdata],
            "present_weather_observation": [i.present_weather_observation for i in epw.weatherdata],
            "relative_humidity": [i.relative_humidity for i in epw.weatherdata],
            "snow_depth": [i.snow_depth for i in epw.weatherdata],
            "total_sky_cover": [i.total_sky_cover for i in epw.weatherdata],
            "visibility": [i.visibility for i in epw.weatherdata],
            "wind_direction": [i.wind_direction for i in epw.weatherdata],
            "wind_speed": [i.wind_speed for i in epw.weatherdata],
            "zenith_luminance": [i.zenith_luminance for i in epw.weatherdata],
        }

        df_hourly = pd.DataFrame(dic)

        index = pd.date_range(
            freq="1H",
            start=datetime.datetime(
                2019,
                df_hourly["month"][0],
                df_hourly["day"][0],
                df_hourly["hour"][0] - 1,
            ),
            end=datetime.datetime(
                2019,
                df_hourly["month"].iloc[-1],
                df_hourly["day"].iloc[-1],
                df_hourly["hour"].iloc[-1] - 1,
            ),
        )
        df_hourly = df_hourly.set_index(index)
        df_hourly["TEMP_F"] = df_hourly["dry_bulb_temperature"] * 1.8 + 32

        df_hourly.to_excel(self.filepath_xlsx)
        self.df_hourly = df_hourly
