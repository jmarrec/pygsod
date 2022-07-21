# In top level directory, run with python -m pytest
# so that the folder is added to PYTHONPATH
import datetime
# import numpy as np
import os
from pathlib import Path

import pandas as pd
import pytest

from pygsod.constants import ISDHISTORY_PATH, WEATHER_DIR
# Right now I have to do this, so that the pandas monkeypatching is done...
from pygsod.epw_converter import clean_df
from pygsod.isdhistory import ISDHistory
from pygsod.ish_full import parse_ish_file
from pygsod.noaadata import NOAAData
from pygsod.output import Output
from pygsod.utils import DataType, ReturnCode, sanitize_usaf_wban

# from mock import patch


class TestGSOD:
    """py.test class for GSOD."""

    def test_init_default(self):
        """py.test for GSOD initialization."""
        gsod = NOAAData(data_type=DataType.gsod)
        assert gsod
        assert gsod.weather_dir
        assert gsod.isd
        assert isinstance(gsod.isd, ISDHistory)
        assert gsod.isd.isd_history_path == ISDHISTORY_PATH

    def test_init_isd_path(self):
        """
        py.test for GSOD initialization with isd_path supplied
        """
        isd_path = Path(__file__).resolve().parent / "test_isd_path.csv"

        gsod = NOAAData(data_type=DataType.gsod, isd_path=isd_path)
        assert gsod
        assert gsod.weather_dir
        assert gsod.isd
        assert isinstance(gsod.isd, ISDHistory)
        assert gsod.isd.isd_history_path
        assert gsod.isd.isd_history_path == isd_path

    def test_set_years(self):
        """
        py.test for GSOD.set_years with bad years
        """
        gsod = NOAAData(data_type=DataType.gsod)
        gsod.set_years([2016, "2003", 2004])
        assert gsod.years == [2003, 2004, 2016]

    def test_set_years_bad(self):
        """
        py.test for GSOD.set_years with bad years
        """
        gsod = NOAAData(data_type=DataType.gsod)
        with pytest.raises(ValueError):
            gsod.set_years([2016, 2004, "2003", "not an int"])

    def test_set_years_range(self):
        """
        py.test for GSOD.set_years_range with bad years
        """
        gsod = NOAAData(data_type=DataType.gsod)

        # test default
        gsod.set_years_range()
        current_year = datetime.date.today().year
        assert gsod.years == [x for x in range(2003, current_year + 1)]

        # test arguments
        gsod.set_years_range(start_year=2008, end_year=2010)
        assert gsod.years == [2008, 2009, 2010]

        # tests bad arguments
        with pytest.raises(ValueError):
            gsod.set_years_range(start_year=2010, end_year=2008)

    def test_sanitize_usaf_wban(self):
        """
        py.test for GSOD.sanitize_usaf_wban with bad years
        """

        # Test sanitation of USAF
        with pytest.warns(SyntaxWarning) as record:
            sanit = sanitize_usaf_wban("64500-99999")
        # check that only one warning was raised
        assert len(record) == 1
        assert sanit == "064500-99999"

        # Test sanitation of WBAN
        with pytest.warns(SyntaxWarning) as record:
            sanit = sanitize_usaf_wban("064500-9999")
        # check that only one warning was raised
        assert len(record) == 1
        assert sanit == "064500-09999"

        # Test bad USAF, too long
        with pytest.raises(ValueError):
            sanitize_usaf_wban("0645007-99999")

        # Test bad WBAN, too long
        with pytest.raises(ValueError):
            sanitize_usaf_wban("064500-919999")

    def test_get_stations_from_file(self):
        """
        py.test for GSOD.get_stations_from_file
        """

        gsod = NOAAData(data_type=DataType.gsod)
        # 744860-94789
        # 725020-14734
        # 64500-99999 ==> Sanitized to 064500-99999

        with pytest.warns(SyntaxWarning) as record:
            gsod.get_stations_from_file("tests/test_weather_stations.txt")
        # check that only one warning was raised
        assert len(record) == 1
        assert sorted(gsod.stations) == sorted(
            ["744860-94789", "725020-14734", "064500-99999"]
        )


class TestGSODDownloads:
    """py.test class for tests around GSOD's function for downloading/cleaning files."""

    def test_download_GSOD_file(self):
        """py.test for GSOD._download_GSOD_file."""
        gsod = NOAAData(data_type=DataType.gsod)
        (return_code, local_path) = gsod.get_year_file(
            year=2017, usaf_wban="744860-94789"
        )
        assert return_code == ReturnCode.success
        assert (
            local_path
            == (
                WEATHER_DIR
                / "gsod"
                / "2017"
                / "JOHN F KENNEDY INTERNATIONAL AIRPORT-2017.op"
            ).resolve()
        )
        assert local_path.is_file()


class TestISD:
    """
    py.test class for ISD
    """

    @pytest.fixture(scope="class", autouse=False)
    def isd(self):
        gsod = NOAAData(data_type=DataType.gsod)
        return gsod.isd

    # @patch (ISD.download_isd)
    def test_update_isd_needed(self, isd):
        """
        py.test for ISD.update_isd_history
        """

        # This ran once already, within the fixture, so the file should be
        # downloaded

        # This not should update it
        assert not isd.update_isd_history(dry_run=True)

        # Force update
        assert isd.update_isd_history(force=True, dry_run=True)

        # Remove the file, then try again
        os.remove(isd.isd_history_path)
        assert isd.update_isd_history(dry_run=True)

    def test_parse_isd(self, isd):
        """
        py.test for ISD.parse_isd
        """
        assert isinstance(isd.df, pd.DataFrame)

    def test_closest_weather_station(self, isd):
        """
        py.test for ISD.parse_isd
        """
        lat = 51.177593
        lon = 4.410888
        closest = isd.closest_weather_station(lat, lon)
        assert closest == "064500-99999"


class TestISDFULL:
    """
    py.test class for isd_full
    """

    @pytest.fixture(scope="class", autouse=False)
    def isd_full(self):
        isd_full = NOAAData(data_type=DataType.isd_full)

        # This is what's run
        start_year = 2012
        end_year = 2013

        # Download the data
        isd_full.set_years_range(start_year=start_year, end_year=end_year)

        isd_full.set_stations(["744860-94789"])

        isd_full.get_all_data()

        return isd_full

    @pytest.fixture(scope="class", autouse=False)
    def df(self, isd_full):
        df = parse_ish_file(isd_full)

        return df

    def test_parse_ish_file(self, df):
        # isd_full = NOAAData(data_type=DataType.isd_full)
        # isd_full.set_years_range(2012, 2012)
        # isd_full.stations = ['744860-94789']
        # isd_full.get_all_data()

        assert isinstance(df, pd.DataFrame)
        assert df.shape == (13520, 18)

    """
    test for epw_converter
    """

    def test_clean_df(self, df, isd_full):
        #      Test clean_df()
        # ----------------
        df_hourly = clean_df(df, "test")
        assert df_hourly.shape == (8784, 12)  # year of 2012 has 8784 hrs

        #      Test output_daily()
        # ----------------

        o = Output(
            file=isd_full.ops_files[0],
            type_of_output="CSV",
            hdd_threshold=65.0,
            cdd_threshold=65.0,
        )

        df_daily = o.output_daily(df_hourly)
        assert df_daily.shape == (366, 11)

        #      Test output_monthly()
        # ----------------
        df_monthly = o.output_monthly(df_hourly, df_daily)
        assert df_monthly.shape == (12, 11)

        # TO DO
        # ADD TEST FOR EPW and JSON?

    # def test_epw_convert(self):
    #     epw_convert(df, root, file)
