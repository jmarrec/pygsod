# In top level directory, run with python -m pytest
# so that the folder is added to PYTHONPATH
import pytest
from mock import patch

# Right now I have to do this, so that the pandas monkeypatching is done...
from gsodpy.gsodpy import GSOD, ISD

import pandas as pd
#import numpy as np
import os
import datetime


# This is used by almost all tests, so declaring it as global constant

class TestGSOD():
    """
    py.test class for GSOD
    """

    def test_init_default(self):
        """
        py.test for GSOD initialization
        """
        gsod = GSOD()
        assert gsod
        assert gsod.weather_dir
        assert gsod.isd
        assert isinstance(gsod.isd, ISD)
        print(gsod.isd.isd_path)
        print(os.path.realpath('../support/isd-history.csv'))
        assert gsod.isd.isd_path == os.path.realpath('support/isd-history.csv')

    def test_init_isd_path(self):
        """
        py.test for GSOD initialization with isd_path supplied
        """
        gsod = GSOD(isd_path='tests/test_isd_path.csv')
        assert gsod
        assert gsod.weather_dir
        assert gsod.isd
        assert isinstance(gsod.isd, ISD)
        assert gsod.isd.isd_path
        assert gsod.isd.isd_path == 'tests/test_isd_path.csv'


    def test_set_years(self):
        """
        py.test for GSOD.set_years with bad years
        """
        gsod = GSOD()
        gsod.set_years([2016, '2003', 2004])
        assert gsod.years == [2003, 2004, 2016]

    def test_set_years_bad(self):
        """
        py.test for GSOD.set_years with bad years
        """
        gsod = GSOD()
        with pytest.raises(ValueError):
            gsod.set_years([2016,2004, '2003', 'not an int'])


    def test_set_years_range(self):
        """
        py.test for GSOD.set_years_range with bad years
        """
        gsod = GSOD()

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
        gsod = GSOD()

        # Test sanitation of USAF
        with pytest.warns(SyntaxWarning) as record:
            sanit = gsod.sanitize_usaf_wban('64500-99999')
        # check that only one warning was raised
        assert len(record) == 1
        assert sanit == '064500-99999'

        # Test sanitation of WBAN
        with pytest.warns(SyntaxWarning) as record:
            sanit = gsod.sanitize_usaf_wban('064500-9999')
        # check that only one warning was raised
        assert len(record) == 1
        assert sanit == '064500-09999'

        # Test bad USAF, too long
        with pytest.raises(ValueError):
            gsod.sanitize_usaf_wban('0645007-99999')

        # Test bad WBAN, too long
        with pytest.raises(ValueError):
            gsod.sanitize_usaf_wban('064500-919999')

    def test_get_stations_from_file(self):
        """
        py.test for GSOD.get_stations_from_file
        """

        gsod = GSOD()
        # 744860-94789
        # 725020-14734
        # 64500-99999 ==> Sanitized to 064500-99999

        with pytest.warns(SyntaxWarning) as record:
            gsod.get_stations_from_file('tests/test_weather_stations.txt')
        # check that only one warning was raised
        assert len(record) == 1
        assert sorted(gsod.stations) == sorted(['744860-94789', '725020-14734',
                                                '064500-99999'])



class TestGSODDownloads():
    """
    py.test class for tests around GSOD's function for
    downloading/cleaning files
    """

    def test_download_GSOD_file(self):
        """
        py.test for GSOD._download_GSOD_file
        """
        gsod = GSOD()
        (return_code,
         local_path) = gsod._download_GSOD_file(year=2017,
                                                usaf_wban='064500-99999')
        assert return_code == 0
        assert local_path == 'weather_files/2017/064500-99999.op.gz'
        assert os.path.isfile(local_path)



class TestISD():
    """
    py.test class for ISD
    """

    @pytest.fixture(scope="class", autouse=False)
    def isd(self):
        gsod = GSOD()
        return gsod.isd

    def test_update_isd(self, isd):
        """
        py.test class for ISD.update_isd_history
        """
        assert isd.isd_path == os.path.realpath('support/isd-history.csv')

    #@patch (ISD.download_isd)
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
        os.remove(isd.isd_path)
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
        assert closest == '064500-99999'




