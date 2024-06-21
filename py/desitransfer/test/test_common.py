# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test desitransfer.common.
"""
from datetime import datetime, timedelta
import unittest
from unittest.mock import patch
from tempfile import TemporaryDirectory
from ..common import (dt, MST, dir_perm, file_perm, empty_rsync, new_exposures, rsync,
                      stamp, ensure_scratch, yesterday, today, idle_time, exclude_years)


class FakeDateTime(datetime):
    """Enable easier mocking of certain methods of :class:`datetime.datetime`.
    """

    def __new__(cls, *args, **kwargs):
        return datetime.__new__(datetime, *args, **kwargs)


class TestCommon(unittest.TestCase):
    """Test desitransfer.common.
    """

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        """Create a temporary directory to simulate SCRATCH.
        """
        self.tmp = TemporaryDirectory()

    def tearDown(self):
        """Clean up temporary directory.
        """
        self.tmp.cleanup()

    def test_permissions(self):
        """Ensure that file and directory permissions do not change.
        """
        self.assertEqual(dir_perm, 0o2750)
        self.assertEqual(file_perm, 0o0440)

    def test_empty_rsync(self):
        """Test parsing of rsync output.
        """
        r = """receiving incremental file list

sent 765 bytes  received 238,769 bytes  159,689.33 bytes/sec
total size is 118,417,836,324  speedup is 494,367.55
"""
        self.assertTrue(empty_rsync(r))
        r = """receiving incremental file list
foo/bar.txt

sent 765 bytes  received 238,769 bytes  159,689.33 bytes/sec
total size is 118,417,836,324  speedup is 494,367.55
"""
        self.assertFalse(empty_rsync(r))

    def test_new_exposures(self):
        """Test parsing of rsync output for new exposures.
        """
        r = """receiving incremental file list

sent 765 bytes  received 238,769 bytes  159,689.33 bytes/sec
total size is 118,417,836,324  speedup is 494,367.55
"""
        self.assertEqual(len(new_exposures(r)), 0)
        r = """receiving incremental file list
12345678/foo.txt
12345679/foo.txt

sent 765 bytes  received 238,769 bytes  159,689.33 bytes/sec
total size is 118,417,836,324  speedup is 494,367.55
"""
        self.assertEqual(len(new_exposures(r)), 2)

    def test_rsync(self):
        """Test construction of rsync command.
        """
        r = rsync('/source', '/destination')
        self.assertListEqual(r, ['/bin/rsync', '--verbose',
                                 '--recursive', '--copy-dirlinks', '--times',
                                 '--omit-dir-times', 'dts:/source/',
                                 '/destination/'])
        r = rsync('/source', '/destination', test=True)
        self.assertListEqual(r, ['/bin/rsync', '--dry-run', '--verbose',
                                 '--recursive', '--copy-dirlinks', '--times',
                                 '--omit-dir-times', 'dts:/source/',
                                 '/destination/'])
        r = rsync('/source', '/destination', reverse=True)
        self.assertListEqual(r, ['/bin/rsync', '--verbose',
                                 '--recursive', '--copy-dirlinks', '--times',
                                 '--omit-dir-times', '/source/',
                                 'dts:/destination/'])

    @patch('desitransfer.common.dt')
    def test_stamp(self, mock_dt):
        """Test timestamp.
        """
        mock_dt.datetime.utcnow.return_value = datetime(2019, 7, 3, 12, 0, 0)
        s = stamp('US/Arizona')
        self.assertEqual(s, '2019-07-03 05:00:00 MST')

    def test_ensure_scratch(self):
        """Test ensure_scratch.
        """
        tmp = self.tmp.name
        t = ensure_scratch([tmp, '/foo', '/bar'])
        self.assertEqual(t, tmp)
        t = ensure_scratch(['/foo', tmp])
        self.assertEqual(t, tmp)
        t = ensure_scratch(['/foo', '/bar', tmp])
        self.assertEqual(t, tmp)
        t = ensure_scratch(['/foo', '/bar', '/abcdefg', tmp])
        self.assertEqual(t, tmp)

    @patch('desitransfer.common.dt')
    def test_yesterday(self, mock_dt):
        """Test yesterday's date.
        """
        mock_dt.datetime.now.return_value = datetime(2019, 7, 3, 12, 0, 0)
        mock_dt.timedelta.return_value = timedelta(seconds=86400)
        y = yesterday()
        self.assertEqual(y, '20190702')

    @patch('desitransfer.common.dt')
    def test_today(self, mock_dt):
        """Test today's date.
        """
        mock_dt.datetime.utcnow.return_value = datetime(2019, 7, 3, 5, 0, 0)
        mock_dt.timedelta.return_value = timedelta(7 / 24 + 0.5)
        y = today()
        self.assertEqual(y, '20190702')

    @patch('desitransfer.common.dt.datetime', FakeDateTime)
    def test_idle_time(self):
        """Test idle_time check.
        """
        FakeDateTime.now = classmethod(lambda cls, tz: datetime(2021, 7, 3, 7, 0, 0, tzinfo=tz))
        # mock_datetime.now.return_value = datetime(2021, 7, 3, 7, 0, 0, tzinfo=MST)
        # mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
        i = idle_time()
        self.assertEqual(i, -3600)
        FakeDateTime.now = classmethod(lambda cls, tz: datetime(2021, 7, 3, 11, 0, 0, tzinfo=tz))
        # mock_datetime.return_value = datetime(2021, 7, 3, 11, 0, 0, tzinfo=MST)
        i = idle_time()
        self.assertEqual(i, 3600)
        FakeDateTime.now = classmethod(lambda cls, tz: datetime(2021, 7, 3, 13, 0, 0, tzinfo=tz))
        # mock_datetime.return_value = datetime(2021, 7, 3, 13, 0, 0, tzinfo=MST)
        i = idle_time()
        self.assertEqual(i, -3600)

    @patch('desitransfer.common.dt.datetime', FakeDateTime)
    def test_idle_time_alt_time_zone(self):
        """Test idle_time check with alternate time zone.
        """
        FakeDateTime.now = classmethod(lambda cls, tz: datetime(2021, 7, 3, 7, 0, 0, tzinfo=tz))
        # mock_datetime.now.return_value = datetime(2021, 7, 3, 7, 0, 0, tzinfo=MST)
        # mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
        i = idle_time(tz='US/Pacific')
        self.assertEqual(i, -3600)
        FakeDateTime.now = classmethod(lambda cls, tz: datetime(2021, 7, 3, 11, 0, 0, tzinfo=tz))
        # mock_datetime.return_value = datetime(2021, 7, 3, 11, 0, 0, tzinfo=MST)
        i = idle_time(tz='US/Pacific')
        self.assertEqual(i, 3600)
        FakeDateTime.now = classmethod(lambda cls, tz: datetime(2021, 7, 3, 13, 0, 0, tzinfo=tz))
        # mock_datetime.return_value = datetime(2021, 7, 3, 13, 0, 0, tzinfo=MST)
        i = idle_time(tz='US/Pacific')
        self.assertEqual(i, -3600)

    def test_exclude_years(self):
        """Test exclude statements for a range of years.
        """
        last_year = datetime.now().year - 1
        ex = exclude_years(2018)
        self.assertEqual(ex[1], '2018*')
        self.assertEqual(ex[-1], f'{last_year:d}*')
