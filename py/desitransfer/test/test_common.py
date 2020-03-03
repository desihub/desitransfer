# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test desitransfer.common.
"""
import datetime
import os
import unittest
from unittest.mock import patch
from ..common import dir_perm, file_perm, empty_rsync, rsync, stamp, yesterday, today


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
        pass

    def tearDown(self):
        pass

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

    @patch('desitransfer.common.dt')
    def test_stamp(self, mock_dt):
        """Test timestamp.
        """
        mock_dt.datetime.utcnow.return_value = datetime.datetime(2019, 7, 3, 12, 0, 0)
        s = stamp('US/Arizona')
        self.assertEqual(s, '2019-07-03 05:00:00 MST')

    @patch('desitransfer.common.dt')
    def test_yesterday(self, mock_dt):
        """Test yesterday's date.
        """
        mock_dt.datetime.now.return_value = datetime.datetime(2019, 7, 3, 12, 0, 0)
        mock_dt.timedelta.return_value = datetime.timedelta(seconds=86400)
        y = yesterday()
        self.assertEqual(y, '20190702')

    @patch('desitransfer.common.dt')
    def test_today(self, mock_dt):
        """Test today's date.
        """
        mock_dt.datetime.now.return_value = datetime.datetime(2019, 7, 3, 5, 0, 0)
        mock_dt.timedelta.return_value = datetime.timedelta(7/24+0.5)
        y = today()
        self.assertEqual(y, '20190702')


def test_suite():
    """Allows testing of only this module with the command::

        python setup.py test -m <modulename>
    """
    return unittest.defaultTestLoader.loadTestsFromName(__name__)
