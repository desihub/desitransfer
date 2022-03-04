# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test desitransfer.tucson.
"""
import os
import sys
import unittest
from unittest.mock import patch, call, mock_open, Mock
from ..tucson import _options, _rsync
from .. import __version__ as dtVersion


class TestTucson(unittest.TestCase):
    """Test desitransfer.tucson.
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

    def test_options(self):
        """Test command-line arguments.
        """
        with patch.dict('os.environ',
                        {'DESI_ROOT': '/desi/root'}):
            with patch.object(sys, 'argv',
                              ['desi_tucson_transfer', '--debug', '--sleep', '30m',
                               '--exclude', 'foo', 'bar']):
                options = _options()
                self.assertTrue(options.debug)
                self.assertEqual(options.sleep, '30m')
                self.assertListEqual(options.exclude, ['foo', 'bar'])
                self.assertEqual(options.log,
                                 os.path.join(os.environ['HOME'], 'Documents', 'Logfiles'))
            with patch.object(sys, 'argv',
                              ['desi_tucson_transfer', '--debug', '--sleep', '30m',
                               '--destination', 'foo/bar']):
                options = _options()
                self.assertTrue(options.debug)
                self.assertEqual(options.sleep, '30m')
                self.assertEqual(options.destination, 'foo/bar')
                self.assertIsNone(options.exclude)
                self.assertEqual(options.log,
                                 os.path.join(os.environ['HOME'], 'Documents', 'Logfiles'))

    def test_rsync(self):
        """Test rsync command construction.
        """
        rsync = _rsync('/Source', '/Destination', 'foo', checksum=False)
        self.assertListEqual(rsync, ['/usr/bin/rsync', '--archive', '--verbose',
                                     '--delete', '--delete-after', '--no-motd',
                                     '--password-file', os.path.join(os.environ['HOME'], '.desi'),
                                     '/Source/foo/', '/Destination/foo/'])
        rsync = _rsync('/Source', '/Destination', 'spectro/desi_spectro_calib', checksum=True)
        self.assertListEqual(rsync, ['/usr/bin/rsync', '--archive', '--checksum', '--verbose',
                                     '--delete', '--delete-after', '--no-motd',
                                     '--password-file', os.path.join(os.environ['HOME'], '.desi'),
                                     "--exclude", ".svn",
                                     '/Source/spectro/desi_spectro_calib/', '/Destination/spectro/desi_spectro_calib/'])
