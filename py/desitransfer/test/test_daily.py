# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test desitransfer.daily.
"""
import os
import sys
import unittest
from unittest.mock import patch
from ..daily import _config, _options


class TestDaily(unittest.TestCase):
    """Test desitransfer.daily.
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

    def test_config(self):
        """Test transfer directory configuration.
        """
        with patch.dict('os.environ',
                        {'DESI_ROOT': '/desi/root'}):
            c = _config()
            self.assertEqual(c[0].source, '/exposures/desi/sps')
            self.assertEqual(c[0].staging, 'UNUSED')
            self.assertEqual(c[0].destination, os.path.join(os.environ['DESI_ROOT'],
                                                            'engineering', 'spectrograph', 'sps'))
            self.assertEqual(c[0].hpss, 'UNUSED')

    def test_options(self):
        """Test command-line arguments.
        """
        with patch.object(sys, 'argv',
                          ['desi_daily_transfer', '--daemon', '--kill',
                           os.path.expanduser('~/stop_daily_transfer')]):
            options = _options()
            self.assertEqual(options.sleep, 24)
            self.assertTrue(options.daemon)
            self.assertEqual(options.kill,
                             os.path.join(os.environ['HOME'],
                                          'stop_daily_transfer'))


def test_suite():
    """Allows testing of only this module with the command::

        python setup.py test -m <modulename>
    """
    return unittest.defaultTestLoader.loadTestsFromName(__name__)
