# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test desitransfer.daemon.
"""
import os
import unittest
from unittest.mock import patch
from ..daemon import _config


class TestDaemon(unittest.TestCase):
    """Test desitransfer.daemon.
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
                        {'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            c = _config()
            self.assertEqual(c[0].source, '/data/dts/exposures/raw')
            self.assertEqual(c[0].staging,
                             os.path.join(os.environ['DESI_ROOT'],
                                          'spectro', 'staging', 'raw'))
            self.assertEqual(c[0].destination, os.environ['DESI_SPECTRO_DATA'])
            self.assertEqual(c[0].hpss, 'desi/spectro/data')


def test_suite():
    """Allows testing of only this module with the command::

        python setup.py test -m <modulename>
    """
    return unittest.defaultTestLoader.loadTestsFromName(__name__)
