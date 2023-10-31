# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test desitransfer.spacewatch.
"""
import logging
import os
import sys
import unittest
from tempfile import TemporaryDirectory
from unittest.mock import call, patch
from ..spacewatch import (main, )


class TestSpacewatch(unittest.TestCase):
    """Test desitransfer.spacewatch.
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

    # def test_options(self):
    #     """Test command-line arguments.
    #     """
    #     with patch.object(sys, 'argv', ['desi_nightwatch_transfer', '--debug']):
    #         options = _options()
    #         self.assertTrue(options.debug)
    #         self.assertEqual(options.kill,
    #                          os.path.join(os.environ['HOME'],
    #                                       'stop_desi_transfer'))

    # @patch('desitransfer.nightwatch.SMTPHandler')
    # @patch('desitransfer.nightwatch.RotatingFileHandler')
    # @patch('desitransfer.nightwatch.get_logger')
    # @patch('desitransfer.nightwatch.log')  # Needed to restore the module-level log object after test.
    # def test_configure_log(self, mock_log, gl, rfh, smtp):
    #     """Test logging configuration.
    #     """
    #     with patch.dict('os.environ',
    #                     {'SCRATCH': self.tmp.name,
    #                      'DESI_ROOT': '/desi/root',
    #                      'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
    #         with patch.object(sys, 'argv', ['desi_nightwatch_transfer', '--debug']):
    #             options = _options()
    #         _configure_log(options)
    #     rfh.assert_called_once_with('/desi/root/spectro/nightwatch/desi_nightwatch_transfer.log',
    #                                 backupCount=100, maxBytes=100000000)
    #     gl.assert_called_once_with(timestamp=True)
    #     gl().setLevel.assert_called_once_with(logging.DEBUG)
