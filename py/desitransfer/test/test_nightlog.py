# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test desitransfer.nightlog.
"""
# import datetime
import logging
import os
# import shutil
import sys
import unittest
from tempfile import TemporaryDirectory
from unittest.mock import call, patch, MagicMock
from pkg_resources import resource_filename
from ..nightlog import (_options, _configure_log)


class TestNightlog(unittest.TestCase):
    """Test desitransfer.nightlog.
    """

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        """Create a temporary directory to simulate CSCRATCH.
        """
        self.tmp = TemporaryDirectory()

    def tearDown(self):
        """Clean up temporary directory.
        """
        self.tmp.cleanup()

    def test_options(self):
        """Test command-line arguments.
        """
        with patch.object(sys, 'argv', ['desi_nightlog_transfer', '--debug']):
            options = _options()
            self.assertTrue(options.debug)
            self.assertEqual(options.kill,
                             os.path.join(os.environ['HOME'],
                                          'stop_desi_transfer'))

    @patch('desitransfer.nightlog.SMTPHandler')
    @patch('desitransfer.nightlog.RotatingFileHandler')
    @patch('desitransfer.nightlog.get_logger')
    @patch('desitransfer.nightlog.log')  # Needed to restore the module-level log object after test.
    def test_configure_log(self, mock_log, gl, rfh, smtp):
        """Test logging configuration.
        """
        with patch.dict('os.environ', {'CSCRATCH': self.tmp.name,
                                       'DESI_ROOT': '/desi/root'}):
            with patch.object(sys, 'argv', ['desi_nightlog_transfer', '--debug']):
                options = _options()
            _configure_log(options)
        rfh.assert_called_once_with('/desi/root/survey/ops/nightlogs/desi_nightlog_transfer.log',
                                    backupCount=100, maxBytes=100000000)
        gl.assert_called_once_with(timestamp=True)
        gl().setLevel.assert_called_once_with(logging.DEBUG)
