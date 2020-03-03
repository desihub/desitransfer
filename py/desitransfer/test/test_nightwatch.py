# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test desitransfer.nightwatch.
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
from ..nightwatch import (_options, _popen, _configure_log)

class TestNightwatch(unittest.TestCase):
    """Test desitransfer.nightwatch.
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
        with patch.object(sys, 'argv', ['desi_nightwatch_transfer', '--debug']):
            options = _options()
            self.assertTrue(options.debug)
            self.assertEqual(options.kill,
                             os.path.join(os.environ['HOME'],
                                          'stop_desi_transfer'))

    @patch('desitransfer.nightwatch.SMTPHandler')
    @patch('desitransfer.nightwatch.RotatingFileHandler')
    @patch('desitransfer.nightwatch.get_logger')
    @patch('desitransfer.nightwatch.log')  # Needed to restore the module-level log object after test.
    def test_configure_log(self, mock_log, gl, rfh, smtp):
        """Test logging configuration.
        """
        with patch.dict('os.environ',
                        {'CSCRATCH': self.tmp.name,
                         'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_nightwatch_transfer', '--debug']):
                options = _options()
            _configure_log(options)
        rfh.assert_called_once_with('/desi/root/spectro/nightwatch/desi_nightwatch_transfer.log',
                                    backupCount=100, maxBytes=100000000)
        gl.assert_called_once_with(timestamp=True)
        gl().setLevel.assert_called_once_with(logging.DEBUG)

    @patch('desitransfer.nightwatch.TemporaryFile')
    @patch('subprocess.Popen')
    @patch('desitransfer.nightwatch.log')
    def test_popen(self, mock_log, mock_popen, mock_temp):
        """Test Popen wrapper.
        """
        mock_file = mock_temp().__enter__.return_value = MagicMock()
        mock_file.read.return_value = b'MOCK'
        proc = mock_popen.return_value = MagicMock()
        proc.returncode = 0
        pp = _popen(['foo', 'bar'])
        self.assertEqual(pp, ('0', 'MOCK', 'MOCK'))
        mock_log.debug.assert_called_once_with('foo bar')
        mock_popen.assert_called_once_with(['foo', 'bar'], stdout=mock_file, stderr=mock_file)


def test_suite():
    """Allows testing of only this module with the command::

        python setup.py test -m <modulename>
    """
    return unittest.defaultTestLoader.loadTestsFromName(__name__)
