# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test desitransfer.tucson.
"""
import os
import sys
import unittest
import logging
import subprocess as sub
from tempfile import mkdtemp
from shutil import rmtree
from unittest.mock import patch, call, mock_open, MagicMock
from ..tucson import _options, _rsync, _configure_log, running, _get_proc
from .. import __version__ as dtVersion


class TestTucson(unittest.TestCase):
    """Test desitransfer.tucson.
    """

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = mkdtemp()

    @classmethod
    def tearDownClass(cls):
        rmtree(cls.temp_dir)

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

    @patch('desitransfer.tucson.SMTPHandler')
    @patch('desitransfer.tucson.get_logger')
    @patch('desitransfer.tucson.log')  # Needed to restore the module-level log object after test.
    def test_configure_log(self, mock_log, gl, smtp):
        """Test logging configuration.
        """
        with patch.dict('os.environ', {'MAILTO': 'alerts@my.org',
                                       'MAILFROM': 'from.me@example.com'}):
            _configure_log(True)
        gl.assert_called_once_with(timestamp=True)
        gl().setLevel.assert_called_once_with(logging.DEBUG)
        email_from = 'NOIRLab Mirror Account <from.me@example.com>'
        smtp.assert_called_once_with('localhost', email_from,
                                     ['alerts@my.org'],
                                     'Error reported by desi_tucson_transfer!')

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

    @patch('os.path.exists')
    def test_running_write(self, mock_exists):
        """Test check for running process, with no actual process running.
        """
        pid = str(os.getpid())
        mock_exists.return_value = False
        m = mock_open()
        with patch('desitransfer.tucson.open', m) as mo:
            r = running('foo.pid')
        self.assertFalse(r)
        m.assert_called_once_with('foo.pid', 'w')
        handle = m()
        handle.write.assert_called_once_with(pid)

    @patch('os.remove')
    @patch('os.path.exists')
    @patch('subprocess.Popen')
    @patch('desitransfer.tucson.log')
    def test_running_read(self, mock_log, mock_popen, mock_exists, mock_remove):
        """Test check for running process, with pid file present.
        """
        pid = str(os.getpid())
        mock_exists.return_value = True
        m = mock_open(read_data='12345\n')
        proc = mock_popen()
        proc.returncode = 0
        proc.communicate.return_value = (b'', b'')
        with patch('desitransfer.tucson.open', m) as mo:
            r = running('foo.pid')
        self.assertFalse(r)
        m.assert_has_calls([call('foo.pid'),
                            call().__enter__(),
                            call().read(),
                            call().__exit__(None, None, None),
                            call('foo.pid', 'w'),
                            call().__enter__(),
                            call().write(pid),
                            call().__exit__(None, None, None)])
        # handle = m()
        # handle.read.assert_called()
        mock_log.debug.assert_has_calls([call('/usr/bin/ps -q 12345 -o comm='),
                                         call("os.remove('%s')", 'foo.pid')])
        mock_popen.assert_has_calls([call(['/usr/bin/ps', '-q', '12345', '-o', 'comm='],
                                          stdout=sub.PIPE, stderr=sub.PIPE),
                                     call().communicate()])
        mock_popen().communicate.assert_called_once()
        mock_remove.assert_called_once_with('foo.pid')

    @patch('os.remove')
    @patch('os.path.exists')
    @patch('subprocess.Popen')
    @patch('desitransfer.tucson.log')
    def test_running_read_exit(self, mock_log, mock_popen, mock_exists, mock_remove):
        """Test check for running process, with actual process running.
        """
        pid = str(os.getpid())
        mock_exists.return_value = True
        m = mock_open(read_data='12345\n')
        proc = mock_popen()
        proc.returncode = 0
        proc.communicate.return_value = (b'desi_tucson_transfer\n', b'')
        with patch('desitransfer.tucson.open', m) as mo:
            r = running('foo.pid')
        self.assertTrue(r)
        m.assert_has_calls([call('foo.pid'),
                            call().__enter__(),
                            call().read(),
                            call().__exit__(None, None, None)])
        # handle = m()
        # handle.read.assert_called()
        mock_log.debug.assert_has_calls([call('/usr/bin/ps -q 12345 -o comm='),
                                         call('desi_tucson_transfer')])
        mock_log.critical.assert_called_once_with("Running process detected (%s = %s), exiting.",
                                                  '12345', 'desi_tucson_transfer')
        mock_popen.assert_has_calls([call(['/usr/bin/ps', '-q', '12345', '-o', 'comm='],
                                          stdout=sub.PIPE, stderr=sub.PIPE),
                                     call().communicate()])
        mock_popen().communicate.assert_called_once()

    @patch('subprocess.Popen')
    @patch('desitransfer.tucson.log')
    def test_get_proc(self, mock_log, mock_popen):
        """Test the function for generating external procedures.
        """
        directories = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i']
        exclude = set(['d', 'g'])
        options = MagicMock()
        options.test = False
        options.log = self.temp_dir
        proc, LOG_A, d = _get_proc(directories, exclude, '/src', '/dst', options)
        self.assertEqual(d, 'a')
        LOG_A.close()
        options.test = True
        proc, LOG_B, d = _get_proc(directories, exclude, '/src', '/dst', options)
        self.assertEqual(d, 'b')
        self.assertEqual(LOG_B, os.path.join(self.temp_dir, 'desi_tucson_transfer_b.log'))
        proc, LOG_C, d = _get_proc(directories, exclude, '/src', '/dst', options)
        self.assertEqual(d, 'c')
        proc, LOG_E, d = _get_proc(directories, exclude, '/src', '/dst', options)
        self.assertEqual(d, 'e')
        proc, LOG_F, d = _get_proc(directories, exclude, '/src', '/dst', options)
        self.assertEqual(d, 'f')
        proc, LOG_H, d = _get_proc(directories, exclude, '/src', '/dst', options)
        self.assertEqual(d, 'h')
        proc, LOG_I, d = _get_proc(directories, exclude, '/src', '/dst', options)
        self.assertEqual(d, 'i')
        proc, LOG_J, d = _get_proc(directories, exclude, '/src', '/dst', options)
        self.assertIsNone(proc)
