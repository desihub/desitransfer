# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test desitransfer.daemon.
"""
import logging
import os
import sys
import unittest
from unittest.mock import call, patch, MagicMock
from pkg_resources import resource_filename
from ..daemon import (_config, _configure_log, PipelineCommand,
                      _options, _read_configuration, _popen, log,
                      check_exposure, verify_checksum,
                      lock_directory, unlock_directory, rsync_night)


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

    def test_read_configuration(self):
        """Test reading configuration file.
        """
        with patch.dict('os.environ',
                        {'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            c = _read_configuration()
        self.assertEqual(c[0]['destination'], '/desi/root/spectro/data')
        self.assertEqual(c[0]['staging'], '/desi/root/spectro/staging/raw')
        self.assertEqual(c[0]['pipeline']['desi_night'],
                         os.path.join(os.environ['HOME'], 'bin', 'wrap_desi_night.sh'))

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

    def test_PipelineCommand(self):
        """Test pipeline command generation.
        """
        dn = os.path.join(os.environ['HOME'], 'bin', 'wrap_desi_night.sh')
        with patch('desitransfer.daemon.log') as m:
            p = PipelineCommand('cori')
            c = p.command('20200703', '12345678')
            self.assertListEqual(c, ['ssh', '-q', 'cori', dn, 'update',
                                     '--night', '20200703',
                                     '--expid', '12345678',
                                     '--nersc', 'cori',
                                     '--nersc_queue', 'realtime',
                                     '--nersc_maxnodes', '25'])
        m.debug.assert_called_with(' '.join(c))
        with patch('desitransfer.daemon.log') as m:
            p = PipelineCommand('cori')
            c = p.command('20200703', '12345678', 'science')
            self.assertListEqual(c, ['ssh', '-q', 'cori', dn, 'redshifts',
                                     '--night', '20200703',
                                     '--expid', '12345678',
                                     '--nersc', 'cori',
                                     '--nersc_queue', 'realtime',
                                     '--nersc_maxnodes', '25'])
        m.debug.assert_called_with(' '.join(c))

    def test_options(self):
        """Test command-line arguments.
        """
        with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug']):
            options = _options()
            self.assertEqual(options.backup, 20)
            self.assertTrue(options.debug)
            self.assertEqual(options.kill,
                             os.path.join(os.environ['HOME'],
                                          'stop_desi_transfer'))

    @patch('desitransfer.daemon.TemporaryFile')
    @patch('subprocess.Popen')
    @patch('desitransfer.daemon.log')
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

    @patch('desitransfer.daemon.SMTPHandler')
    @patch('desitransfer.daemon.RotatingFileHandler')
    @patch('desitransfer.daemon.get_logger')
    def test_configure_log(self, gl, rfh, smtp):
        """Test logging configuration.
        """
        ll = gl.return_value = MagicMock()
        with patch.dict('os.environ', {'DESI_ROOT': '/desi'}):
            _configure_log(True)
        rfh.assert_called_once_with('/desi/spectro/staging/logs/desi_transfer_daemon.log',
                                    backupCount=100, maxBytes=100000000)
        gl.assert_called_once_with(timestamp=True)
        ll.setLevel.assert_called_once_with(logging.DEBUG)

    @patch('os.path.exists')
    def test_check_exposure(self, mock_exists):
        """Test detection of expected files.
        """
        mock_exists.return_value = True
        self.assertTrue(check_exposure('/desi/20190703', 12345678))
        mock_exists.assert_has_calls([call('/desi/20190703/desi-12345678.fits.fz'),
                                      call('/desi/20190703/fibermap-12345678.fits'),
                                      call('/desi/20190703/guider-12345678.fits.fz')])

    def test_verify_checksum(self):
        """Test checksum verification.
        """
        c = resource_filename('desitransfer.test', 't/t.sha256sum')
        d = os.path.dirname(c)
        with patch('desitransfer.daemon.log') as l:
            o = verify_checksum(c, ['t.sha256sum', 'test_file_1.txt', 'test_file_2.txt'])
            self.assertEqual(o, 0)
        l.debug.assert_has_calls([call("%s is valid.", os.path.join(d, 'test_file_1.txt')),
                                  call("%s is valid.", os.path.join(d, 'test_file_2.txt'))])
        #
        # Wrong number of files.
        #
        with patch('desitransfer.daemon.log') as l:
            o = verify_checksum(c, ['t.sha256sum', 'test_file_1.txt'])
            self.assertEqual(o, -1)
        l.error.assert_has_calls([call("%s does not match the number of files!", c)])
        #
        # Bad list of files.
        #
        with patch('desitransfer.daemon.log') as l:
            o = verify_checksum(c, ['t.sha256sum', 'test_file_1.txt', 'test_file_3.txt'])
            self.assertEqual(o, 1)
        l.debug.assert_has_calls([call("%s is valid.", os.path.join(d, 'test_file_1.txt'))])
        l.error.assert_has_calls([call("%s does not appear in %s!", os.path.join(d, 'test_file_3.txt'), c)])
        #
        # Hack hashlib to produce incorrect checksums.
        #
        with patch('desitransfer.daemon.log') as l:
            with patch('hashlib.sha256') as h:
                # h.sha256 = MagicMock()
                h.hexdigest.return_value = 'abcdef'
                o = verify_checksum(c, ['t.sha256sum', 'test_file_1.txt', 'test_file_2.txt'])
                self.assertEqual(o, 2)
        l.error.assert_has_calls([call("Checksum mismatch for %s!", os.path.join(d, 'test_file_1.txt')),
                                  call("Checksum mismatch for %s!", os.path.join(d, 'test_file_2.txt'))])

    @patch('os.walk')
    @patch('os.chmod')
    @patch('desitransfer.daemon.log')
    def test_lock_directory(self, mock_log, mock_chmod, mock_walk):
        """Test directory locking.
        """
        mock_walk.return_value = [('/d0', ['d1', 'd2'], ['f1', 'f2']),
                                  ('/d0/d1', [], ['f3']),
                                  ('/d0/d2', [], ['f4'])]
        lock_directory('/d0')
        mock_log.debug.assert_has_calls([call("os.chmod('%s', 0o%o)", '/d0', 0o2750),
                                         call("os.chmod('%s', 0o%o)", '/d0/f1', 0o0440),
                                         call("os.chmod('%s', 0o%o)", '/d0/f2', 0o0440),
                                         call("os.chmod('%s', 0o%o)", '/d0/d1', 0o2750),
                                         call("os.chmod('%s', 0o%o)", '/d0/d1/f3', 0o0440),
                                         call("os.chmod('%s', 0o%o)", '/d0/d2', 0o2750),
                                         call("os.chmod('%s', 0o%o)", '/d0/d2/f4', 0o0440)])
        mock_chmod.assert_has_calls([call('/d0', 0o2750),
                                     call('/d0/f1', 0o0440),
                                     call('/d0/f2', 0o0440),
                                     call('/d0/d1', 0o2750),
                                     call('/d0/d1/f3', 0o0440),
                                     call('/d0/d2', 0o2750),
                                     call('/d0/d2/f4', 0o0440)])


    @patch('os.walk')
    @patch('os.chmod')
    @patch('desitransfer.daemon.log')
    def test_unlock_directory(self, mock_log, mock_chmod, mock_walk):
        """Test directory unlocking.
        """
        mock_walk.return_value = [('/d0', ['d1', 'd2'], ['f1', 'f2']),
                                  ('/d0/d1', [], ['f3']),
                                  ('/d0/d2', [], ['f4'])]
        unlock_directory('/d0')
        mock_log.debug.assert_has_calls([call("os.chmod('%s', 0o%o)", '/d0', 0o2750),
                                         call("os.chmod('%s', 0o%o)", '/d0/f1', 0o0640),
                                         call("os.chmod('%s', 0o%o)", '/d0/f2', 0o0640),
                                         call("os.chmod('%s', 0o%o)", '/d0/d1', 0o2750),
                                         call("os.chmod('%s', 0o%o)", '/d0/d1/f3', 0o0640),
                                         call("os.chmod('%s', 0o%o)", '/d0/d2', 0o2750),
                                         call("os.chmod('%s', 0o%o)", '/d0/d2/f4', 0o0640)])
        mock_chmod.assert_has_calls([call('/d0', 0o2750),
                                     call('/d0/f1', 0o0640),
                                     call('/d0/f2', 0o0640),
                                     call('/d0/d1', 0o2750),
                                     call('/d0/d1/f3', 0o0640),
                                     call('/d0/d2', 0o2750),
                                     call('/d0/d2/f4', 0o0640)])

    @patch('desitransfer.daemon._popen')
    @patch('desitransfer.daemon.lock_directory')
    @patch('desitransfer.daemon.unlock_directory')
    @patch('desitransfer.daemon.log')
    def test_rsync_night(self, mock_log, mock_unlock, mock_lock, mock_popen):
        """Test resyncing an entire night.
        """
        cmd = ['/bin/rsync', '--verbose', '--recursive', '--copy-dirlinks',
               '--times', '--omit-dir-times',
               'dts:/source/20190703/', '/destination/20190703/']
        mock_popen.return_value = ('0', 'stdout', 'stderr')
        rsync_night('/source', '/destination', '20190703', True)
        mock_log.debug.assert_called_with(' '.join(cmd))
        rsync_night('/source', '/destination', '20190703')
        mock_popen.assert_called_with(cmd)


def test_suite():
    """Allows testing of only this module with the command::

        python setup.py test -m <modulename>
    """
    return unittest.defaultTestLoader.loadTestsFromName(__name__)
