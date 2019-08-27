# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test desitransfer.daemon.
"""
import datetime
import logging
import os
import sys
import unittest
from tempfile import TemporaryDirectory
from unittest.mock import call, patch, MagicMock
from pkg_resources import resource_filename
from ..daemon import (_config, _configure_log, PipelineCommand,
                      _options, _read_configuration, _popen, log,
                      check_exposure, verify_checksum,
                      lock_directory, unlock_directory, rsync_night,
                      transfer_directory, transfer_exposure,
                      catchup_night, backup_night)


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
            c, s = _read_configuration()
        self.assertEqual(c[s[0]]['destination'], '/desi/root/spectro/data')
        self.assertEqual(c[s[0]]['staging'], '/desi/root/spectro/staging/raw')
        self.assertEqual(c[s[0]+'::pipeline']['desi_night'],
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
        with patch('os.listdir') as mock_listdir:
            mock_listdir.return_value = ['t.sha256sum', 'test_file_1.txt', 'test_file_2.txt']
            with patch('desitransfer.daemon.log') as l:
                o = verify_checksum(c)
        self.assertEqual(o, 0)
        l.debug.assert_has_calls([call("%s is valid.", os.path.join(d, 'test_file_1.txt')),
                                  call("%s is valid.", os.path.join(d, 'test_file_2.txt'))])
        #
        # Wrong number of files.
        #
        with patch('os.listdir') as mock_listdir:
            mock_listdir.return_value = ['t.sha256sum', 'test_file_1.txt']
            with patch('desitransfer.daemon.log') as l:
                o = verify_checksum(c)
        self.assertEqual(o, -1)
        l.error.assert_has_calls([call("%s does not match the number of files!", c)])
        #
        # Bad list of files.
        #
        with patch('os.listdir') as mock_listdir:
            mock_listdir.return_value = ['t.sha256sum', 'test_file_1.txt', 'test_file_3.txt']
            with patch('desitransfer.daemon.log') as l:
                o = verify_checksum(c)
        self.assertEqual(o, 1)
        l.debug.assert_has_calls([call("%s is valid.", os.path.join(d, 'test_file_1.txt'))])
        l.error.assert_has_calls([call("%s does not appear in %s!", os.path.join(d, 'test_file_3.txt'), c)])
        #
        # Hack hashlib to produce incorrect checksums.
        #
        with patch('os.listdir') as mock_listdir:
            mock_listdir.return_value = ['t.sha256sum', 'test_file_1.txt', 'test_file_2.txt']
            with patch('desitransfer.daemon.log') as l:
                with patch('hashlib.sha256') as h:
                    # h.sha256 = MagicMock()
                    h.hexdigest.return_value = 'abcdef'
                    o = verify_checksum(c)
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

    @patch('desitransfer.daemon.backup_night')
    @patch('desitransfer.daemon.catchup_night')
    @patch('desitransfer.daemon.dt')
    @patch('desitransfer.daemon.yesterday')
    @patch('desitransfer.daemon.transfer_exposure')
    @patch('desitransfer.daemon._popen')
    @patch('desitransfer.daemon.TransferStatus')
    @patch('desitransfer.daemon.log')
    def test_transfer_directory(self, mock_log, mock_status, mock_popen, mock_exposure, mock_yst, mock_dt, mock_catchup, mock_backup):
        """Test transfer for an entire configured directory.
        """
        mock_yst.return_value = '20190703'
        mock_dt.datetime.utcnow.return_value = datetime.datetime(2019, 7, 3, 21, 0, 0)
        links1 = """20190702/00000123
20190702/00000124
20190703/00000125
20190703/00000126
20190703/00000127
"""
        links2 = "\n"
        with patch.dict('os.environ',
                        {'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug']):
                c = _config()
                options = _options()
                pipeline = PipelineCommand(options.nersc, ssh=options.ssh)
                mock_popen.return_value = ('0', links1, '')
                transfer_directory(c[0], options, pipeline)
                mock_status.assert_called_once_with(os.path.join(os.path.dirname(c[0].staging), 'status'))
                mock_popen.assert_called_once_with(['/bin/ssh', '-q', 'dts', '/bin/find', c[0].source, '-type', 'l'])
                mock_catchup.assert_called_once_with(c[0], '20190703', False)
                mock_backup.assert_called_once_with(c[0], '20190703', mock_status(), False)
                #
                # No links.
                #
                mock_popen.return_value = ('0', links2, '')
                transfer_directory(c[0], options, pipeline)
                mock_log.warning.assert_has_calls([call('No links found, check connection.')])

    @patch('shutil.move')
    @patch('os.chmod')
    @patch('os.makedirs')
    @patch('os.path.exists')
    @patch('os.path.isdir')
    @patch('desitransfer.daemon.verify_checksum')
    @patch('desitransfer.daemon.lock_directory')
    @patch('desitransfer.daemon._popen')
    @patch('desitransfer.daemon.TransferStatus')
    @patch('desitransfer.daemon.log')
    def test_transfer_exposure(self, mock_log, mock_status, mock_popen, mock_lock, mock_cksum, mock_isdir, mock_exists, mock_mkdir, mock_chmod, mock_mv):
        """Test transfer of a single exposure.
        """
        desi_night = os.path.join(os.environ['HOME'], 'bin', 'wrap_desi_night.sh')
        with patch.dict('os.environ',
                        {'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug']):
                c = _config()
                options = _options()
                pipeline = PipelineCommand(options.nersc, ssh=options.ssh)
                #
                # Already transferred
                #
                mock_isdir.return_value = True
                transfer_exposure(c[0], options, '20190703/00000127', mock_status, pipeline)
                mock_log.debug.assert_has_calls([call('%s already transferred.', '/desi/root/spectro/staging/raw/20190703/00000127')])
                #
                # rsync error bypasses a lot of code.
                #
                mock_isdir.return_value = False
                mock_popen.return_value = ('1', '', '')
                transfer_exposure(c[0], options, '20190703/00000127', mock_status, pipeline)
                mock_log.debug.assert_has_calls([call("os.makedirs('%s', exist_ok=True)", '/desi/root/spectro/staging/raw/20190703')])
                mock_mkdir.assert_called_once_with('/desi/root/spectro/staging/raw/20190703', exist_ok=True)
                mock_popen.assert_called_once_with(['/bin/rsync', '--verbose', '--recursive',
                                                    '--copy-dirlinks', '--times', '--omit-dir-times',
                                                    'dts:/data/dts/exposures/raw/20190703/00000127/', '/desi/root/spectro/staging/raw/20190703/00000127/'])
                mock_log.error.assert_called_once_with('rsync problem detected!')
                mock_status.update.assert_called_once_with('20190703', '00000127', 'rsync', failure=True)
                #
                # Actually run the pipeline
                #
                mock_isdir.return_value = False
                mock_exists.return_value = True
                mock_popen.return_value = ('0', '', '')
                mock_cksum.return_value = 0
                transfer_exposure(c[0], options, '20190703/00000127', mock_status, pipeline)
                mock_lock.assert_called_once_with('/desi/root/spectro/staging/raw/20190703/00000127', options.shadow)
                mock_log.debug.assert_has_calls([call("os.makedirs('%s', exist_ok=True)", '/desi/root/spectro/data/20190703'),
                                                         call("os.chmod('%s', 0o%o)", '/desi/root/spectro/data/20190703', 0o2750)])
                mock_mkdir.assert_has_calls([call('/desi/root/spectro/data/20190703', exist_ok=True)])
                mock_chmod.assert_called_once_with('/desi/root/spectro/data/20190703', 0o2750)
                mock_cksum.assert_called_once_with('/desi/root/spectro/staging/raw/20190703/00000127/checksum-20190703-00000127.sha256sum')
                mock_mv.assert_called_once_with('/desi/root/spectro/staging/raw/20190703/00000127', '/desi/root/spectro/data/20190703')
                mock_popen.assert_has_calls([call(['/bin/ssh', '-q', 'cori', desi_night, 'update', '--night', '20190703', '--expid', '00000127', '--nersc', 'cori', '--nersc_queue', 'realtime', '--nersc_maxnodes', '25']),
                                             call(['/bin/ssh', '-q', 'cori', desi_night, 'flats', '--night', '20190703', '--expid', '00000127', '--nersc', 'cori', '--nersc_queue', 'realtime', '--nersc_maxnodes', '25']),
                                             call(['/bin/ssh', '-q', 'cori', desi_night, 'arcs', '--night', '20190703', '--expid', '00000127', '--nersc', 'cori', '--nersc_queue', 'realtime', '--nersc_maxnodes', '25']),
                                             call(['/bin/ssh', '-q', 'cori', desi_night, 'redshifts', '--night', '20190703', '--expid', '00000127', '--nersc', 'cori', '--nersc_queue', 'realtime', '--nersc_maxnodes', '25'])])
            #
            # Shadow mode will trigger main code body
            #
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug', '--shadow']):
                c = _config()
                options = _options()
                pipeline = PipelineCommand(options.nersc, ssh=options.ssh)
                #
                # Not already transferred, checksum file does not exist.
                #
                mock_isdir.return_value = False
                mock_exists.return_value = False
                transfer_exposure(c[0], options, '20190703/00000127', mock_status, pipeline)
                mock_status.update.assert_has_calls([call('20190703', '00000127', 'rsync'),
                                                     call('20190703', '00000127', 'checksum', failure=True)])
                mock_log.debug.assert_has_calls([call("%s does not exist, ignore checksum error.", '/desi/root/spectro/staging/raw/20190703/00000127')])
                mock_log.error.assert_has_calls([call("Checksum problem detected for %s/%s!", '20190703', '00000127')])
                #
                # Not already transferred, checksum file does exist.
                #
                mock_exists.return_value = True
                mock_cksum.return_value = 0
                transfer_exposure(c[0], options, '20190703/00000127', mock_status, pipeline)
                mock_exists.assert_has_calls([call('/desi/root/spectro/data/20190703/00000127/flats-20190703-00000127.done'),
                                              call('/desi/root/spectro/data/20190703/00000127/arcs-20190703-00000127.done'),
                                              call('/desi/root/spectro/data/20190703/00000127/science-20190703-00000127.done')])
                mock_status.update.assert_has_calls([call('20190703', '00000127', 'checksum'),
                                                     call('20190703', '00000127', 'pipeline'),
                                                     call('20190703', '00000127', 'pipeline', last='flats'),
                                                     call('20190703', '00000127', 'pipeline', last='arcs'),
                                                     call('20190703', '00000127', 'pipeline', last='science')])
                mock_log.debug.assert_has_calls([call("os.makedirs('%s', exist_ok=True)", '/desi/root/spectro/data/20190703'),
                                                 call("os.chmod('%s', 0o%o)", '/desi/root/spectro/data/20190703', 0o2750),
                                                 call("shutil.move('%s', '%s')", '/desi/root/spectro/staging/raw/20190703/00000127', '/desi/root/spectro/data/20190703')])
                # mock_popen.assert_has_calls([call(['/bin/ssh', '-q', 'cori', 'wrap_desi_night.sh'])])
            #
            # No-pipeline mode.
            #
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug', '--no-pipeline']):
                c = _config()
                options = _options()
                pipeline = PipelineCommand(options.nersc, ssh=options.ssh)
                mock_isdir.return_value = False
                mock_exists.return_value = True
                mock_popen.return_value = ('0', '', '')
                mock_cksum.return_value = 0
                transfer_exposure(c[0], options, '20190703/00000127', mock_status, pipeline)
                mock_log.info.assert_has_calls([call("%s/%s appears to be test data. Skipping pipeline activation.", '20190703', '00000127')])

    @patch('os.path.isdir')
    @patch('desitransfer.daemon.verify_checksum')
    @patch('desitransfer.daemon._popen')
    @patch('desitransfer.daemon.TransferStatus')
    @patch('desitransfer.daemon.log')
    def test_transfer_exposure_real_files(self, mock_log, mock_status, mock_popen, mock_cksum, mock_isdir):
        """Test single exposure files with files that actually exist.
        """
        with TemporaryDirectory() as desi_root:
            os.makedirs(os.path.join(desi_root, 'spectro', 'staging', 'raw', '20190703', '00000127'))
            os.makedirs(os.path.join(desi_root, 'spectro', 'data'))
            # with open(os.path.join(desi_root, 'spectro', 'staging', 'raw', '20190703', '00000127', 'checksum-20190703-00000127.sha256sum'), 'w') as s:
            #     s.write('foo')
            with patch.dict('os.environ',
                            {'DESI_ROOT': desi_root,
                             'DESI_SPECTRO_DATA': os.path.join(desi_root, 'spectro', 'data')}):
                with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug', '--shadow']):
                    c = _config()
                    options = _options()
                    pipeline = PipelineCommand(options.nersc, ssh=options.ssh)
                    mock_popen.return_value = ('0', '', '')
                    mock_cksum.return_value = 0
                    mock_isdir.return_value = False
                    transfer_exposure(c[0], options, '20190703/00000127', mock_status, pipeline)
                    mock_log.warning.assert_has_calls([call("No checksum file for %s/%s!", '20190703', '00000127')])
                    mock_log.info.assert_has_calls([call("%s/%s appears to be test data. Skipping pipeline activation.", '20190703', '00000127')])
                    # mock_log.debug.assert_has_calls([call('%s already transferred.', desi_root + '/spectro/staging/raw/20190703/00000127')])

    @patch('desitransfer.daemon.rsync_night')
    @patch('desitransfer.daemon._popen')
    @patch('os.path.exists')
    @patch('os.path.isdir')
    @patch('desitransfer.daemon.log')
    def test_catchup_night(self, mock_log, mock_isdir, mock_exists, mock_popen, mock_rsync):
        """Test morning catch-up pass.
        """
        r0 = """receiving incremental file list

sent 765 bytes  received 238,769 bytes  159,689.33 bytes/sec
total size is 118,417,836,324  speedup is 494,367.55
"""
        r1 = """receiving incremental file list
foo/bar.txt

sent 765 bytes  received 238,769 bytes  159,689.33 bytes/sec
total size is 118,417,836,324  speedup is 494,367.55
"""
        with TemporaryDirectory() as cscratch:
            with patch.dict('os.environ',
                            {'CSCRATCH': cscratch,
                             'DESI_ROOT': '/desi/root',
                             'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
                c = _config()
                mock_isdir.return_value = False
                catchup_night(c[0], '20190703', True)
                mock_isdir.assert_called_once_with('/desi/root/spectro/data/20190703')
                mock_log.warning.assert_has_calls([call("No data from %s detected, skipping catch-up transfer.", '20190703')])
                mock_isdir.return_value = True
                mock_exists.return_value = True
                catchup_night(c[0], '20190703', True)
                sync_file = os.path.join(cscratch, 'ketchup__desi_root_spectro_data_20190703.shadow.txt')
                mock_exists.assert_called_once_with(sync_file)
                mock_log.debug.assert_has_calls([call("%s detected, catch-up transfer is done.", sync_file)])
                mock_exists.return_value = False
                mock_popen.return_value = ('0', r0, '')
                catchup_night(c[0], '20190703', False)
                self.assertTrue(os.path.isfile(os.path.join(cscratch, 'ketchup__desi_root_spectro_data_20190703.txt')))
                mock_log.info.assert_has_calls([call('No files appear to have changed in %s.', '20190703')])
                mock_popen.return_value = ('0', r1, '')
                catchup_night(c[0], '20190703', False)
                mock_rsync.assert_called_once_with('/data/dts/exposures/raw', '/desi/root/spectro/data', '20190703', False)
                mock_log.warning.assert_has_calls([call('New files detected in %s!', '20190703')])

    @patch('desitransfer.daemon.rsync_night')
    @patch('desitransfer.daemon.TransferStatus')
    @patch('os.chdir')
    @patch('os.getcwd')
    @patch('desitransfer.daemon.empty_rsync')
    @patch('desitransfer.daemon._popen')
    @patch('os.remove')
    @patch('os.path.isdir')
    @patch('desitransfer.daemon.log')
    def test_backup_night(self, mock_log, mock_isdir, mock_rm, mock_popen, mock_empty, mock_getcwd, mock_chdir, mock_status, mock_rsync):
        """Test HPSS backup of nights.
        """
        fake_hsi1 = """desi_spectro_data_20190703.tar
desi_spectro_data_20190703.tar.idx
"""
        fake_hsi2 = """desi_spectro_data_20190702.tar
desi_spectro_data_20190702.tar.idx
"""
        with TemporaryDirectory() as cscratch:
            with patch.dict('os.environ',
                            {'CSCRATCH': cscratch,
                             'DESI_ROOT': '/desi/root',
                             'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
                c = _config()
                #
                # No data
                #
                mock_isdir.return_value = False
                backup_night(c[0], '20190703', mock_status, True)
                mock_isdir.assert_called_once_with('/desi/root/spectro/data/20190703')
                mock_log.warning.assert_has_calls([call("No data from %s detected, skipping HPSS backup.", '20190703')])
                mock_isdir.return_value = True
                #
                # Already backed up
                #
                ls_file = os.path.join(cscratch, 'desi_spectro_data.shadow.txt')
                mock_popen.return_value = ('0', '', '')
                with open(ls_file, 'w') as f:
                    f.write(fake_hsi1)
                backup_night(c[0], '20190703', mock_status, True)
                mock_log.debug.assert_has_calls([call("os.remove('%s')", ls_file),
                                                 call("Backup of %s already complete.", '20190703')])
                mock_rm.assert_called_once_with(ls_file)
                #
                # Not yet backed up
                #
                with open(ls_file, 'w') as f:
                    f.write(fake_hsi2)
                mock_empty.return_value = True
                mock_getcwd.return_value = cscratch
                backup_night(c[0], '20190703', mock_status, True)
                mock_chdir.assert_has_calls([call('/desi/root/spectro/data'),
                                             call(cscratch)])
                mock_log.info.assert_has_calls([call('No files appear to have changed in %s.', '20190703')])
                mock_log.debug.assert_has_calls([call("os.remove('%s')", ls_file),
                                                 call("os.chdir('%s')", '/desi/root/spectro/data'),
                                                 call('/usr/common/mss/bin/htar -cvhf desi/spectro/data/desi_spectro_data_20190703.tar -H crc:verify=all 20190703'),
                                                 call("os.chdir('%s')", cscratch)])
                mock_status.update.assert_called_once_with('20190703', 'all', 'backup')
                #
                # Not yet backed up and not test
                #
                ls_file = ls_file.replace('.shadow.txt', '.txt')
                with open(ls_file, 'w') as f:
                    f.write(fake_hsi2)
                backup_night(c[0], '20190703', mock_status, False)
                mock_popen.assert_has_calls([call(['/usr/common/mss/bin/htar', '-cvhf', 'desi/spectro/data/desi_spectro_data_20190703.tar', '-H', 'crc:verify=all', '20190703'])])
                #
                # Not yet backed up and delayed data
                #
                ls_file = ls_file.replace('.shadow.txt', '.txt')
                with open(ls_file, 'w') as f:
                    f.write(fake_hsi2)
                mock_empty.return_value = False
                backup_night(c[0], '20190703', mock_status, False)
                mock_log.warning.assert_has_calls([call('New files detected in %s!', '20190703')])
                mock_rsync.assert_called_with('/data/dts/exposures/raw', '/desi/root/spectro/data', '20190703', False)


def test_suite():
    """Allows testing of only this module with the command::

        python setup.py test -m <modulename>
    """
    return unittest.defaultTestLoader.loadTestsFromName(__name__)
