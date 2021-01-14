# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test desitransfer.daemon.
"""
import datetime
import logging
import os
import shutil
import sys
import unittest
from tempfile import TemporaryDirectory
from unittest.mock import call, patch, MagicMock
from pkg_resources import resource_filename
from ..daemon import (_options, TransferDaemon, _popen, log,
                      verify_checksum, lock_directory, unlock_directory,
                      rsync_night)


class TestDaemon(unittest.TestCase):
    """Test desitransfer.daemon.
    """

    @classmethod
    def setUpClass(cls):
        cls.fake_hsi1 = """desi_spectro_data_20190703.tar
desi_spectro_data_20190703.tar.idx
"""
        cls.fake_hsi2 = """desi_spectro_data_20190702.tar
desi_spectro_data_20190702.tar.idx
"""

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
        with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug']):
            options = _options()
            self.assertTrue(options.debug)
            self.assertEqual(options.kill,
                             os.path.join(os.environ['HOME'],
                                          'stop_desi_transfer'))

    @patch.object(TransferDaemon, '_configure_log')
    def test_TransferDaemon_init(self, mock_cl):
        """Test reading configuration file.
        """
        with patch.dict('os.environ',
                        {'CSCRATCH': self.tmp.name,
                         'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug']):
                options = _options()
            d = TransferDaemon(options)
        mock_cl.assert_called_once_with(True)
        self.assertEqual(d.sections, ['DESI_SPECTRO_DATA'])
        c = d.conf[d.sections[0]]
        self.assertEqual(c['destination'], '/desi/root/spectro/data')
        self.assertEqual(c['staging'], '/desi/root/spectro/staging/raw')
        self.assertEqual(d.directories[0].destination, '/desi/root/spectro/data')
        self.assertEqual(d.directories[0].staging, '/desi/root/spectro/staging/raw')

    @patch.object(TransferDaemon, '_configure_log')
    def test_TransferDaemon_alternate_init(self, mock_cl):
        """Test reading configuration with an alternate file.
        """
        with TemporaryDirectory() as config:
            ini = os.path.join(config, 'foo.ini')
            shutil.copy(TransferDaemon._default_configuration,
                        ini)
            with patch.dict('os.environ',
                            {'CSCRATCH': self.tmp.name,
                             'DESI_ROOT': '/desi/root',
                             'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
                with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug', '--configuration', ini]):
                    options = _options()
                d = TransferDaemon(options)
        self.assertEqual(d.sections, ['DESI_SPECTRO_DATA'])
        c = d.conf[d.sections[0]]
        self.assertEqual(c['destination'], '/desi/root/spectro/data')
        self.assertEqual(c['staging'], '/desi/root/spectro/staging/raw')
        self.assertEqual(d.directories[0].destination, '/desi/root/spectro/data')
        self.assertEqual(d.directories[0].staging, '/desi/root/spectro/staging/raw')

    @patch('desitransfer.daemon.SMTPHandler')
    @patch('desitransfer.daemon.RotatingFileHandler')
    @patch('desitransfer.daemon.get_logger')
    @patch('desitransfer.daemon.log')  # Needed to restore the module-level log object after test.
    def test_TransferDaemon_configure_log(self, mock_log, gl, rfh, smtp):
        """Test logging configuration.
        """
        with patch.dict('os.environ',
                        {'CSCRATCH': self.tmp.name,
                         'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug']):
                options = _options()
            d = TransferDaemon(options)
        rfh.assert_called_once_with('/desi/root/spectro/staging/logs/desi_transfer_daemon.log',
                                    backupCount=100, maxBytes=100000000)
        gl.assert_called_once_with(timestamp=True)
        gl().setLevel.assert_called_once_with(logging.DEBUG)

    @patch.object(TransferDaemon, 'checksum_lock')
    @patch.object(TransferDaemon, 'directory')
    @patch('desitransfer.daemon.log')
    @patch.object(TransferDaemon, '_configure_log')
    def test_TransferDaemon_transfer(self, mock_cl, mock_log, mock_dir, mock_lock):
        """Test loop over all configured directories.
        """
        with patch.dict('os.environ',
                        {'CSCRATCH': self.tmp.name,
                         'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug']):
                options = _options()
            d = TransferDaemon(options)
        mock_lock.return_value = True
        d.transfer()
        try:
            mock_lock.assert_called_once()
        except AttributeError:
            # Python 3.5 doesn't have this.
            pass
        mock_lock.return_value = False
        d.transfer()
        mock_log.info.assert_called_once_with('Looking for new data in %s.',
                                              d.directories[0].source)
        mock_dir.assert_called_once_with(d.directories[0])
        mock_dir.side_effect = Exception('Test Exception')
        d.transfer()
        try:
            mock_log.critical.assert_called_once()
        except AttributeError:
            # Python 3.5 doesn't have this.
            pass

    @patch('desitransfer.daemon._popen')
    @patch('desitransfer.daemon.log')
    @patch.object(TransferDaemon, '_configure_log')
    def test_TransferDaemon_checksum_lock(self, mock_cl, mock_log, mock_popen):
        """Test checksum locking mechanism.
        """
        with patch.dict('os.environ',
                        {'CSCRATCH': self.tmp.name,
                         'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug']):
                options = _options()
            d = TransferDaemon(options)
        mock_popen.return_value = ('0', '/tmp/checksum-running', '')
        self.assertTrue(d.checksum_lock())
        mock_log.info.assert_called_once_with('Checksums are being computed at KPNO.')
        mock_popen.return_value = ('2', '', 'No such file.')
        self.assertFalse(d.checksum_lock())

    @patch.object(TransferDaemon, 'backup')
    @patch.object(TransferDaemon, 'catchup')
    @patch.object(TransferDaemon, 'exposure')
    @patch('desitransfer.daemon.dt')
    @patch('desitransfer.daemon.yesterday')
    @patch('desitransfer.daemon._popen')
    @patch('desitransfer.daemon.TransferStatus')
    @patch('desitransfer.daemon.log')
    @patch.object(TransferDaemon, '_configure_log')
    def test_TransferDaemon_directory(self, mock_cl, mock_log, mock_status, mock_popen, mock_yst, mock_dt, mock_exposure, mock_catchup, mock_backup):
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
        links3 = "20190702/00000123.tmp\n20190702/0000012\n"
        with patch.dict('os.environ',
                        {'CSCRATCH': self.tmp.name,
                         'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug']):
                options = _options()
            transfer = TransferDaemon(options)
        c = transfer.directories
        mock_popen.return_value = ('0', links1, '')
        mock_backup.return_value = True
        transfer.directory(c[0])
        mock_status.assert_called_once_with(os.path.join(os.path.dirname(c[0].staging), 'status'))
        mock_popen.assert_called_once_with(['/bin/ssh', '-q', 'dts', '/bin/find', c[0].source, '-type', 'l'])
        mock_catchup.assert_called_once_with(c[0], '20190703', mock_status())
        mock_backup.assert_called_once_with(c[0], '20190703', mock_status())
        mock_status().update.assert_called_once_with('20190703', 'all', 'backup')
        #
        # No links.
        #
        mock_popen.return_value = ('0', links2, '')
        transfer.directory(c[0])
        mock_log.warning.assert_has_calls([call('No links found, check connection.')])
        #
        # Malformed links.
        #
        mock_popen.return_value = ('0', links3, '')
        transfer.directory(c[0])
        mock_log.warning.assert_has_calls([call('No links found, check connection.'),
                                           call('Malformed symlink detected: %s. Skipping.', '20190702/0000012'),
                                           call('Malformed symlink detected: %s. Skipping.', '20190702/00000123.tmp')])

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
    @patch.object(TransferDaemon, '_configure_log')
    def test_TransferDaemon_exposure_already_transferred(self, mock_cl, mock_log, mock_status, mock_popen, mock_lock, mock_cksum, mock_isdir, mock_exists, mock_mkdir, mock_chmod, mock_mv):
        """Test single exposure that already exists.
        """
        with patch.dict('os.environ',
                        {'CSCRATCH': self.tmp.name,
                         'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug']):
                options = _options()
            transfer = TransferDaemon(options)
        c = transfer.directories
        mock_isdir.return_value = True
        transfer.exposure(c[0], '20190703/00000127', mock_status)
        mock_log.debug.assert_called_once_with('%s already transferred.', '/desi/root/spectro/staging/raw/20190703/00000127')

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
    @patch.object(TransferDaemon, '_configure_log')
    def test_TransferDaemon_exposure_transfer(self, mock_cl, mock_log, mock_status, mock_popen, mock_lock, mock_cksum, mock_isdir, mock_exists, mock_mkdir, mock_chmod, mock_mv):
        """Test normal transfer of a single exposure.
        """
        with patch.dict('os.environ',
                        {'CSCRATCH': self.tmp.name,
                         'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug']):
                options = _options()
            transfer = TransferDaemon(options)
        c = transfer.directories
        #
        # Already transferred
        #
        mock_isdir.return_value = False
        mock_popen.return_value = ('0', '', '')
        mock_exists.return_value = True
        mock_cksum.return_value = 0
        transfer.exposure(c[0], '20190703/00000127', mock_status)
        mock_log.debug.assert_has_calls([call("os.makedirs('%s', exist_ok=True)", '/desi/root/spectro/staging/raw/20190703'),
                                         call("os.makedirs('%s', exist_ok=True)", '/desi/root/spectro/data/20190703'),
                                         call("os.chmod('%s', 0o%o)", '/desi/root/spectro/data/20190703', 1512),
                                         call('/bin/rsync --verbose --recursive --copy-dirlinks --times --omit-dir-times dts:/data/dts/exposures/raw/20190703/00000127/ /desi/root/spectro/staging/raw/20190703/00000127/'),
                                         call("status.update('%s', '%s', 'rsync')", '20190703', '00000127'),
                                         call("lock_directory('%s', %s)", '/desi/root/spectro/staging/raw/20190703/00000127', 'False'),
                                         call("verify_checksum('%s')", '/desi/root/spectro/staging/raw/20190703/00000127/checksum-00000127.sha256sum'),
                                         call("status.update('%s', '%s', 'checksum')", '20190703', '00000127'),
                                         call("shutil.move('%s', '%s')", '/desi/root/spectro/staging/raw/20190703/00000127', '/desi/root/spectro/data/20190703')])
        mock_mkdir.assert_has_calls([call('/desi/root/spectro/staging/raw/20190703', exist_ok=True),
                                     call('/desi/root/spectro/data/20190703', exist_ok=True)])
        mock_chmod.assert_has_calls([call('/desi/root/spectro/data/20190703', 1512)])
        mock_popen.assert_called_once_with(['/bin/rsync', '--verbose', '--recursive',
                                            '--copy-dirlinks', '--times', '--omit-dir-times',
                                            'dts:/data/dts/exposures/raw/20190703/00000127/',
                                            '/desi/root/spectro/staging/raw/20190703/00000127/'])
        mock_lock.assert_called_once_with('/desi/root/spectro/staging/raw/20190703/00000127', False)
        mock_exists.assert_has_calls([call('/desi/root/spectro/staging/raw/20190703/00000127/checksum-00000127.sha256sum'),])
        mock_cksum.assert_called_once_with('/desi/root/spectro/staging/raw/20190703/00000127/checksum-00000127.sha256sum')
        mock_status.update.assert_has_calls([call('20190703', '00000127', 'rsync'),
                                             call('20190703', '00000127', 'checksum')])
        mock_mv.assert_called_once_with('/desi/root/spectro/staging/raw/20190703/00000127', '/desi/root/spectro/data/20190703')

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
    @patch.object(TransferDaemon, '_configure_log')
    def test_TransferDaemon_exposure_transfer_testmode(self, mock_cl, mock_log, mock_status, mock_popen, mock_lock, mock_cksum, mock_isdir, mock_exists, mock_mkdir, mock_chmod, mock_mv):
        """Test normal transfer of a single exposure in test mode.
        """
        with patch.dict('os.environ',
                        {'CSCRATCH': self.tmp.name,
                         'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug', '--test']):
                options = _options()
            transfer = TransferDaemon(options)
        c = transfer.directories
        #
        # Already transferred
        #
        mock_isdir.return_value = False
        mock_popen.return_value = ('0', '', '')
        mock_exists.return_value = True
        mock_cksum.return_value = 0
        transfer.exposure(c[0], '20190703/00000127', mock_status)
        mock_log.debug.assert_has_calls([call("os.makedirs('%s', exist_ok=True)", '/desi/root/spectro/staging/raw/20190703'),
                                         call("os.makedirs('%s', exist_ok=True)", '/desi/root/spectro/data/20190703'),
                                         call("os.chmod('%s', 0o%o)", '/desi/root/spectro/data/20190703', 1512),
                                         call('/bin/rsync --verbose --recursive --copy-dirlinks --times --omit-dir-times dts:/data/dts/exposures/raw/20190703/00000127/ /desi/root/spectro/staging/raw/20190703/00000127/'),
                                         call("status.update('%s', '%s', 'rsync')", '20190703', '00000127'),
                                         call("lock_directory('%s', %s)", '/desi/root/spectro/staging/raw/20190703/00000127', 'True'),
                                         call("verify_checksum('%s')", '/desi/root/spectro/staging/raw/20190703/00000127/checksum-00000127.sha256sum'),
                                         # call("status.update('%s', '%s', 'checksum')", '20190703', '00000127'),
                                         call("shutil.move('%s', '%s')", '/desi/root/spectro/staging/raw/20190703/00000127', '/desi/root/spectro/data/20190703')])
        mock_mkdir.assert_not_called()
        mock_chmod.assert_not_called()
        mock_popen.assert_not_called()
        mock_lock.assert_called_once_with('/desi/root/spectro/staging/raw/20190703/00000127', True)
        mock_cksum.assert_not_called()
        mock_status.update.assert_not_called()
        mock_mv.assert_not_called()

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
    @patch.object(TransferDaemon, '_configure_log')
    def test_TransferDaemon_exposure_rsync_failure(self, mock_cl, mock_log, mock_status, mock_popen, mock_lock, mock_cksum, mock_isdir, mock_exists, mock_mkdir, mock_chmod, mock_mv):
        """Test transfer of a single exposure with an rsync failure.
        """
        with patch.dict('os.environ',
                        {'CSCRATCH': self.tmp.name,
                         'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug']):
                options = _options()
            transfer = TransferDaemon(options)
        c = transfer.directories
        #
        # Already transferred
        #
        mock_isdir.return_value = False
        mock_popen.return_value = ('1', 'stdout', 'stderr')
        mock_exists.return_value = True
        mock_cksum.return_value = 0
        transfer.exposure(c[0], '20190703/00000127', mock_status)
        mock_log.debug.assert_has_calls([call("os.makedirs('%s', exist_ok=True)", '/desi/root/spectro/staging/raw/20190703'),
                                         call("os.makedirs('%s', exist_ok=True)", '/desi/root/spectro/data/20190703'),
                                         call("os.chmod('%s', 0o%o)", '/desi/root/spectro/data/20190703', 1512),
                                         call('/bin/rsync --verbose --recursive --copy-dirlinks --times --omit-dir-times dts:/data/dts/exposures/raw/20190703/00000127/ /desi/root/spectro/staging/raw/20190703/00000127/'),
                                         call("status.update('%s', '%s', 'rsync', failure=True)", '20190703', '00000127'),
                                         call("lock_directory('%s', %s)", '/desi/root/spectro/staging/raw/20190703/00000127', 'False'),
                                         call("verify_checksum('%s')", '/desi/root/spectro/staging/raw/20190703/00000127/checksum-00000127.sha256sum'),
                                         call("status.update('%s', '%s', 'checksum')", '20190703', '00000127'),
                                         call("shutil.move('%s', '%s')", '/desi/root/spectro/staging/raw/20190703/00000127', '/desi/root/spectro/data/20190703')])
        mock_mkdir.assert_has_calls([call('/desi/root/spectro/staging/raw/20190703', exist_ok=True),
                                     call('/desi/root/spectro/data/20190703', exist_ok=True)])
        mock_chmod.assert_has_calls([call('/desi/root/spectro/data/20190703', 1512)])
        mock_lock.assert_called_once_with('/desi/root/spectro/staging/raw/20190703/00000127', False)
        mock_exists.assert_has_calls([call('/desi/root/spectro/staging/raw/20190703/00000127/checksum-00000127.sha256sum'),])
        mock_cksum.assert_called_once_with('/desi/root/spectro/staging/raw/20190703/00000127/checksum-00000127.sha256sum')
        mock_status.update.assert_has_calls([call('20190703', '00000127', 'rsync', failure=True),
                                             call('20190703', '00000127', 'checksum')])
        mock_mv.assert_called_once_with('/desi/root/spectro/staging/raw/20190703/00000127', '/desi/root/spectro/data/20190703')

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
    @patch.object(TransferDaemon, '_configure_log')
    def test_TransferDaemon_exposure_checksum_missing(self, mock_cl, mock_log, mock_status, mock_popen, mock_lock, mock_cksum, mock_isdir, mock_exists, mock_mkdir, mock_chmod, mock_mv):
        """Test normal transfer of a single exposure with missing checksum file.
        """
        with patch.dict('os.environ',
                        {'CSCRATCH': self.tmp.name,
                         'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug']):
                options = _options()
            transfer = TransferDaemon(options)
        c = transfer.directories
        #
        # Already transferred
        #
        mock_isdir.return_value = False
        mock_popen.return_value = ('0', '', '')
        mock_exists.return_value = False
        mock_cksum.return_value = 0
        transfer.exposure(c[0], '20190703/00000127', mock_status)
        mock_log.debug.assert_has_calls([call("os.makedirs('%s', exist_ok=True)", '/desi/root/spectro/staging/raw/20190703'),
                                         call("os.makedirs('%s', exist_ok=True)", '/desi/root/spectro/data/20190703'),
                                         call("os.chmod('%s', 0o%o)", '/desi/root/spectro/data/20190703', 1512),
                                         call('/bin/rsync --verbose --recursive --copy-dirlinks --times --omit-dir-times dts:/data/dts/exposures/raw/20190703/00000127/ /desi/root/spectro/staging/raw/20190703/00000127/'),
                                         call("status.update('%s', '%s', 'rsync')", '20190703', '00000127'),
                                         call("lock_directory('%s', %s)", '/desi/root/spectro/staging/raw/20190703/00000127', 'False'),
                                         call("verify_checksum('%s')", '/desi/root/spectro/staging/raw/20190703/00000127/checksum-00000127.sha256sum'),
                                         call("status.update('%s', '%s', 'checksum', failure=True)", '20190703', '00000127'),
                                         call("shutil.move('%s', '%s')", '/desi/root/spectro/staging/raw/20190703/00000127', '/desi/root/spectro/data/20190703')])
        mock_mkdir.assert_has_calls([call('/desi/root/spectro/staging/raw/20190703', exist_ok=True),
                                     call('/desi/root/spectro/data/20190703', exist_ok=True)])
        mock_chmod.assert_has_calls([call('/desi/root/spectro/data/20190703', 1512)])
        mock_popen.assert_called_once_with(['/bin/rsync', '--verbose', '--recursive',
                                            '--copy-dirlinks', '--times', '--omit-dir-times',
                                            'dts:/data/dts/exposures/raw/20190703/00000127/',
                                            '/desi/root/spectro/staging/raw/20190703/00000127/'])
        mock_lock.assert_called_once_with('/desi/root/spectro/staging/raw/20190703/00000127', False)
        mock_exists.assert_has_calls([call('/desi/root/spectro/staging/raw/20190703/00000127/checksum-00000127.sha256sum'),])
        # mock_cksum.assert_called_once_with('/desi/root/spectro/staging/raw/20190703/00000127/checksum-00000127.sha256sum')
        mock_log.warning.assert_called_once_with("No checksum file for %s/%s!", '20190703', '00000127')
        mock_status.update.assert_has_calls([call('20190703', '00000127', 'rsync'),
                                             call('20190703', '00000127', 'checksum', failure=True)])
        mock_mv.assert_called_once_with('/desi/root/spectro/staging/raw/20190703/00000127', '/desi/root/spectro/data/20190703')

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
    @patch.object(TransferDaemon, '_configure_log')
    def test_TransferDaemon_exposure_checksum_failure(self, mock_cl, mock_log, mock_status, mock_popen, mock_lock, mock_cksum, mock_isdir, mock_exists, mock_mkdir, mock_chmod, mock_mv):
        """Test normal transfer of a single exposure with bad checksum file.
        """
        with patch.dict('os.environ',
                        {'CSCRATCH': self.tmp.name,
                         'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug']):
                options = _options()
            transfer = TransferDaemon(options)
        c = transfer.directories
        #
        # Already transferred
        #
        mock_isdir.return_value = False
        mock_popen.return_value = ('0', '', '')
        mock_exists.return_value = True
        mock_cksum.return_value = 1
        transfer.exposure(c[0], '20190703/00000127', mock_status)
        mock_log.debug.assert_has_calls([call("os.makedirs('%s', exist_ok=True)", '/desi/root/spectro/staging/raw/20190703'),
                                         call("os.makedirs('%s', exist_ok=True)", '/desi/root/spectro/data/20190703'),
                                         call("os.chmod('%s', 0o%o)", '/desi/root/spectro/data/20190703', 1512),
                                         call('/bin/rsync --verbose --recursive --copy-dirlinks --times --omit-dir-times dts:/data/dts/exposures/raw/20190703/00000127/ /desi/root/spectro/staging/raw/20190703/00000127/'),
                                         call("status.update('%s', '%s', 'rsync')", '20190703', '00000127'),
                                         call("lock_directory('%s', %s)", '/desi/root/spectro/staging/raw/20190703/00000127', 'False'),
                                         call("verify_checksum('%s')", '/desi/root/spectro/staging/raw/20190703/00000127/checksum-00000127.sha256sum'),
                                         call("status.update('%s', '%s', 'checksum', failure=True)", '20190703', '00000127'),
                                         call("shutil.move('%s', '%s')", '/desi/root/spectro/staging/raw/20190703/00000127', '/desi/root/spectro/data/20190703')])
        mock_mkdir.assert_has_calls([call('/desi/root/spectro/staging/raw/20190703', exist_ok=True),
                                     call('/desi/root/spectro/data/20190703', exist_ok=True)])
        mock_chmod.assert_has_calls([call('/desi/root/spectro/data/20190703', 1512)])
        mock_popen.assert_called_once_with(['/bin/rsync', '--verbose', '--recursive',
                                            '--copy-dirlinks', '--times', '--omit-dir-times',
                                            'dts:/data/dts/exposures/raw/20190703/00000127/',
                                            '/desi/root/spectro/staging/raw/20190703/00000127/'])
        mock_lock.assert_called_once_with('/desi/root/spectro/staging/raw/20190703/00000127', False)
        mock_exists.assert_has_calls([call('/desi/root/spectro/staging/raw/20190703/00000127/checksum-00000127.sha256sum'),])
        # mock_cksum.assert_called_once_with('/desi/root/spectro/staging/raw/20190703/00000127/checksum-00000127.sha256sum')
        mock_log.critical.assert_called_once_with("Checksum problem detected for %s/%s, check logs!", '20190703', '00000127')
        mock_status.update.assert_has_calls([call('20190703', '00000127', 'rsync'),
                                             call('20190703', '00000127', 'checksum', failure=True)])
        mock_mv.assert_called_once_with('/desi/root/spectro/staging/raw/20190703/00000127', '/desi/root/spectro/data/20190703')

    @patch('os.path.isdir')
    @patch('desitransfer.daemon.TransferStatus')
    @patch('desitransfer.daemon.log')
    @patch.object(TransferDaemon, '_configure_log')
    def test_TransferDaemon_catchup_no_data(self, mock_cl, mock_log, mock_status, mock_isdir):
        """Test morning catch-up pass with no data for the night.
        """
        with patch.dict('os.environ',
                        {'CSCRATCH': self.tmp.name,
                         'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug', '--test']):
                options = _options()
            transfer = TransferDaemon(options)
        c = transfer.directories
        mock_isdir.return_value = False
        transfer.catchup(c[0], '20190703', mock_status)
        mock_isdir.assert_called_once_with('/desi/root/spectro/data/20190703')
        mock_log.warning.assert_called_once_with("No data from %s detected, skipping catch-up transfer.", '20190703')
        mock_status.assert_not_called()
        mock_status.update.assert_not_called()

    @patch('os.path.exists')
    @patch('os.path.isdir')
    @patch('desitransfer.daemon.TransferStatus')
    @patch('desitransfer.daemon.log')
    @patch.object(TransferDaemon, '_configure_log')
    def test_TransferDaemon_catchup_complete(self, mock_cl, mock_log, mock_status, mock_isdir, mock_exists):
        """Test morning catch-up pass after catch-up completion.
        """
        with patch.dict('os.environ',
                        {'CSCRATCH': self.tmp.name,
                         'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug', '--test']):
                options = _options()
            transfer = TransferDaemon(options)
        c = transfer.directories
        mock_isdir.return_value = True
        mock_exists.return_value = True
        transfer.catchup(c[0], '20190703', mock_status)
        sync_file = os.path.join(self.tmp.name, 'ketchup__desi_root_spectro_data_20190703.test.txt')
        mock_exists.assert_called_with(sync_file)
        mock_log.debug.assert_called_once_with("%s detected, catch-up transfer is done.", sync_file)
        mock_status.assert_not_called()
        mock_status.update.assert_not_called()

    @patch('desitransfer.daemon._popen')
    @patch('os.path.exists')
    @patch('os.path.isdir')
    @patch('desitransfer.daemon.TransferStatus')
    @patch('desitransfer.daemon.log')
    @patch.object(TransferDaemon, '_configure_log')
    def test_TransferDaemon_catchup_no_changes(self, mock_cl, mock_log, mock_status, mock_isdir, mock_exists, mock_popen):
        """Test morning catch-up pass with no new data detected.
        """
        r0 = """receiving incremental file list

sent 765 bytes  received 238,769 bytes  159,689.33 bytes/sec
total size is 118,417,836,324  speedup is 494,367.55
"""
        with patch.dict('os.environ',
                        {'CSCRATCH': self.tmp.name,
                         'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug']):
                options = _options()
            transfer = TransferDaemon(options)
        c = transfer.directories
        mock_isdir.return_value = True
        mock_exists.return_value = False
        mock_popen.return_value = ('0', r0, '')
        transfer.catchup(c[0], '20190703', mock_status)
        self.assertTrue(os.path.isfile(os.path.join(self.tmp.name, 'ketchup__desi_root_spectro_data_20190703.txt')))
        mock_log.info.assert_called_once_with('No files appear to have changed in %s.', '20190703')
        mock_status.assert_not_called()
        mock_status.update.assert_not_called()

    @patch('desitransfer.daemon._popen')
    @patch('os.path.exists')
    @patch('os.path.isdir')
    @patch('desitransfer.daemon.TransferStatus')
    @patch('desitransfer.daemon.log')
    @patch.object(TransferDaemon, '_configure_log')
    def test_TransferDaemon_catchup_no_changes_in_backup(self, mock_cl, mock_log, mock_status, mock_isdir, mock_exists, mock_popen):
        """Test morning catch-up pass with no new data detected at backup time.
        """
        r0 = """receiving incremental file list

sent 765 bytes  received 238,769 bytes  159,689.33 bytes/sec
total size is 118,417,836,324  speedup is 494,367.55
"""
        with patch.dict('os.environ',
                        {'CSCRATCH': self.tmp.name,
                         'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug']):
                options = _options()
            transfer = TransferDaemon(options)
        c = transfer.directories
        mock_isdir.return_value = True
        mock_exists.return_value = False
        mock_popen.return_value = ('0', r0, '')
        transfer.catchup(c[0], '20190703', mock_status, backup=True)
        self.assertTrue(os.path.isfile(os.path.join(self.tmp.name, 'backup__desi_root_spectro_data_20190703.txt')))
        mock_log.info.assert_called_once_with('No files appear to have changed in %s.', '20190703')
        mock_status.assert_not_called()
        mock_status.update.assert_not_called()

    @patch('desitransfer.daemon.rsync_night')
    @patch('desitransfer.daemon._popen')
    @patch('os.path.exists')
    @patch('os.path.isdir')
    @patch('desitransfer.daemon.TransferStatus')
    @patch('desitransfer.daemon.log')
    @patch.object(TransferDaemon, '_configure_log')
    def test_TransferDaemon_catchup_no_exposures(self, mock_cl, mock_log, mock_status, mock_isdir, mock_exists, mock_popen, mock_rsync):
        """Test morning catch-up pass with no new exposures detected.
        """
        r0 = """receiving incremental file list
foo/bar.txt

sent 765 bytes  received 238,769 bytes  159,689.33 bytes/sec
total size is 118,417,836,324  speedup is 494,367.55
"""
        with patch.dict('os.environ',
                        {'CSCRATCH': self.tmp.name,
                         'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug']):
                options = _options()
            transfer = TransferDaemon(options)
        c = transfer.directories
        mock_isdir.return_value = True
        mock_exists.return_value = False
        mock_popen.return_value = ('0', r0, '')
        transfer.catchup(c[0], '20190703', mock_status)
        mock_rsync.assert_called_once_with('/data/dts/exposures/raw', '/desi/root/spectro/data', '20190703', False)
        mock_log.warning.assert_has_calls([call('New files detected in %s!', '20190703'),
                                           call('No updated exposures in night %s detected.', '20190703')])
        mock_status.assert_not_called()
        mock_status.update.assert_not_called()


    @patch('desitransfer.daemon.rsync_night')
    @patch('desitransfer.daemon._popen')
    @patch('os.path.exists')
    @patch('os.path.isdir')
    @patch('desitransfer.daemon.TransferStatus')
    @patch('desitransfer.daemon.log')
    @patch.object(TransferDaemon, '_configure_log')
    def test_TransferDaemon_catchup(self, mock_cl, mock_log, mock_status, mock_isdir, mock_exists, mock_popen, mock_rsync):
        """Test morning catch-up pass.
        """
        r1 = """receiving incremental file list
00001234/bar.txt
00001235/foo.txt

sent 765 bytes  received 238,769 bytes  159,689.33 bytes/sec
total size is 118,417,836,324  speedup is 494,367.55
"""
        with patch.dict('os.environ',
                        {'CSCRATCH': self.tmp.name,
                         'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug']):
                options = _options()
            transfer = TransferDaemon(options)
        c = transfer.directories
        mock_isdir.return_value = True
        mock_exists.return_value = False
        mock_popen.return_value = ('0', r1, '')
        transfer.catchup(c[0], '20190703', mock_status)
        mock_rsync.assert_called_once_with('/data/dts/exposures/raw', '/desi/root/spectro/data', '20190703', False)
        mock_log.warning.assert_has_calls([call('New files detected in %s!', '20190703'),
                                           call("No checksum file for %s/%s!", '20190703', '00001234'),
                                           call("No checksum file for %s/%s!", '20190703', '00001235')],
                                           any_order=True)
        mock_log.debug.assert_has_calls([call("verify_checksum('%s')", '/desi/root/spectro/data/20190703/00001234/checksum-00001234.sha256sum'),
                                         call("status.update('%s', '%s', 'checksum', failure=True)", '20190703', '00001234'),
                                         call("verify_checksum('%s')", '/desi/root/spectro/data/20190703/00001235/checksum-00001235.sha256sum'),
                                         call("status.update('%s', '%s', 'checksum', failure=True)", '20190703', '00001235')],
                                         any_order=True)
        mock_status.update.assert_has_calls([call('20190703', '00001234', 'checksum', failure=True),
                                             call('20190703', '00001235', 'checksum', failure=True)],
                                             any_order=True)

    @patch('desitransfer.daemon.rsync_night')
    @patch('os.chdir')
    @patch('os.getcwd')
    @patch('desitransfer.daemon.empty_rsync')
    @patch('desitransfer.daemon._popen')
    @patch('os.remove')
    @patch('os.path.isdir')
    @patch('desitransfer.daemon.TransferStatus')
    @patch('desitransfer.daemon.log')
    @patch.object(TransferDaemon, '_configure_log')
    def test_TransferDaemon_backup_no_data(self, mock_cl, mock_log, mock_status, mock_isdir, mock_rm, mock_popen, mock_empty, mock_getcwd, mock_chdir, mock_rsync):
        """Test HPSS backup with no data for the night.
        """
        with patch.dict('os.environ',
                        {'CSCRATCH': self.tmp.name,
                         'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug', '--test']):
                options = _options()
            transfer = TransferDaemon(options)
        c = transfer.directories
        mock_isdir.return_value = False
        s = transfer.backup(c[0], '20190703', mock_status)
        self.assertFalse(s)
        mock_isdir.assert_called_once_with('/desi/root/spectro/data/20190703')
        mock_log.warning.assert_has_calls([call("No data from %s detected, skipping HPSS backup.", '20190703')])
        mock_status.assert_not_called()
        mock_status.update.assert_not_called()

    @patch('desitransfer.daemon.rsync_night')
    @patch('os.chdir')
    @patch('os.getcwd')
    @patch('desitransfer.daemon.empty_rsync')
    @patch('desitransfer.daemon._popen')
    @patch('os.remove')
    @patch('os.path.isdir')
    @patch('desitransfer.daemon.TransferStatus')
    @patch('desitransfer.daemon.log')
    @patch.object(TransferDaemon, '_configure_log')
    def test_TransferDaemon_backup_already_done(self, mock_cl, mock_log, mock_status, mock_isdir, mock_rm, mock_popen, mock_empty, mock_getcwd, mock_chdir, mock_rsync):
        """Test HPSS backup with night already done.
        """
        with patch.dict('os.environ',
                        {'CSCRATCH': self.tmp.name,
                         'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug', '--test']):
                options = _options()
            transfer = TransferDaemon(options)
        c = transfer.directories
        mock_isdir.return_value = True
        mock_popen.return_value = ('0', '', '')
        ls_file = os.path.join(self.tmp.name, 'desi_spectro_data.test.txt')
        with open(ls_file, 'w') as f:
            f.write(self.fake_hsi1)
        s = transfer.backup(c[0], '20190703', mock_status)
        self.assertFalse(s)
        mock_log.debug.assert_has_calls([call("os.remove('%s')", ls_file),
                                         call("Backup of %s already complete.", '20190703')])
        mock_rm.assert_called_once_with(ls_file)
        mock_status.assert_not_called()
        mock_status.update.assert_not_called()

    @patch('desitransfer.daemon.rsync_night')
    @patch('os.chdir')
    @patch('os.getcwd')
    @patch('desitransfer.daemon.empty_rsync')
    @patch('desitransfer.daemon._popen')
    @patch('os.remove')
    @patch('os.path.isdir')
    @patch('desitransfer.daemon.TransferStatus')
    @patch('desitransfer.daemon.log')
    @patch.object(TransferDaemon, '_configure_log')
    def test_TransferDaemon_backup_test(self, mock_cl, mock_log, mock_status, mock_isdir, mock_rm, mock_popen, mock_empty, mock_getcwd, mock_chdir, mock_rsync):
        """Test HPSS backup of night in 'test' mode.
        """
        with patch.dict('os.environ',
                        {'CSCRATCH': self.tmp.name,
                         'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug', '--test']):
                options = _options()
            transfer = TransferDaemon(options)
        c = transfer.directories
        mock_isdir.return_value = True
        mock_popen.return_value = ('0', '', '')
        ls_file = os.path.join(self.tmp.name, 'desi_spectro_data.test.txt')
        #
        # Not yet backed up
        #
        with open(ls_file, 'w') as f:
            f.write(self.fake_hsi2)
        mock_empty.return_value = True
        mock_getcwd.return_value = self.tmp.name
        mock_rm.side_effect = FileNotFoundError
        s = transfer.backup(c[0], '20190703', mock_status)
        self.assertTrue(s)
        mock_chdir.assert_has_calls([call('/desi/root/spectro/data'),
                                     call(self.tmp.name)])
        mock_log.info.assert_has_calls([call('No files appear to have changed in %s.', '20190703')])
        mock_log.debug.assert_has_calls([call("os.remove('%s')", ls_file),
                                         call("Failed to remove %s because it didn't exist. That's OK.", ls_file),
                                         call("os.chdir('%s')", '/desi/root/spectro/data'),
                                         call('/usr/common/mss/bin/htar -cvhf desi/spectro/data/desi_spectro_data_20190703.tar -H crc:verify=all 20190703'),
                                         call("os.chdir('%s')", self.tmp.name)])
        mock_status.assert_not_called()
        mock_status.update.assert_not_called()

    @patch('desitransfer.daemon.rsync_night')
    @patch('os.chdir')
    @patch('os.getcwd')
    @patch('desitransfer.daemon.empty_rsync')
    @patch('desitransfer.daemon._popen')
    @patch('os.remove')
    @patch('os.path.isdir')
    @patch('desitransfer.daemon.TransferStatus')
    @patch('desitransfer.daemon.log')
    @patch.object(TransferDaemon, '_configure_log')
    def test_TransferDaemon_backup_no_test(self, mock_cl, mock_log, mock_status, mock_isdir, mock_rm, mock_popen, mock_empty, mock_getcwd, mock_chdir, mock_rsync):
        """Test HPSS backup of night in 'real' mode.
        """
        with patch.dict('os.environ',
                        {'CSCRATCH': self.tmp.name,
                         'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug']):
                options = _options()
            transfer = TransferDaemon(options)
        c = transfer.directories
        mock_isdir.return_value = True
        mock_popen.return_value = ('0', '', '')
        ls_file = os.path.join(self.tmp.name, 'desi_spectro_data.txt')
        with open(ls_file, 'w') as f:
            f.write(self.fake_hsi2)
        s = transfer.backup(c[0], '20190703', mock_status)
        self.assertTrue(s)
        mock_popen.assert_has_calls([call(['/usr/common/mss/bin/htar', '-cvhf', 'desi/spectro/data/desi_spectro_data_20190703.tar', '-H', 'crc:verify=all', '20190703'])])
        mock_status.assert_not_called()
        mock_status.update.assert_not_called()

    @patch('desitransfer.daemon.rsync_night')
    @patch('os.chdir')
    @patch('os.getcwd')
    @patch('desitransfer.daemon.empty_rsync')
    @patch('desitransfer.daemon._popen')
    @patch('os.remove')
    @patch('os.path.isdir')
    @patch('desitransfer.daemon.TransferStatus')
    @patch('desitransfer.daemon.log')
    @patch.object(TransferDaemon, '_configure_log')
    def test_TransferDaemon_backup_delayed_data(self, mock_cl, mock_log, mock_status, mock_isdir, mock_rm, mock_popen, mock_empty, mock_getcwd, mock_chdir, mock_rsync):
        """Test HPSS backup of night with delayed data.
        """
        with patch.dict('os.environ',
                        {'CSCRATCH': self.tmp.name,
                         'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug']):
                options = _options()
            transfer = TransferDaemon(options)
        c = transfer.directories
        ls_file = os.path.join(self.tmp.name, 'desi_spectro_data.txt')
        with open(ls_file, 'w') as f:
            f.write(self.fake_hsi2)
        mock_empty.return_value = False
        mock_popen.return_value = ('0', '', '')
        mock_getcwd.return_value = 'HOME'
        s = transfer.backup(c[0], '20190703', mock_status)
        self.assertTrue(s)
        mock_log.debug.assert_has_calls([call("os.remove('%s')", os.path.join(self.tmp.name, 'desi_spectro_data.txt')),
                                         call("os.chdir('%s')", '/desi/root/spectro/data'),
                                         call("os.chdir('%s')", 'HOME')])
        mock_log.warning.assert_has_calls([call('New files detected in %s!', '20190703'),
                                           call('No updated exposures in night %s detected.', '20190703')])
        mock_rsync.assert_called_once_with('/data/dts/exposures/raw', '/desi/root/spectro/data', '20190703', False)
        mock_status.assert_not_called()
        mock_status.update.assert_not_called()

    @patch('desitransfer.daemon.rsync_night')
    @patch('desitransfer.daemon.empty_rsync')
    @patch('desitransfer.daemon._popen')
    @patch('os.remove')
    @patch('os.path.isdir')
    @patch('desitransfer.daemon.TransferStatus')
    @patch('desitransfer.daemon.log')
    @patch.object(TransferDaemon, '_configure_log')
    def test_TransferDaemon_no_backup(self, mock_cl, mock_log, mock_status, mock_isdir, mock_rm, mock_popen, mock_empty, mock_rsync):
        """Test disabling HPSS backups.
        """
        with patch.dict('os.environ',
                        {'CSCRATCH': self.tmp.name,
                         'DESI_ROOT': '/desi/root',
                         'DESI_SPECTRO_DATA': '/desi/root/spectro/data'}):
            with patch.object(sys, 'argv', ['desi_transfer_daemon', '--debug', '--test', '--no-backup']):
                options = _options()
            transfer = TransferDaemon(options)
        c = transfer.directories
        mock_isdir.return_value = True
        mock_popen.return_value = ('0', '', '')
        s = transfer.backup(c[0], '20190703', mock_status)
        self.assertTrue(s)
        mock_isdir.assert_has_calls([call('/desi/root/spectro/data/20190703'),
                                     call('/desi/root/spectro/data/20190703')])
        mock_log.info.assert_has_calls([call('Tape backup disabled by user request.')])
        mock_status.assert_not_called()
        mock_status.update.assert_not_called()

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
        l.error.assert_has_calls([call("%s lists %d file(s) that are not present!", c, 1)])
        with patch('os.listdir') as mock_listdir:
            mock_listdir.return_value = ['t.sha256sum', 'test_file_1.txt', 'test_file_2.txt', 'test_file_3.txt']
            with patch('desitransfer.daemon.log') as l:
                o = verify_checksum(c)
        self.assertEqual(o, 1)
        l.error.assert_has_calls([call("%d files are not listed in %s!", 1, c)])
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
        l.error.assert_has_calls([call("Checksum mismatch for %s in %s!", os.path.join(d, 'test_file_1.txt'), c),
                                  call("Checksum mismatch for %s in %s!", os.path.join(d, 'test_file_2.txt'), c)])

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
        mock_popen.return_value = ('1', 'stdout', 'stderr')
        rsync_night('/source', '/destination', '20190703')
        mock_log.critical.assert_called_once_with('rsync problem (status = %s) detected on catch-up for %s, check logs!',
                                                  '1', '20190703')
        mock_log.error.assert_has_calls([call('rsync STDOUT = \n%s', 'stdout'),
                                         call('rsync STDERR = \n%s', 'stderr')])


def test_suite():
    """Allows testing of only this module with the command::

        python setup.py test -m <modulename>
    """
    return unittest.defaultTestLoader.loadTestsFromName(__name__)
