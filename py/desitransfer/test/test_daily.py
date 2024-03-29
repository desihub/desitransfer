# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test desitransfer.daily.
"""
import os
import sys
import unittest
from unittest.mock import patch, call, mock_open, Mock
from ..daily import _config, _options, DailyDirectory
from .. import __version__ as dtVersion


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
            c = _config('noon')
            self.assertEqual(c[0].source, '/data/dts/exposures/lost+found')
            self.assertEqual(c[0].destination, os.path.join(os.environ['DESI_ROOT'],
                                                            'spectro', 'staging', 'lost+found'))
            self.assertTrue(c[0].dirlinks)
            self.assertFalse(c[1].dirlinks)

    def test_config_morning(self):
        """Test transfer directory configuration at a different time.
        """
        with patch.dict('os.environ',
                        {'DESI_ROOT': '/desi/root'}):
            c = _config('morning')
            self.assertEqual(c[0].source, '/software/www2/html/nightlogs')
            self.assertEqual(c[0].destination, os.path.join(os.environ['DESI_ROOT'],
                                                            'survey', 'ops', 'nightlogs'))
            self.assertFalse(c[0].dirlinks)

    def test_options(self):
        """Test command-line arguments.
        """
        with patch.dict('os.environ',
                        {'DESI_ROOT': '/desi/root'}):
            with patch.object(sys, 'argv',
                              ['desi_daily_transfer', '--debug', '--kill',
                               os.path.expanduser('~/stop_daily_transfer'),
                               'noon']):
                options = _options()
                self.assertTrue(options.permission)
                self.assertEqual(options.completion,
                                 '/desi/root/spectro/staging/status/daily.txt')
                self.assertTrue(options.debug)
                self.assertEqual(options.kill,
                                 os.path.join(os.environ['HOME'],
                                              'stop_daily_transfer'))

    @patch('os.walk')
    @patch('os.stat')
    @patch('os.chmod')
    @patch('subprocess.Popen')
    @patch('desitransfer.daily.stamp')
    @patch('builtins.open', new_callable=mock_open)
    def test_transfer(self, mo, mock_stamp, mock_popen, mock_chmod, mock_stat, mock_walk):
        """Test the transfer functions in DailyDirectory.transfer().
        """
        mock_walk.return_value = [('/dst/d0', [], ['f1', 'f2'])]
        mode = Mock()
        mode.st_mode = 137
        mock_stat.return_value = mode
        mock_stamp.return_value = '2019-07-03'
        mock_popen().wait.return_value = 0
        d = DailyDirectory('/src/d0', '/dst/d0')
        d.transfer()
        mo.assert_has_calls([call('/dst/d0.log', 'ab'),
                             call().__enter__(),
                             call().write(('DEBUG: desi_daily_transfer {}\n'.format(dtVersion)).encode('utf-8')),
                             call().write(b'DEBUG: /bin/rsync --verbose --recursive --links --times --omit-dir-times dts:/src/d0/ /dst/d0/\n'),
                             call().write(b'DEBUG: Transfer start: 2019-07-03\n'),
                             call().flush(),
                             call().write(b'DEBUG: Transfer complete: 2019-07-03\n'),
                             call().__exit__(None, None, None)])
        mock_walk.assert_called_once_with('/dst/d0')
        mock_chmod.assert_has_calls([call('/dst/d0', 0o2750),
                                     call('/dst/d0/f1', 0o0440),
                                     call('/dst/d0/f2', 0o0440)])

    @patch('os.walk')
    @patch('os.stat')
    @patch('os.chmod')
    @patch('subprocess.Popen')
    @patch('desitransfer.daily.stamp')
    @patch('builtins.open', new_callable=mock_open)
    def test_transfer_extra(self, mo, mock_stamp, mock_popen, mock_chmod, mock_stat, mock_walk):
        """Test the transfer functions in DailyDirectory.transfer() with extra options.
        """
        mock_walk.return_value = [('/dst/d0', [], ['f1', 'f2'])]
        mode = Mock()
        mode.st_mode = 137
        mock_stat.return_value = mode
        mock_stamp.return_value = '2019-07-03'
        mock_popen().wait.return_value = 0
        d = DailyDirectory('/src/d0', '/dst/d0', extra=['--exclude-from', 'foo'])
        d.transfer()
        mo.assert_has_calls([call('/dst/d0.log', 'ab'),
                             call().__enter__(),
                             call().write(('DEBUG: desi_daily_transfer {}\n'.format(dtVersion)).encode('utf-8')),
                             call().write(b'DEBUG: /bin/rsync --verbose --recursive --links --times --omit-dir-times --exclude-from foo dts:/src/d0/ /dst/d0/\n'),
                             call().write(b'DEBUG: Transfer start: 2019-07-03\n'),
                             call().flush(),
                             call().write(b'DEBUG: Transfer complete: 2019-07-03\n'),
                             call().__exit__(None, None, None)])
        mock_walk.assert_called_once_with('/dst/d0')
        mock_chmod.assert_has_calls([call('/dst/d0', 0o2750),
                                     call('/dst/d0/f1', 0o0440),
                                     call('/dst/d0/f2', 0o0440)])

    @patch('os.walk')
    @patch('os.stat')
    @patch('os.chmod')
    @patch('desitransfer.daily.stamp')
    @patch('builtins.open', new_callable=mock_open)
    def test_lock(self, mo, mock_stamp, mock_chmod, mock_stat, mock_walk):
        """Test the lock functions in DailyDirectory.lock().
        """
        mock_walk.return_value = [('/dst/d0', ['d1', 'd2'], ['f1', 'f2']),
                                  ('/dst/d0/d1', [], ['f3']),
                                  ('/dst/d0/d2', [], ['f4'])]
        mode = Mock()
        mode.st_mode = 137
        mock_stat.return_value = mode
        mock_stamp.return_value = '2019-07-03'
        d = DailyDirectory('/src/d0', '/dst/d0')
        d.lock()
        mo.assert_has_calls([call('/dst/d0.log', 'ab'),
                             call().__enter__(),
                             call().write(b'DEBUG: Lock complete: 2019-07-03\n'),
                             call().__exit__(None, None, None)])
        mock_walk.assert_called_once_with('/dst/d0')
        mock_chmod.assert_has_calls([call('/dst/d0', 0o2750),
                                     call('/dst/d0/f1', 0o0440),
                                     call('/dst/d0/f2', 0o0440),
                                     call('/dst/d0/d1', 0o2750),
                                     call('/dst/d0/d1/f3', 0o0440),
                                     call('/dst/d0/d2', 0o2750),
                                     call('/dst/d0/d2/f4', 0o0440)])

    @patch('subprocess.Popen')
    @patch('desitransfer.daily.stamp')
    @patch('builtins.open', new_callable=mock_open)
    def test_permission(self, mo, mock_stamp, mock_popen):
        """Test granting permissions.
        """
        mock_popen().wait.return_value = 0
        mock_stamp.return_value = '2019-07-03'
        d = DailyDirectory('/src/d0', '/dst/d0')
        d.permission()
        mo.assert_has_calls([call('/dst/d0.log', 'ab'),
                             call().__enter__(),
                             call().write(b'DEBUG: fix_permissions.sh /dst/d0\n'),
                             call().flush(),
                             call().write(b'DEBUG: Permission reset complete: 2019-07-03\n'),
                             call().__exit__(None, None, None)])
        mock_popen.assert_has_calls([call(),
                                     call(['fix_permissions.sh', '/dst/d0'],
                                          stdout=mo(), stderr=-2),
                                     call().wait()])
