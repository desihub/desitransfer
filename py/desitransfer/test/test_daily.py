# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test desitransfer.daily.
"""
import os
import sys
import unittest
from unittest.mock import patch, call, mock_open
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
            c = _config()
            self.assertEqual(c[0].source, '/exposures/desi/sps')
            self.assertEqual(c[0].destination, os.path.join(os.environ['DESI_ROOT'],
                                                            'engineering', 'spectrograph', 'sps'))

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

    @patch('os.walk')
    @patch('os.chmod')
    @patch('subprocess.Popen')
    @patch('desitransfer.daily.stamp')
    @patch('builtins.open', new_callable=mock_open)
    def test_transfer(self, mo, mock_stamp, mock_popen, mock_chmod, mock_walk):
        """Test the transfer functions in DailyDirectory.transfer().
        """
        mock_walk.return_value = [('/dst/d0', [], ['f1', 'f2'])]
        mock_stamp.return_value = '2019-07-03'
        mock_popen().wait.return_value = 0
        d = DailyDirectory('/src/d0', '/dst/d0')
        d.transfer()
        mo.assert_has_calls([call('/dst/d0.log', 'ab'),
                             call().__enter__(),
                             call().write(('DEBUG: desi_daily_transfer {}\n'.format(dtVersion)).encode('utf-8')),
                             call().write(b'DEBUG: 2019-07-03\n'),
                             call().write(b'DEBUG: /bin/rsync --verbose --recursive --copy-dirlinks --times --omit-dir-times dts:/src/d0/ /dst/d0/\n'),
                             call().flush(),
                             call().__exit__(None, None, None)])
        mock_walk.assert_called_once_with('/dst/d0')
        mock_chmod.assert_has_calls([call('/dst/d0', 1512),
                                     call('/dst/d0/f1', 288),
                                     call('/dst/d0/f2', 288)])

    @patch('os.walk')
    @patch('os.chmod')
    def test_lock(self, mock_chmod, mock_walk):
        """Test the lock functions in DailyDirectory.lock().
        """
        mock_walk.return_value = [('/dst/d0', ['d1', 'd2'], ['f1', 'f2']),
                                  ('/dst/d0/d1', [], ['f3']),
                                  ('/dst/d0/d2', [], ['f4'])]
        d = DailyDirectory('/src/d0', '/dst/d0')
        d.lock()
        mock_walk.assert_called_once_with('/dst/d0')
        mock_chmod.assert_has_calls([call('/dst/d0', 0o2750),
                                     call('/dst/d0/f1', 0o0440),
                                     call('/dst/d0/f2', 0o0440),
                                     call('/dst/d0/d1', 0o2750),
                                     call('/dst/d0/d1/f3', 0o0440),
                                     call('/dst/d0/d2', 0o2750),
                                     call('/dst/d0/d2/f4', 0o0440)])

    @patch('subprocess.Popen')
    @patch('builtins.open', new_callable=mock_open)
    def test_apache(self, mo, mock_popen):
        """Test granting apache/www permissions.
        """
        mock_popen().wait.return_value = 0
        d = DailyDirectory('/src/d0', '/dst/d0')
        d.apache()
        mo.assert_has_calls([call('/dst/d0.log', 'ab'),
                             call().__enter__(),
                             call().write(b'DEBUG: fix_permissions.sh -a /dst/d0\n'),
                             call().flush(),
                             call().__exit__(None, None, None)])
        mock_popen.assert_has_calls([call(),
                                     call(['fix_permissions.sh', '-a', '/dst/d0'],
                                          stdout=mo(), stderr=-2),
                                     call().wait()])


def test_suite():
    """Allows testing of only this module with the command::

        python setup.py test -m <modulename>
    """
    return unittest.defaultTestLoader.loadTestsFromName(__name__)
