# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test desitransfer.status.
"""
import json
import os
import shutil
import sys
import unittest
from unittest.mock import patch, call
from tempfile import TemporaryDirectory
from pkg_resources import resource_filename
from ..status import TransferStatus, _options


class TestStatus(unittest.TestCase):
    """Test desitransfer.status.
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

    def test_options(self):
        """Test command-line arguments.
        """
        with patch.dict('os.environ', {'DESI_ROOT': '/desi'}):
            with patch.object(sys, 'argv', ['desi_transfer_status', '20190703', '12345678', 'rsync']):
                options = _options()
                self.assertEqual(options.night, 20190703)
                self.assertEqual(options.expid, '12345678')
                self.assertEqual(options.stage, 'rsync')

    def test_TransferStatus_init(self):
        """Test status reporting mechanism setup.
        """
        h = resource_filename('desitransfer',
                              'data/desi_transfer_status.html')
        j = resource_filename('desitransfer',
                              'data/desi_transfer_status.js')
        #
        # Existing empty directory.
        #
        with TemporaryDirectory() as d:
            s = TransferStatus(d)
            self.assertEqual(s.directory, d)
            self.assertTrue(os.path.isdir(d))
            self.assertListEqual(os.listdir(d), [])
            self.assertListEqual(s.status, [])
        #
        # Directory with JSON file.
        #
        st = [[20200703, 12345678, 'rsync', True, '', 1565300074664],
              [20200703, 12345677, 'rsync', True, '', 1565300073000]]
        with TemporaryDirectory() as d:
            js = os.path.join(d, 'desi_transfer_status.json')
            with open(js, 'w') as f:
                json.dump(st, f, indent=None, separators=(',', ':'))
            s = TransferStatus(d)
            self.assertEqual(s.directory, d)
            self.assertTrue(os.path.isdir(d))
            self.assertListEqual(os.listdir(d), ['desi_transfer_status.json'])
            self.assertListEqual(s.status[0], st[0])
            self.assertListEqual(s.status[1], st[1])
        #
        # New directory.
        #
        d = '/desi/spectro/status'
        # with patch('desitransfer.status.log') as l:
        with patch('os.makedirs') as m:
            with patch('shutil.copy') as cp:
                with patch('shutil.copyfile') as cf:
                    s = TransferStatus(d)
        # l.debug.assert_called_once_with("os.makedirs('%s')", d)
        m.assert_called_once_with(d)
        cp.assert_called_once_with(j, d)
        cf.assert_called_once_with(h, os.path.join(d, 'index.html'))

    @patch('desitransfer.daemon.log')
    def test_TransferStatus_handle_malformed_with_log(self, mock_log):
        """Test handling of malformed JSON files.
        """
        bad = resource_filename('desitransfer.test', 't/bad.json')
        with TemporaryDirectory() as d:
            shutil.copy(bad, os.path.join(d, 'desi_transfer_status.json'))
            s = TransferStatus(d)
            self.assertTrue(os.path.exists(os.path.join(d, 'desi_transfer_status.json.bad')))
            self.assertListEqual(s.status, [])
            self.assertListEqual(os.listdir(d), ['desi_transfer_status.json.bad',
                                                 'desi_transfer_status.json'])
        mock_log.error.assert_called_once_with('Malformed JSON file detected: %s; saving original file as %s.',
                                               os.path.join(d, 'desi_transfer_status.json'),
                                               os.path.join(d, 'desi_transfer_status.json.bad'))
        mock_log.debug.assert_called_once_with("shutil.copy2('%s', '%s')",
                                               os.path.join(d, 'desi_transfer_status.json'),
                                               os.path.join(d, 'desi_transfer_status.json.bad'))
        mock_log.info.assert_called_once_with('Writing empty array to %s.',
                                              os.path.join(d, 'desi_transfer_status.json'))

    @patch('builtins.print')
    def test_TransferStatus_handle_malformed_without_log(self, mock_print):
        """Test handling of malformed JSON files (no log object).
        """
        bad = resource_filename('desitransfer.test', 't/bad.json')
        with TemporaryDirectory() as d:
            shutil.copy(bad, os.path.join(d, 'desi_transfer_status.json'))
            s = TransferStatus(d)
            self.assertTrue(os.path.exists(os.path.join(d, 'desi_transfer_status.json.bad')))
            self.assertListEqual(s.status, [])
            self.assertListEqual(os.listdir(d), ['desi_transfer_status.json.bad',
                                                 'desi_transfer_status.json'])
        mock_print.assert_has_calls([call('ERROR: Malformed JSON file detected: %s; saving original file as %s.' % (os.path.join(d, 'desi_transfer_status.json'),
                                                                                                                    os.path.join(d, 'desi_transfer_status.json.bad'))),
                                     call("DEBUG: shutil.copy2('%s', '%s')" % (os.path.join(d, 'desi_transfer_status.json'),
                                                                               os.path.join(d, 'desi_transfer_status.json.bad'))),
                                     call("INFO: Writing empty array to %s." % (os.path.join(d, 'desi_transfer_status.json'),))])

    @patch('time.time')
    def test_TransferStatus_update(self, mock_time):
        """Test status reporting mechanism updates.
        """
        mock_time.return_value = 1565300090
        st = [[20200703, 12345678, 'rsync', True, '', 1565300074664],
              [20200703, 12345677, 'rsync', True, '', 1565300073000]]
        with TemporaryDirectory() as d:
            js = os.path.join(d, 'desi_transfer_status.json')
            with open(js, 'w') as f:
                json.dump(st, f, indent=None, separators=(',', ':'))
            s = TransferStatus(d)
            s.update('20200703', '12345678', 'checksum')
            self.assertTrue(os.path.exists(js + '.bak'))
            self.assertEqual(s.status[0], [20200703, 12345678, 'checksum', True, '', 1565300090000])
            s.update('20200703', '12345680', 'rsync')
            self.assertEqual(s.status[0], [20200703, 12345680, 'rsync', True, '', 1565300090000])
            s.update('20200703', '12345678', 'rsync', failure=True)
            self.assertEqual(s.status[2], [20200703, 12345678, 'rsync', False, '', 1565300090000])
            s.update('20200703', '12345681', 'pipeline')
            self.assertEqual(s.status[0], [20200703, 12345681, 'pipeline', True, '', 1565300090000])
            s.update('20200703', '12345681', 'pipeline', last='arcs')
            self.assertEqual(s.status[0], [20200703, 12345681, 'pipeline', True, 'arcs', 1565300090000])
            s.update('20200703', 'all', 'backup')
            b = [i[3] for i in s.status if i[2] == 'backup']
            self.assertTrue(all(b))
            self.assertEqual(len(b), 4)

    @patch('time.time')
    def test_TransferStatus_update_empty(self, mock_time):
        """Test status reporting mechanism updates (with no initial JSON file).
        """
        mock_time.return_value = 1565300090
        # st = [[20200703, 12345678, 'rsync', True, '', 1565300074664],
        #       [20200703, 12345677, 'rsync', True, '', 1565300073000]]
        with TemporaryDirectory() as d:
            js = os.path.join(d, 'desi_transfer_status.json')
            # with open(js, 'w') as f:
            #     json.dump(st, f, indent=None, separators=(',', ':'))
            s = TransferStatus(d)
            s.update('20200703', '12345678', 'checksum')
            self.assertFalse(os.path.exists(js + '.bak'))
            self.assertEqual(s.status[0], [20200703, 12345678, 'checksum', True, '', 1565300090000])
            s.update('20200703', '12345680', 'rsync')
            self.assertTrue(os.path.exists(js + '.bak'))
            self.assertEqual(s.status[0], [20200703, 12345680, 'rsync', True, '', 1565300090000])
            s.update('20200703', '12345678', 'checksum', failure=True)
            self.assertEqual(s.status[1], [20200703, 12345678, 'checksum', False, '', 1565300090000])
            s.update('20200703', '12345681', 'pipeline')
            self.assertEqual(s.status[0], [20200703, 12345681, 'pipeline', True, '', 1565300090000])
            s.update('20200703', '12345681', 'pipeline', last='arcs')
            self.assertEqual(s.status[0], [20200703, 12345681, 'pipeline', True, 'arcs', 1565300090000])
            s.update('20200703', 'all', 'backup')
            b = [i[3] for i in s.status if i[2] == 'backup']
            self.assertTrue(all(b))
            self.assertEqual(len(b), 3)

    def test_TransferStatus_find(self):
        """Test status search.
        """
        st = [[20200703, 12345678, 'checksum', True, '', 1565300074664],
              [20200703, 12345678, 'rsync', True, '', 1565300074664],
              [20200703, 12345677, 'checksum', True, '', 1565300073000],
              [20200703, 12345677, 'rsync', True, '', 1565300073000]]
        with TemporaryDirectory() as d:
            js = os.path.join(d, 'desi_transfer_status.json')
            with open(js, 'w') as f:
                json.dump(st, f, indent=None, separators=(',', ':'))
            s = TransferStatus(d)
            i = s.find(20200702)
            self.assertEqual(len(i), 0)
            i = s.find(20200703)
            self.assertEqual(len(i), 4)
            i = s.find(20200703, 12345678)
            self.assertEqual(len(i), 2)
            i = s.find(20200703, stage='checksum')
            self.assertEqual(len(i), 2)


def test_suite():
    """Allows testing of only this module with the command::

        python setup.py test -m <modulename>
    """
    return unittest.defaultTestLoader.loadTestsFromName(__name__)
