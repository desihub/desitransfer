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
        cls.fake_status = {"20200703": {"12345677": [[0, 1, 1565300073000]],
                                        "12345678": [[0, 1, 1565300074664]]}}

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
            self.assertListEqual(list(s.status.keys()), [])
        #
        # Directory with JSON file.
        #
        with TemporaryDirectory() as d:
            js = os.path.join(d, 'desi_transfer_status_2020.json')
            with open(js, 'w') as f:
                json.dump(self.fake_status, f, indent=None, separators=(',', ':'))
            s = TransferStatus(d, year='2020')
            self.assertEqual(s.directory, d)
            self.assertTrue(os.path.isdir(d))
            self.assertListEqual(os.listdir(d), ['desi_transfer_status_2020.json'])
            self.assertListEqual(sorted(list(s.status.keys())),
                                 sorted(list(self.fake_status.keys())))
            self.assertListEqual(sorted(list(s.status['20200703'].keys())),
                                 sorted(list(self.fake_status['20200703'].keys())))
        #
        # New directory.
        #
        d = '/desi/spectro/status'
        with patch('desitransfer.status.log') as l:
            with patch('os.makedirs') as m:
                with patch('shutil.copy') as cp:
                    with patch('shutil.copyfile') as cf:
                        s = TransferStatus(d)
        l.debug.assert_has_calls([call("os.makedirs('%s', exist_ok=True)", d),
                                  call("shutil.copyfile('%s', '%s')", h, os.path.join(d, 'index.html')),
                                  call("shutil.copy('%s', '%s')", j, d)])
        m.assert_called_once_with(d, exist_ok=True)
        cp.assert_called_once_with(j, d)
        cf.assert_called_once_with(h, os.path.join(d, 'index.html'))

    @patch('desitransfer.status.log')
    def test_TransferStatus_handle_malformed_with_log(self, mock_log):
        """Test handling of malformed JSON files.
        """
        bad = resource_filename('desitransfer.test', 't/bad.json')
        with TemporaryDirectory() as d:
            shutil.copy(bad, os.path.join(d, 'desi_transfer_status_2020.json'))
            s = TransferStatus(d, year=2020)
            self.assertTrue(os.path.exists(os.path.join(d, 'desi_transfer_status_2020.json.bad')))
            self.assertListEqual(list(s.status.keys()), [])
            self.assertSetEqual(frozenset(os.listdir(d)),
                                frozenset(['desi_transfer_status_2020.json.bad',
                                           'desi_transfer_status_2020.json']))
        mock_log.error.assert_called_once_with('Malformed JSON file detected: %s; saving original file as %s.',
                                               os.path.join(d, 'desi_transfer_status_2020.json'),
                                               os.path.join(d, 'desi_transfer_status_2020.json.bad'))
        mock_log.debug.assert_called_once_with("shutil.copy2('%s', '%s')",
                                               os.path.join(d, 'desi_transfer_status_2020.json'),
                                               os.path.join(d, 'desi_transfer_status_2020.json.bad'))
        mock_log.info.assert_called_once_with('Writing empty array to %s.',
                                              os.path.join(d, 'desi_transfer_status_2020.json'))

    @patch('time.time')
    def test_TransferStatus_update(self, mock_time):
        """Test status reporting mechanism updates.
        """
        mock_time.return_value = 1565300090
        with TemporaryDirectory() as d:
            js = os.path.join(d, 'desi_transfer_status_2020.json')
            with open(js, 'w') as f:
                json.dump(self.fake_status, f, indent=None, separators=(',', ':'))
            s = TransferStatus(d, year=2020)
            r = s.update('20200703', '12345677', 'checksum')
            self.assertTrue(os.path.exists(js + '.bak'))
            self.assertEqual(r, 1)
            self.assertEqual(s.status['20200703']['12345677'][0], [1, 1, 1565300090000])

            r = s.update('20200703', '12345678', 'checksum', failure=True)
            self.assertEqual(r, 1)
            self.assertEqual(s.status['20200703']['12345678'][0], [1, 0, 1565300090000])

            r = s.update('20200703', '12345680', 'rsync')
            self.assertEqual(r, 1)
            self.assertEqual(s.status['20200703']['12345680'][0], [0, 1, 1565300090000])

            r = s.update('20200703', '12345678', 'checksum')
            self.assertEqual(r, 1)
            self.assertEqual(s.status['20200703']['12345678'][0], [1, 1, 1565300090000])

            r = s.update('20200703', '12345680', 'checksum')
            self.assertEqual(r, 1)
            self.assertEqual(s.status['20200703']['12345680'][0], [1, 1, 1565300090000])

            r = s.update('20200703', '12345681', 'rsync')
            self.assertEqual(r, 1)
            self.assertEqual(s.status['20200703']['12345681'][0], [0, 1, 1565300090000])

            r = s.update('20200703', '12345681', 'checksum')
            self.assertEqual(r, 1)
            self.assertEqual(s.status['20200703']['12345681'][0], [1, 1, 1565300090000])

            r = s.update('20200703', 'all', 'backup')
            self.assertEqual(r, 4)
            c = 0
            for e in s.status['20200703']:
                for r in s.status['20200703'][e]:
                    if r[0] == 2:
                        self.assertEqual(r[1], 1)
                        c += 1
            self.assertEqual(c, 4)

    @patch('time.time')
    def test_TransferStatus_update_empty(self, mock_time):
        """Test status reporting mechanism updates (with no initial JSON file).
        """
        mock_time.return_value = 1565300090
        with TemporaryDirectory() as d:
            js = os.path.join(d, 'desi_transfer_status_2020.json')
            s = TransferStatus(d, year=2020)
            s.update('20200703', '12345678', 'checksum')
            self.assertFalse(os.path.exists(js + '.bak'))
            self.assertEqual(s.status['20200703']['12345678'][0], [1, 1, 1565300090000])
            s.update('20200703', '12345680', 'rsync')
            self.assertTrue(os.path.exists(js + '.bak'))
            self.assertEqual(s.status['20200703']['12345680'][0], [0, 1, 1565300090000])
            s.update('20200703', '12345678', 'checksum', failure=True)
            self.assertEqual(s.status['20200703']['12345678'][0], [1, 0, 1565300090000])
            s.update('20200703', '12345681', 'rsync')
            self.assertEqual(s.status['20200703']['12345681'][0], [0, 1, 1565300090000])
            s.update('20200703', 'all', 'backup')
            c = 0
            for e in s.status['20200703']:
                for r in s.status['20200703'][e]:
                    if r[0] == 2:
                        self.assertEqual(r[1], 1)
                        c += 1
            self.assertEqual(c, 3)

    @patch('time.time')
    def test_TransferStatus_edge_case(self, mock_time):
        """Test edge case when desitransfer.daemon is running in test mode.
        """
        mock_time.return_value = 1565300000
        with TemporaryDirectory() as d:
            js = os.path.join(d, 'desi_transfer_status_2020.json')
            with open(js, 'w') as f:
                json.dump(self.fake_status, f, indent=None, separators=(',', ':'))
            s = TransferStatus(d, year=2020)
            r = s.update("20200703", "12345677", "rsync", failure=True)
            self.assertEqual(r, 0)
            self.assertEqual(s.status['20200703']['12345677'][0], [0, 1, 1565300073000])

    def test_TransferStatus_find(self):
        """Test status search.
        """
        st = {"20200703": {"12345677": [[0, 1, 1565300073000], [1, 0, 1565300073000]],
                           "12345678": [[0, 1, 1565300074664], [1, 0, 1565300074664]]}}
        with TemporaryDirectory() as d:
            js = os.path.join(d, 'desi_transfer_status_2020.json')
            with open(js, 'w') as f:
                json.dump(st, f, indent=None, separators=(',', ':'))
            s = TransferStatus(d, year=2020)
            with self.assertRaises(KeyError) as cm:
                i = s.find('20200702')
            self.assertEqual(cm.exception.args[0], "Undefined night = '20200702'!")
            i = s.find('20200703')
            self.assertEqual(len(i), 2)
            i = s.find('20200703', '12345678')
            self.assertEqual(len(i), 2)
            i = s.find('20200703', '12345679')
            self.assertEqual(len(i), 0)
            i = s.find('20200703', stage='checksum')
            self.assertEqual(len(i), 3)
            self.assertListEqual(i["12345677"], [1])
            self.assertListEqual(i["12345678"], [1])
            self.assertListEqual(i["12345679"], [])
