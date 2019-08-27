# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test desitransfer.status.
"""
import json
import os
import unittest
from unittest.mock import patch
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
            options = _options('20190703', '12345678')
            self.assertEqual(options.night, 20190703)
            self.assertEqual(options.expid, 12345678)

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
        with patch('desitransfer.status.log') as l:
            with patch('os.makedirs') as m:
                with patch('shutil.copy') as cp:
                    with patch('shutil.copyfile') as cf:
                        s = TransferStatus(d)
        l.debug.assert_called_once_with("os.makedirs('%s')", d)
        m.assert_called_once_with(d)
        cp.assert_called_once_with(j, d)
        cf.assert_called_once_with(h, os.path.join(d, 'index.html'))

    def test_TransferStatus_update(self):
        """Test status reporting mechanism updates.
        """
        st = [[20200703, 12345678, 'rsync', True, '', 1565300074664],
              [20200703, 12345677, 'rsync', True, '', 1565300073000]]
        with patch('time.time') as t:
            t.return_value = 1565300090
            with TemporaryDirectory() as d:
                js = os.path.join(d, 'desi_transfer_status.json')
                with open(js, 'w') as f:
                    json.dump(st, f, indent=None, separators=(',', ':'))
                s = TransferStatus(d)
                s.update('20200703', '12345679', 'checksum')
                self.assertEqual(s.status[0], [20200703, 12345679, 'checksum', True, '', 1565300090000])
                s.update('20200703', '12345680', 'rsync', last='science')
                self.assertEqual(s.status[0], [20200703, 12345680, 'rsync', True, 'science', 1565300090000])
                s.update('20200703', '12345681', 'pipeline')
                self.assertEqual(s.status[0], [20200703, 12345681, 'pipeline', True, '', 1565300090000])
                s.update('20200703', '12345681', 'pipeline', last='arcs')
                self.assertEqual(s.status[0], [20200703, 12345681, 'pipeline', True, 'arcs', 1565300090000])
                s.update('20200703', 'all', 'backup')
                b = [i[3] for i in s.status if i[2] == 'backup']
                self.assertTrue(all(b))
                self.assertEqual(len(b), 5)

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
