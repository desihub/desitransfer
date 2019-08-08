# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test desitransfer.daemon.
"""
import json
import os
import unittest
from tempfile import TemporaryDirectory
from unittest.mock import patch
from pkg_resources import resource_filename
from ..daemon import _config, DTSPipeline, DTSStatus


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

    def test_DTSPipeline(self):
        """Test pipeline command generation.
        """
        dn = os.path.join(os.environ['HOME'], 'bin', 'wrap_desi_night.sh')
        with patch('desitransfer.daemon.log') as m:
            p = DTSPipeline('cori')
            c = p.command('20200703', '12345678')
            self.assertListEqual(c, ['ssh', '-q', 'cori', dn, 'update',
                                     '--night', '20200703',
                                     '--expid', '12345678',
                                     '--nersc', 'cori',
                                     '--nersc_queue', 'realtime',
                                     '--nersc_maxnodes', '25'])
        m.debug.assert_called_with(' '.join(c))
        with patch('desitransfer.daemon.log') as m:
            p = DTSPipeline('cori')
            c = p.command('20200703', '12345678', 'science')
            self.assertListEqual(c, ['ssh', '-q', 'cori', dn, 'redshifts',
                                     '--night', '20200703',
                                     '--expid', '12345678',
                                     '--nersc', 'cori',
                                     '--nersc_queue', 'realtime',
                                     '--nersc_maxnodes', '25'])
        m.debug.assert_called_with(' '.join(c))

    def test_DTSStatus_init(self):
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
            s = DTSStatus(d)
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
            s = DTSStatus(d)
            self.assertEqual(s.directory, d)
            self.assertTrue(os.path.isdir(d))
            self.assertListEqual(os.listdir(d), ['desi_transfer_status.json'])
            self.assertListEqual(s.status[0], st[0])
            self.assertListEqual(s.status[1], st[1])
        #
        # New directory.
        #
        d = '/desi/spectro/status'
        with patch('desitransfer.daemon.log') as l:
            with patch('os.makedirs') as m:
                with patch('shutil.copy') as cp:
                    with patch('shutil.copyfile') as cf:
                        s = DTSStatus(d)
        l.debug.assert_called_once_with("os.makedirs('%s')", d)
        m.assert_called_once_with(d)
        cp.assert_called_once_with(j, d)
        cf.assert_called_once_with(h, os.path.join(d, 'index.html'))

    def test_DTSStatus_update(self):
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
                s = DTSStatus(d)
                s.update('20200703', '12345679', 'checksum')
                self.assertEqual(s.status[0], [20200703, 12345679, 'checksum', True, '', 1565300090000])
                s.update('20200703', '12345680', 'rsync', last='science')
                self.assertEqual(s.status[0], [20200703, 12345680, 'rsync', True, 'science', 1565300090000])
                s.update('20200703', 'all', 'backup')
                b = [i[3] for i in s.status if i[2] == 'backup']
                self.assertTrue(all(b))
                self.assertEqual(len(b), 4)


def test_suite():
    """Allows testing of only this module with the command::

        python setup.py test -m <modulename>
    """
    return unittest.defaultTestLoader.loadTestsFromName(__name__)
