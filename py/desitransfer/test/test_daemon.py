# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test desitransfer.daemon.
"""
import os
import unittest
from unittest.mock import patch
from ..daemon import _config, DTSPipeline


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
        m.debug.assert_called_with('ssh -q cori ' +
                                   '/home/weaver/bin/wrap_desi_night.sh ' +
                                   'update --night 20200703 ' +
                                   '--expid 12345678 --nersc cori ' +
                                   '--nersc_queue realtime ' +
                                   '--nersc_maxnodes 25')
        with patch('desitransfer.daemon.log') as m:
            p = DTSPipeline('cori')
            c = p.command('20200703', '12345678', 'science')
            self.assertListEqual(c, ['ssh', '-q', 'cori', dn, 'redshifts',
                                     '--night', '20200703',
                                     '--expid', '12345678',
                                     '--nersc', 'cori',
                                     '--nersc_queue', 'realtime',
                                     '--nersc_maxnodes', '25'])
        m.debug.assert_called_with('ssh -q cori ' +
                                   '/home/weaver/bin/wrap_desi_night.sh ' +
                                   'redshifts --night 20200703 ' +
                                   '--expid 12345678 --nersc cori ' +
                                   '--nersc_queue realtime ' +
                                   '--nersc_maxnodes 25')


def test_suite():
    """Allows testing of only this module with the command::

        python setup.py test -m <modulename>
    """
    return unittest.defaultTestLoader.loadTestsFromName(__name__)
