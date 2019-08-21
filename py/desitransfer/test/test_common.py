# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test desitransfer.common.
"""
import os
import unittest
from unittest.mock import patch
from ..common import DTSDir, dir_perm, file_perm, rsync


class TestCommon(unittest.TestCase):
    """Test desitransfer.common.
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

    def test_permissions(self):
        """Ensure that file and directory permissions do not change.
        """
        self.assertEqual(dir_perm, 0o2750)
        self.assertEqual(file_perm, 0o0440)

    def test_DTSDir(self):
        """Test data structure to hold directory information.
        """
        d = DTSDir('/data/dts/exposures/raw',
                   '/desi/spectro/staging/raw',
                   '/desi/spectro/data',
                   '/nersc/projects/desi/spectro/data')
        self.assertEqual(d.source, '/data/dts/exposures/raw')
        self.assertEqual(d.staging, '/desi/spectro/staging/raw')
        self.assertEqual(d.destination, '/desi/spectro/data')
        self.assertEqual(d.hpss, '/nersc/projects/desi/spectro/data')

    def test_rsync(self):
        """Test construction of rsync command.
        """
        r = rsync('/source', '/destination')
        self.assertListEqual(r, ['/bin/rsync', '--verbose',
                                 '--recursive', '--copy-dirlinks', '--times',
                                 '--omit-dir-times', 'dts:/source/',
                                 '/destination/'])


def test_suite():
    """Allows testing of only this module with the command::

        python setup.py test -m <modulename>
    """
    return unittest.defaultTestLoader.loadTestsFromName(__name__)
