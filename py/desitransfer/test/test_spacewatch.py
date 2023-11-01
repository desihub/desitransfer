# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""Test desitransfer.spacewatch.
"""
import logging
import os
import sys
import unittest
from tempfile import TemporaryDirectory
from unittest.mock import call, patch, Mock
from ..spacewatch import (_options, jpg_list, download_jpg)


class TestSpacewatch(unittest.TestCase):
    """Test desitransfer.spacewatch.
    """

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        """Create a temporary directory to simulate SCRATCH.
        """
        self.tmp = TemporaryDirectory()

    def tearDown(self):
        """Clean up temporary directory.
        """
        self.tmp.cleanup()

    def test_options(self):
        """Test command-line arguments.
        """
        with patch.object(sys, 'argv', ['desi_spacewatch_transfer', '--debug', '/desi/external/spacewatch']):
            options = _options()
            self.assertTrue(options.debug)

    @patch('desitransfer.spacewatch.requests')
    def test_jpg_files(self, mock_requests):
        """Test parsing an index.html file.
        """
        mock_contents = Mock()
        mock_contents.headers = {'Content-Type': 'text/html;charset=ISO-8859-1'}
        mock_contents.status_code = 200
        mock_contents.content = """<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">
<html>
<head>
    <title>Index of /allsky-all/images/cropped/2023/10/31</title>
</head>
<body>
    <h1>Index of /allsky-all/images/cropped/2023/10/31</h1>
    <table>
        <tr>
            <th valign="top">
                <img src="/icons/blank.gif" alt="[ICO]">
            </th>
            <th>
                <a href="?C=N;O=D">Name</a>
            </th>
            <th>
                <a href="?C=M;O=A">Last modified</a>
            </th>
            <th>
                <a href="?C=S;O=A">Size</a>
            </th>
            <th>
                <a href="?C=D;O=A">Description</a>
            </th>
        </tr>
        <tr>
            <th colspan="5">
                <hr>
            </th>
        </tr>
        <tr>
            <td valign="top">
                <img src="/icons/back.gif" alt="[PARENTDIR]">
            </td>
            <td>
                <a href="/allsky-all/images/cropped/2023/10/">Parent Directory</a>
            </td>
            <td>&nbsp;</td>
            <td align="right">  - </td>
            <td>&nbsp;</td>
        </tr>
        <tr>
            <td valign="top">
                <img src="/icons/image2.gif" alt="[IMG]">
            </td>
            <td>
                <a href="20231031_000005.jpg">20231031_000005.jpg</a>
            </td>
            <td align="right">2023-10-31 00:00  </td>
            <td align="right">142K</td>
            <td>&nbsp;</td>
        </tr>
        <tr>
            <td valign="top">
                <img src="/icons/image2.gif" alt="[IMG]">
            </td>
            <td>
                <a href="20231031_000205.jpg">20231031_000205.jpg</a>
            </td>
            <td align="right">2023-10-31 00:02  </td>
            <td align="right">143K</td>
            <td>&nbsp;</td>
        </tr>
        <tr>
            <td valign="top">
                <img src="/icons/image2.gif" alt="[IMG]">
            </td>
            <td>
                <a href="20231031_000405.jpg">20231031_000405.jpg</a>
            </td>
            <td align="right">2023-10-31 00:04  </td>
            <td align="right">138K</td>
            <td>&nbsp;</td>
        </tr>
        <tr>
            <td valign="top">
                <img src="/icons/image2.gif" alt="[IMG]">
            </td>
            <td>
                <a href="20231031_000605.jpg">20231031_000605.jpg</a>
            </td>
            <td align="right">2023-10-31 00:06  </td>
            <td align="right">142K</td>
            <td>&nbsp;</td>
        </tr>
        <tr>
            <th colspan="5">
                <hr>
            </th>
        </tr>
    </table>
</body>
</html>""".encode('ISO-8859-1')
        mock_requests.get.return_value = mock_contents
        jpg_files = jpg_list('http://foo.bar/')
        mock_requests.get.assert_called_once_with('http://foo.bar/')
        self.assertListEqual(jpg_files, ['http://foo.bar/20231031_000005.jpg',
                                         'http://foo.bar/20231031_000205.jpg',
                                         'http://foo.bar/20231031_000405.jpg',
                                         'http://foo.bar/20231031_000605.jpg'])

    @patch('desitransfer.spacewatch.log')
    @patch('os.utime')
    @patch('desitransfer.spacewatch.requests')
    @patch('os.path.exists')
    def test_download_jpg(self, mock_exists, mock_requests, mock_utime, mock_log):
        """Test downloads of JPEG files.
        """
        mock_exists.side_effect = lambda x: x == os.path.join(destination, 'baz', '20231031_000005.jpg')
        # mock_exists.return_value = False
        mock_contents = Mock()
        mock_contents.headers = {'Last-Modified': 'Mon, 30 Oct 2023 00:00:24 GMT'}
        mock_contents.status_code = 200
        mock_contents.content = b"""123456789"""
        mock_requests.get.return_value = mock_contents
        files = ['http://foo.bar/20231031_000005.jpg',
                 'http://foo.bar/20231031_000205.jpg',
                 'http://foo.bar/20231031_000405.jpg',
                 'http://foo.bar/20231031_000605.jpg']
        destination = self.tmp.name
        n = download_jpg(files, destination + '/baz')
        self.assertEqual(n, 3)
        mock_exists.assert_has_calls([call(os.path.join(destination, 'baz', '20231031_000005.jpg')),
                                      call(os.path.join(destination, 'baz', '20231031_000205.jpg')),
                                      call(os.path.join(destination, 'baz', '20231031_000405.jpg')),
                                      call(os.path.join(destination, 'baz', '20231031_000605.jpg'))])
        mock_requests.get.assert_has_calls([call('http://foo.bar/20231031_000205.jpg'),
                                            call('http://foo.bar/20231031_000405.jpg'),
                                            call('http://foo.bar/20231031_000605.jpg')])
        mock_utime.assert_has_calls([call(os.path.join(destination, 'baz', '20231031_000205.jpg'), (1698624024, 1698624024)),
                                     call(os.path.join(destination, 'baz', '20231031_000405.jpg'), (1698624024, 1698624024)),
                                     call(os.path.join(destination, 'baz', '20231031_000605.jpg'), (1698624024, 1698624024))])
        mock_log.debug.assert_has_calls([call("os.makedirs('%s')", os.path.join(destination, 'baz')),
                                         call("Skipping existing file: %s.",
                                              os.path.join(destination, 'baz', '20231031_000005.jpg')),
                                         call("r = requests.get('%s')", 'http://foo.bar/20231031_000205.jpg'),
                                         call("r = requests.get('%s')", 'http://foo.bar/20231031_000405.jpg'),
                                         call("r = requests.get('%s')", 'http://foo.bar/20231031_000605.jpg')])
