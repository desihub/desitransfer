# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
desitransfer.spacewatch
=======================

Download Spacewatch data from a server at KPNO.

Notes
-----
* Spacewatch data rolls over at 00:00 UTC = 17:00 MST.
* The data relevant to the previous night, say 20231030, would be downloaded
  on the morning of 20231031.
* Therefore to obtain all data of interest, just download the files that
  have already appeared in 2023/10/31/ (Spacewatch directory structure)
  the morning after DESI night 20231030.
"""
import datetime
import os
import re
from argparse import ArgumentParser
from html.parser import HTMLParser
try:
    utc = datetime.UTC
except AttributeError:
    # datetime.UTC is in Python 3.11
    import pytz
    utc = pytz.UTC
import requests
from . import __version__ as dtVersion


class SpacewatchHTMLParser(HTMLParser):
    """Extract JPG files from an HTML index.
    """
    def __init__(self, *args, **kwargs):
        super(SpacewatchHTMLParser, self).__init__(*args, **kwargs)
        self.jpg_re = re.compile(r'[0-9]{8}_[0-9]{6}\.jpg')
        self.jpg_files = list()

    def handle_starttag(self, tag, attrs):
        """Process HTML tags, in this case targeting anchor tags.
        """
        if tag == 'a':
            href = [a[1] for a in attrs if a[0] == 'href']
            if href:
                if self.jpg_re.match(href[0]) is not None:
                    self.jpg_files.append(href[0])


def jpg_list(index):
    """Obtain a list of JPEG files from an HTML index.

    Parameters
    ----------
    index : :class:`str`
        The URL of an HTML index.

    Returns
    -------
    :class:`list`
        A list of JPEG files found in `index`. The `index` URL is attached
        to the file names.
    """
    r = requests.get(index)
    parser = SpacewatchHTMLParser()
    if r.status_code == 200:
        parser.feed(r.content.decode(r.headers['Content-Type'].split('=')[1]))
    return [index + j for j in parser.jpg_files]


def download_jpg(files, destination):
    """Download `files` to `destination`.

    Parameters
    ----------
    files : :class:`list`
        A list of URLs to download.
    destination : :class:`str`
        A local directory to hold the files.

    Returns
    -------
    :class:`int`
        The number of files downloaded.
    """
    downloaded = 0
    for jpg in files:
        base_jpg = jpg.split('/')[-1]
        dst_jpg = os.path.join(destination, base_jpg)
        if os.path.exists(dst_jpg):
            # Overwrite?
            pass
        else:
            r = requests.get(jpg)
            if r.status_code == 200:
                downloaded += 1
                timestamp = int(datetime.datetime.strptime(r.headers['Last-Modified'], '%a, %d %b %Y %H:%M:%S %Z').replace(tzinfo=utc).timestamp())
                with open(dst_jpg, 'wb') as j:
                    j.write(r.content)
                os.utime(dst_jpg, (timestamp, timestamp))
    return downloaded


def _options():
    """Parse command-line options for :command:`desi_nightwatch_transfer`.

    Returns
    -------
    :class:`argparse.Namespace`
        The parsed command-line options.
    """
    desc = "Transfer Spacewatch data files."
    prsr = ArgumentParser(description=desc)
    prsr.add_argument('-d', '--debug', action='store_true',
                      help='Set log level to DEBUG.')
    prsr.add_argument('-V', '--version', action='version',
                      version='%(prog)s {0}'.format(dtVersion))
    prsr.add_argument('destination', metavar='DIR', help='Download files to DIR.')
    return prsr.parse_args()


def main():
    """Entry point for :command:`desi_spacewatch_transfer`.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    options = _options()
    spacewatch_root = 'https://varuna.kpno.noirlab.edu/allsky-all/images/cropped/'
    spacewatch_today = spacewatch_root + datetime.date.today().strftime("%Y/%m/%d/")
    n_files = download_jpg(jpg_list(spacewatch_today), options.destination)
    return 0
