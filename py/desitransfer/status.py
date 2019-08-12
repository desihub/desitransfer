# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
desitransfer.status
===================

Entry point for :command:`desi_transfer_status`.
"""
import json
import os
import shutil
import sys
import time
from argparse import ArgumentParser
from pkg_resources import resource_filename


log = None


class TransferStatus(object):
    """Simple object for interacting with desitransfer status reports.

    Parameters
    ----------
    directory : :class:`str`
        Retrieve and store JSON-encoded transfer status data in `directory`.
    """

    def __init__(self, directory):
        self.directory = directory
        self.json = os.path.join(self.directory, 'desi_transfer_status.json')
        self.status = list()
        if not os.path.exists(self.directory):
            log.debug("os.makedirs('%s')", self.directory)
            os.makedirs(self.directory)
            for ext in ('html', 'js'):
                src = resource_filename('desitransfer',
                                        'data/desi_transfer_status.' + ext)
                if ext == 'html':
                    shutil.copyfile(src,
                                    os.path.join(self.directory, 'index.html'))
                else:
                    shutil.copy(src, self.directory)
            return
        try:
            with open(self.json) as j:
                self.status = json.load(j)
        except FileNotFoundError:
            pass
        return

    def update(self, night, exposure, stage, failure=False, last=''):
        """Update the transfer status.

        Parameters
        ----------
        night : :class:`str`
            Night of observation.
        exposure : :class:`str`
            Exposure number.
        stage : :class:`str`
            Stage of data transfer ('rsync', 'checksum', 'backup', ...).
        failure : :class:`bool`, optional
            Indicate failure.
        last : :class:`str`, optional
            Mark this exposure as the last of a given type for the night
            ('arcs', 'flats', 'science').
        """
        ts = int(time.time() * 1000)  # Convert to milliseconds for JS.
        i = int(night)
        if exposure == 'all':
            rows = [[r[0], r[1], stage, not failure, last, ts]
                    for r in self.status if r[0] == i]
        else:
            rows = [[i, int(exposure), stage, not failure, last, ts], ]
        for row in rows:
            self.status.insert(0, row)
        self.status = sorted(self.status, key=lambda x: x[0]*10000000 + x[1],
                             reverse=True)
        with open(self.json, 'w') as j:
            json.dump(self.status, j, indent=None, separators=(',', ':'))


def _options(*args):
    """Parse command-line options for :command:`desi_transfer_status`.

    Parameters
    ----------
    args : iterable
        Arguments to the function will be parsed for testing purposes.

    Returns
    -------
    :class:`argparse.Namespace`
        The parsed command-line options.
    """
    desc = "Transfer DESI raw data files."
    prsr = ArgumentParser(description='Update the status of DESI raw data transfers.',
                          prog=os.path.basename(sys.argv[0]))
    prsr.add_argument('-d', '--directory', dest='directory', metavar='DIR',
                      default=os.path.join(os.environ['DESI_ROOT'],
                                           'spectro', 'staging', 'status'),
                      help="Install and update files in DIR.")
    prsr.add_argument('-f', '--failure', action='store_true', dest='failure',
                      help='Indicate that the transfer failed somehow.')
    prsr.add_argument('-i', '--install', action='store_true', dest='install',
                      help='Ensure that HTML and related files are in place.')
    prsr.add_argument('-l', '--last', dest='last', default='',
                      choices=['flats', 'arcs', 'science'],
                      help='Indicate that a certain set of exposures is complete.')
    prsr.add_argument('night', type=int, metavar='YYYYMMDD',
                      help="Night of observation.")
    prsr.add_argument('expid', type=int, metavar='N',
                      help="Exposure number.")
    if len(args) > 0:
        options = prsr.parse_args(args)
    else:  # pragma: no cover
        options = prsr.parse_args()
    return options


def main():
    """Entry point for :command:`desi_transfer_status`.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    global log
    options = _options()
    log = get_logger()
    st = TransferStatus(options.directory)
    st.update(options.night, options.expid, 'rsync',
              options.failure, options.last)
    return 0
