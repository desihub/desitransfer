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
# from desiutil.log import get_logger


# log = None


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
            # log.debug("os.makedirs('%s')", self.directory)
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
        success = not failure
        if exposure == 'all':
            unique_ie = frozenset([self.status[k][1] for k in self.find(i)])
            rows = [[i, ie, stage, success, last, ts]
                    for ie in unique_ie]
        else:
            ie = int(exposure)
            r = [i, ie, stage, success, last, ts]
            il = self.find(i, ie, stage)
            if il:
                update = ((ts >= self.status[il[0]][5]) and
                          (success is not self.status[il[0]][3]))
                if last or update:
                    self.status[il[0]] = r
                    rows = []
            else:
                rows = [r, ]
        for row in rows:
            self.status.insert(0, row)
        self.status = sorted(self.status, key=lambda x: x[0]*10000000 + x[1],
                             reverse=True)
        with open(self.json, 'w') as j:
            json.dump(self.status, j, indent=None, separators=(',', ':'))

    def find(self, night, exposure=None, stage=None):
        """Find status entries that match `night`, etc.

        Parameters
        ----------
        night : :class:`str`
            Night of observation.
        exposure : :class:`str`, optional
            Exposure number.
        stage : :class:`str`, optional
            Stage of data transfer ('rsync', 'checksum', 'backup', ...).

        Returns
        -------
        :class:`list`
            A list of the *indexes* of matching status entries.
        """
        if exposure is None and stage is None:
            return [k for k, r in enumerate(self.status) if r[0] == night]
        elif exposure is None:
            return [k for k, r in enumerate(self.status) if r[0] == night
                    and r[2] == stage]
        elif stage is None:
            return [k for k, r in enumerate(self.status) if r[0] == night
                    and r[1] == exposure]
        else:
            return [k for k, r in enumerate(self.status) if r[0] == night
                    and r[1] == exposure and r[2] == stage]


def _options():
    """Parse command-line options for :command:`desi_transfer_status`.

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
    prsr.add_argument('expid', metavar='EXPID',
                      help="Exposure number, or 'all'.")
    prsr.add_argument('stage',
                      choices=['rsync', 'checksum', 'pipeline', 'backup'],
                      help="Transfer stage.")
    return prsr.parse_args()


def main():
    """Entry point for :command:`desi_transfer_status`.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    # global log
    options = _options()
    # log = get_logger()
    st = TransferStatus(options.directory)
    st.update(options.night, options.expid, options.stage,
              options.failure, options.last)
    return 0
