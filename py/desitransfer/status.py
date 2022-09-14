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
from datetime import date
from argparse import ArgumentParser
from pkg_resources import resource_filename
from desiutil.log import log, DEBUG
from . import __version__ as dtVersion


class TransferStatus(object):
    """Simple object for interacting with desitransfer status reports.

    Parameters
    ----------
    directory : :class:`str`
        Retrieve and store JSON-encoded transfer status data in `directory`.
    install : :class:`bool`, optional
        If ``True``, install HTML and JS files.
    year : :class:`str` or :class:`int`
        Update records belonging to `year`. If not set, the current
        year is assumed.
    """

    def __init__(self, directory, install=False, year=None):
        self._stages = {'rsync': 0, 'checksum': 1, 'backup': 2}
        self.directory = directory
        self.status = dict()
        if year is None:
            self.current_year = str(date.today().year)
        else:
            self.current_year = str(year)
        self.first_year = "2018"
        self.json = os.path.join(self.directory,
                                 f'desi_transfer_status_{self.current_year}.json')
        if not os.path.exists(self.directory) or install:
            log.debug("os.makedirs('%s', exist_ok=True)", self.directory)
            os.makedirs(self.directory, exist_ok=True)
            for ext in ('html', 'js'):
                src = resource_filename('desitransfer',
                                        'data/desi_transfer_status.' + ext)
                if ext == 'html':
                    log.debug("shutil.copyfile('%s', '%s')", src,
                              os.path.join(self.directory, 'index.html'))
                    shutil.copyfile(src,
                                    os.path.join(self.directory, 'index.html'))
                else:
                    log.debug("shutil.copy('%s', '%s')", src, self.directory)
                    shutil.copy(src, self.directory)
        try:
            with open(self.json) as j:
                try:
                    self.status = json.load(j)
                except json.JSONDecodeError:
                    self._handle_malformed()
        except FileNotFoundError:
            pass
        return

    def _handle_malformed(self):
        """Handle malformed JSON files.

        This function will save the malformed file to a .bad file for
        later analysis, and write an empty array to a new status file.
        """
        bad = self.json + '.bad'
        log.error("Malformed JSON file detected: %s; saving original file as %s.", self.json, bad)
        log.debug("shutil.copy2('%s', '%s')", self.json, bad)
        shutil.copy2(self.json, bad)
        log.info("Writing empty array to %s.", self.json)
        with open(self.json, 'w') as j:
            j.write('{}')
        return

    def update(self, night, exposure, stage, failure=False):
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

        Returns
        -------
        :class:`int`
            The number of updates performed.
        """
        ts = int(time.time() * 1000)  # Convert to milliseconds for JS.
        success = not failure
        row = [self._stages[stage], int(success), ts]
        if exposure == 'all':
            rows = list()
            for expid in self.find(night):
                log.debug("self.status['%s']['%s'].insert(0, [%d, %d, %d])", night, expid, row[0], row[1], row[2])
                self.status[night][expid].insert(0, row)
                rows.append(row)
        else:
            log.debug("il = self.find('%s', '%s', '%s')", night, exposure, stage)
            il = self.find(night, exposure, stage)
            if il:
                old_row = self.status[night][exposure][il[0]]
                log.debug("self.status['%s']['%s'][%d] = [%d, %d, %d]", night, exposure, il[0], old_row[0], old_row[1], old_row[2])
                update = (ts >= old_row[2]) and (int(success) != old_row[1])
                if update:
                    log.debug("self.status['%s']['%s'][%d] = [%d, %d, %d]", night, exposure, il[0], row[0], row[1], row[2])
                    self.status[night][exposure][il[0]] = row
                    rows = []
                else:
                    #
                    # Rare edge case: daemon is in shadow/test mode and there
                    # are untransferred files.
                    #
                    return 0
            else:
                try:
                    log.debug("self.status['%s']['%s'].insert(0, [%d, %d, %d])", night, exposure, row[0], row[1], row[2])
                    self.status[night][exposure].insert(0, row)
                except KeyError:
                    log.debug("self.status['%s']['%s'] = [%d, %d, %d]", night, exposure, row[0], row[1], row[2])
                    self.status[night][exposure] = [row]
                rows = [row, ]
        #
        # Copy the original file before modifying.
        # This will overwrite any existing .bak file
        #
        log.debug("shutil.copy2('%s', '%s')", self.json, self.json + '.bak')
        try:
            shutil.copy2(self.json, self.json + '.bak')
        except FileNotFoundError:
            pass
        with open(self.json, 'w') as j:
            json.dump(self.status, j, indent=None, separators=(',', ':'))
        r = len(rows)
        if r == 0:
            return 1
        return r

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
        :class:`list` or class:`dict`
            If only `night` is set, return a :class:`dict` containing
            information on all exposures for that `night`. If `exposure`
            is not set, return a :class:`dict` keyed by exposure containing
            all data matching `stage` for that night. If `stage` is not set,
            return a :class:`list` containing *indexes* pointing to
            all data about that exposure. If both `exposure` and `stage` are set,
            return a :class:`list` of *indexes* pointing to the data for `exposure`
            filtered on `stage`.
        """
        try:
            n = self.status[night]
        except KeyError:
            n = self.status[night] = dict()
        if exposure is None and stage is None:
            return n
        elif exposure is None:
            e = dict()
            for expid in n:
                e[expid] = [k for k, r in enumerate(n[expid]) if r[0] == self._stages[stage]]
            return e
        elif stage is None:
            try:
                e = n[exposure]
            except KeyError:
                e = self.status[night][exposure] = list()
            return e
        else:
            try:
                r = [k for k, r in enumerate(n[exposure]) if r[0] == self._stages[stage]]
            except KeyError:
                r = list()
            return r


def _options():
    """Parse command-line options for :command:`desi_transfer_status`.

    Returns
    -------
    :class:`argparse.Namespace`
        The parsed command-line options.
    """
    desc = 'Update the status of DESI raw data transfers.'
    prsr = ArgumentParser(description=desc)
    prsr.add_argument('-d', '--directory', dest='directory', metavar='DIR',
                      default=os.path.join(os.environ['DESI_ROOT'],
                                           'spectro', 'staging', 'status'),
                      help="Install and update files in DIR (default %(default)s).")
    prsr.add_argument('-f', '--failure', action='store_true', dest='failure',
                      help='Indicate that the transfer failed somehow.')
    prsr.add_argument('-i', '--install', action='store_true', dest='install',
                      help='Ensure that HTML and related files are in place.')
    prsr.add_argument('-V', '--version', action='version',
                      version='%(prog)s {0}'.format(dtVersion))
    prsr.add_argument('-v', '--verbose', action='store_true',
                      help='Print debugging information.')
    prsr.add_argument('night', type=int, metavar='YYYYMMDD',
                      help="Night of observation.")
    prsr.add_argument('expid', metavar='EXPID',
                      help="Exposure number, or 'all'.")
    prsr.add_argument('stage',
                      choices=['rsync', 'checksum', 'backup'],
                      help="Transfer stage.")
    return prsr.parse_args()


def main():
    """Entry point for :command:`desi_transfer_status`.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    global log
    options = _options()
    if options.verbose:
        log.setLevel(DEBUG)
    st = TransferStatus(options.directory, install=options.install, year=str(options.night)[0:4])
    st.update(options.night, options.expid, options.stage, options.failure)
    return 0


# import os
# import json
# with open('desi_transfer_status.json') as j:
#     data = json.load(j)
#
# statuses = {'rsync': 0, 'checksum': 1, 'backup': 2}
# for year in ('2022', '2021', '2020', '2019', '2018', '2017'):
#     nights = dict()
#     for row in data:
#         if str(row[0]).startswith(year):
#             if row[0] in nights:
#                 if row[1] in nights[row[0]]:
#                     nights[row[0]][row[1]].append([statuses[row[2]], int(row[3]), row[5]])
#                 else:
#                     nights[row[0]][row[1]] = [[statuses[row[2]], int(row[3]), row[5]]]
#             else:
#                 nights[row[0]] = {row[1]: [[statuses[row[2]], int(row[3]), row[5]]]}
#     with open(f'nights_{year}.json', 'w') as j:
#         json.dump(nights, j, indent=None, separators=(',', ':'))
