# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
desitransfer.daily
==================

Entry point for :command:`desi_daily_transfer`.
"""
import importlib.resources as ir
import os
import stat
import subprocess as sub
from argparse import ArgumentParser
from .common import dir_perm, file_perm, rsync, stamp
from . import __version__ as dtVersion


class DailyDirectory(object):
    """Simple object to hold daily transfer configuration.

    Parameters
    ----------
    source : :class:`str`
        Source directory.
    destination : :class:`str`
        Desitination directory.
    extra : :class:`list`, optional
        Extra :command:`rsync` arguments to splice into command.
    dirlinks : :class:`bool`, optional
        If ``True``, convert source links into linked directory.
    """

    def __init__(self, source, destination, extra=[], dirlinks=False):
        self.source = source
        self.destination = destination
        self.log = self.destination + '.log'
        self.extra = extra
        self.dirlinks = dirlinks

    def transfer(self, permission=True):
        """Data transfer operations for a single destination directory.

        Parameters
        ----------
        permission : :class:`bool`, optional
            If ``True``, set permissions for DESI collaboration access.

        Returns
        -------
        :class:`int`
            The status returned by :command:`rsync`.
        """
        cmd = rsync(self.source, self.destination)
        if not self.dirlinks:
            cmd[cmd.index('--copy-dirlinks')] = '--links'
        if self.extra:
            for i, e in enumerate(self.extra):
                cmd.insert(cmd.index('--omit-dir-times') + 1 + i, e)
        with open(self.log, 'ab') as logfile:
            logfile.write(("DEBUG: desi_daily_transfer %s\n" % dtVersion).encode('utf-8'))
            logfile.write(("DEBUG: %s\n" % ' '.join(cmd)).encode('utf-8'))
            logfile.write(("DEBUG: Transfer start: %s\n" % stamp()).encode('utf-8'))
            logfile.flush()
            p = sub.Popen(cmd, stdout=logfile, stderr=sub.STDOUT)
            status = p.wait()
            logfile.write(("DEBUG: Transfer complete: %s\n" % stamp()).encode('utf-8'))
        if status == 0:
            self.lock()
            if permission:
                s = self.permission()
        return status

    def lock(self):
        """Make a directory read-only.
        """
        for dirpath, dirnames, filenames in os.walk(self.destination):
            if stat.S_IMODE(os.stat(dirpath).st_mode) != dir_perm:
                os.chmod(dirpath, dir_perm)
            for f in filenames:
                fpath = os.path.join(dirpath, f)
                if stat.S_IMODE(os.stat(fpath).st_mode) != file_perm:
                    os.chmod(fpath, file_perm)
        with open(self.log, 'ab') as logfile:
            logfile.write(("DEBUG: Lock complete: %s\n" % stamp()).encode('utf-8'))

    def permission(self):
        """Set permissions for DESI collaboration access.

        In theory this should not change any permissions set by
        :meth:`~DailyDirectory.lock`.

        Returns
        -------
        :class:`int`
            The status returned by :command:`fix_permissions.sh`.
        """
        cmd = ['fix_permissions.sh', self.destination]
        with open(self.log, 'ab') as logfile:
            logfile.write(("DEBUG: %s\n" % ' '.join(cmd)).encode('utf-8'))
            logfile.flush()
            p = sub.Popen(cmd, stdout=logfile, stderr=sub.STDOUT)
            status = p.wait()
            logfile.write(("DEBUG: Permission reset complete: %s\n" % stamp()).encode('utf-8'))
        return status


def _config(timeframe):
    """Wrap configuration so that module can be imported without
    environment variables set.

    Parameters
    ----------
    timeframe : :class:`str`
        Return the set of directories associated with `timeframe`.

    Returns
    -------
    :class:`list`
        A list of directories to transfer.
    """
    nightlog_include = os.path.join(str(ir.files('desitransfer')),
                                    'data', 'desi_nightlog_transfer_kpno.txt')
    # nightwatch_exclude = os.path.join(str(ir.files('desitransfer')),
    #                                   'data', 'desi_nightwatch_transfer_exclude.txt')
    engineering = os.path.realpath(os.path.join(os.environ['DESI_ROOT'],
                                                'engineering'))
    spectro = os.path.realpath(os.path.join(os.environ['DESI_ROOT'],
                                            'spectro'))
    survey = os.path.realpath(os.path.join(os.environ['DESI_ROOT'], 'survey'))
    if timeframe == 'morning':
        return [DailyDirectory('/software/www2/html/nightlogs',
                               os.path.join(survey, 'ops', 'nightlogs'),
                               extra=['--include-from', nightlog_include, '--exclude', '*']),]
    else:
        return [DailyDirectory('/data/dts/exposures/lost+found',
                               os.path.join(spectro, 'staging', 'lost+found'),
                               dirlinks=True),
                DailyDirectory('/data/focalplane/calibration',
                               os.path.join(engineering, 'focalplane', 'calibration')),
                DailyDirectory('/data/focalplane/logs/calib_logs',
                               os.path.join(engineering, 'focalplane', 'logs', 'calib_logs')),
                DailyDirectory('/data/focalplane/logs/kpno',
                               os.path.join(engineering, 'focalplane', 'logs', 'kpno')),
                DailyDirectory('/data/focalplane/logs/sequence_logs',
                               os.path.join(engineering, 'focalplane', 'logs', 'sequence_logs')),
                DailyDirectory('/data/focalplane/fp_temp_files',
                               os.path.join(engineering, 'focalplane', 'hwtables'),
                               extra=['--include', '*.csv', '--exclude', '*'])]


def _options():
    """Parse command-line options for :command:`desi_daily_transfer`.

    Returns
    -------
    :class:`argparse.Namespace`
        The parsed command-line options.
    """
    desc = "Transfer non-critical DESI data from KPNO to NERSC."
    prsr = ArgumentParser(description=desc)
    prsr.add_argument('-c', '--completion', metavar='FILE',
                      default=os.path.join(os.environ['DESI_ROOT'], 'spectro', 'staging', 'status', 'daily.txt'),
                      help='Signal completion of transfer via FILE (default %(default)s).')
    prsr.add_argument('-d', '--debug', action='store_true',
                      help='Set log level to DEBUG.')
    prsr.add_argument('-k', '--kill', metavar='FILE',
                      default=os.path.join(os.environ['HOME'], 'stop_desi_transfer'),
                      help="Exit the script when FILE is detected (default %(default)s).")
    prsr.add_argument('-P', '--no-permission', action='store_false', dest='permission',
                      help='Do not set permissions for DESI collaboration access.')
    prsr.add_argument('-V', '--version', action='version',
                      version='%(prog)s {0}'.format(dtVersion))
    prsr.add_argument('timeframe', choices=['morning', 'noon'],
                      help="Run transfer tasks associated with a specific time.")
    return prsr.parse_args()


def main():
    """Entry point for :command:`desi_daily_transfer`.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    status = 0
    options = _options()
    if options.timeframe == 'noon':
        if options.debug:
            print(f"DEBUG: os.remove('{options.completion}')")
        try:
            os.remove(options.completion)
        except FileNotFoundError:
            pass
    if os.path.exists(options.kill):
        print(f"INFO: {options.kill} detected, shutting down daily {options.timeframe} transfer script.")
        return 0
    for d in _config(options.timeframe):
        s = d.transfer(permission=options.permission)
        if s != 0:
            print(f"ERROR: rsync problem detected for {d.source} -> {d.destination}!")
            status |= s
    if options.timeframe == 'noon':
        if options.debug:
            print(f"DEBUG: daily {options.timeframe} transfer complete at {stamp()}. Writing {options.completion}.")
        with open(options.completion, 'w') as c:
            c.write(stamp() + "\n")
    return status
