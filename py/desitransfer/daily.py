# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
desitransfer.daily
==================

Entry point for :command:`desi_daily_transfer`.
"""
import os
import stat
import subprocess as sub
import sys
import time
from argparse import ArgumentParser
from pkg_resources import resource_filename
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
        with open(self.log, 'ab') as l:
            l.write(("DEBUG: desi_daily_transfer %s\n" % dtVersion).encode('utf-8'))
            l.write(("DEBUG: %s\n" % ' '.join(cmd)).encode('utf-8'))
            l.write(("DEBUG: Transfer start: %s\n" % stamp()).encode('utf-8'))
            l.flush()
            p = sub.Popen(cmd, stdout=l, stderr=sub.STDOUT)
            status = p.wait()
            l.write(("DEBUG: Transfer complete: %s\n" % stamp()).encode('utf-8'))
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
        with open(self.log, 'ab') as l:
            l.write(("DEBUG: Lock complete: %s\n" % stamp()).encode('utf-8'))

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
        with open(self.log, 'ab') as l:
            l.write(("DEBUG: %s\n" % ' '.join(cmd)).encode('utf-8'))
            l.flush()
            p = sub.Popen(cmd, stdout=l, stderr=sub.STDOUT)
            status = p.wait()
            l.write(("DEBUG: Permission reset complete: %s\n" % stamp()).encode('utf-8'))
        return status


def _config():
    """Wrap configuration so that module can be imported without
    environment variables set.
    """
    nightwatch_exclude = resource_filename('desitransfer',
                                           'data/desi_nightwatch_transfer_exclude.txt')
    engineering = os.path.realpath(os.path.join(os.environ['DESI_ROOT'],
                                                'engineering'))
    spectro = os.path.realpath(os.path.join(os.environ['DESI_ROOT'],
                                            'spectro'))
    return [DailyDirectory('/exposures/desi/sps',
                           os.path.join(engineering, 'spectrograph', 'sps')),
            # DailyDirectory('/exposures/nightwatch',
            #                os.path.join(spectro, 'nightwatch', 'kpno'),
            #                extra=['--exclude-from', nightwatch_exclude]),
            DailyDirectory('/data/dts/exposures/lost+found',
                           os.path.join(spectro, 'staging', 'lost+found'),
                           dirlinks=True),
            DailyDirectory('/data/donut/images',
                           os.path.join(engineering, 'donut', 'images')),
            DailyDirectory('/data/focalplane/calibration',
                           os.path.join(engineering, 'focalplane', 'calibration')),
            DailyDirectory('/data/focalplane/logs/calib_logs',
                           os.path.join(engineering, 'focalplane', 'logs', 'calib_logs')),
            DailyDirectory('/data/focalplane/logs/kpno',
                           os.path.join(engineering, 'focalplane', 'logs', 'kpno')),
            DailyDirectory('/data/focalplane/logs/xytest_data',
                           os.path.join(engineering, 'focalplane', 'logs', 'xytest_data')),
            DailyDirectory('/data/fvc/data',
                           os.path.join(engineering, 'fvc', 'images'))]


def _options(*args):
    """Parse command-line options for :command:`desi_daily_transfer`.

    Parameters
    ----------
    args : iterable
        Arguments to the function will be parsed for testing purposes.

    Returns
    -------
    :class:`argparse.Namespace`
        The parsed command-line options.
    """
    desc = "Transfer non-critical DESI data from KPNO to NERSC."
    prsr = ArgumentParser(description=desc)
    # prsr.add_argument('-b', '--backup', metavar='H', type=int, default=20,
    #                   help='UTC time in hours to trigger HPSS backups (default %(default)s:00 UTC).')
    prsr.add_argument('-c', '--completion', metavar='FILE',
                      default=os.path.join(os.environ['DESI_ROOT'], 'spectro', 'staging', 'status', 'daily.txt'),
                      help='Signal completion of transfer via FILE (default %(default)s).')
    prsr.add_argument('-d', '--debug', action='store_true',
                      help='Set log level to DEBUG.')
    # prsr.add_argument('-D', '--daemon', action='store_true',
    #                   help='Run in daemon mode.  If not specificed, the script will run once and exit.')
    # prsr.add_argument('-e', '--rsh', metavar='COMMAND', dest='ssh', default='/bin/ssh',
    #                   help="Use COMMAND for remote shell access (default '%(default)s').")
    prsr.add_argument('-k', '--kill', metavar='FILE',
                      default=os.path.join(os.environ['HOME'], 'stop_desi_transfer'),
                      help="Exit the script when FILE is detected (default %(default)s).")
    # prsr.add_argument('-n', '--nersc', default='cori', metavar='NERSC_HOST',
    #                   help="Trigger DESI pipeline on this NERSC system (default %(default)s).")
    prsr.add_argument('-P', '--no-permission', action='store_false', dest='permission',
                      help='Do not set permissions for DESI collaboration access.')
    # prsr.add_argument('-s', '--sleep', metavar='H', type=int, default=24,
    #                   help='In daemon mode, sleep H hours before checking for new data (default %(default)s hours).')
    # prsr.add_argument('-S', '--shadow', action='store_true',
    #                   help='Observe the actions of another data transfer script but do not make any changes.')
    prsr.add_argument('-V', '--version', action='version',
                      version='%(prog)s {0}'.format(dtVersion))
    return prsr.parse_args()


def main():
    """Entry point for :command:`desi_daily_transfer`.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    options = _options()
    if options.debug:
        print("DEBUG: os.remove('%s')" % options.completion)
    try:
        os.remove(options.completion)
    except FileNotFoundError:
        pass
    while True:
        if os.path.exists(options.kill):
            print("INFO: %s detected, shutting down daily transfer script." % options.kill)
            return 0
        for d in _config():
            status = d.transfer(permission=options.permission)
            if status != 0:
                print("ERROR: rsync problem detected for {0.source} -> {0.destination}!".format(d))
                return status
        # if options.daemon:
        #     time.sleep(options.sleep*60*60)
        # else:
        #     return 0
        if options.debug:
            print("DEBUG: daily transfer complete at %s. Writing %s." % (stamp(), options.completion))
        with open(options.completion, 'w') as c:
            c.write(stamp() + "\n")
        return 0
