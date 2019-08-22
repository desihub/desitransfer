# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
desitransfer.daily
==================

Entry point for :command:`desi_daily_transfer`.
"""
import os
import subprocess as sub
import sys
import time
from argparse import ArgumentParser
from .common import DTSDir, dir_perm, file_perm, rsync, stamp


def _config():
    """Wrap configuration so that module can be imported without
    environment variables set.
    """
    return [DTSDir('/exposures/desi/sps', 'UNUSED',
                   os.path.realpath(os.path.join(os.environ['DESI_ROOT'],
                                                 'engineering', 'spectrograph',
                                                 'sps')),
                   'UNUSED'),
            DTSDir('/data/dts/exposures/lost+found', 'UNUSED',
                   os.path.realpath(os.path.join(os.environ['DESI_ROOT'],
                                                 'spectro', 'staging',
                                                 'lost+found')),
                   'UNUSED'),
            DTSDir('/data/fvc/data', 'UNUSED',
                   os.path.realpath(os.path.join(os.environ['DESI_ROOT'],
                                                 'engineering', 'fvc',
                                                 'images')),
                   'UNUSED')]


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
    prsr = ArgumentParser(prog=os.path.basename(sys.argv[0]), description=desc)
    # prsr.add_argument('-b', '--backup', metavar='H', type=int, default=20,
    #                   help='UTC time in hours to trigger HPSS backups (default %(default)s:00 UTC).')
    # prsr.add_argument('-d', '--debug', action='store_true',
    #                   help='Set log level to DEBUG.')
    prsr.add_argument('-d', '--daemon', action='store_true',
                      help='Run in daemon mode.  If not specificed, the script will run once and exit.')
    prsr.add_argument('-e', '--rsh', metavar='COMMAND', dest='ssh', default='/bin/ssh',
                      help="Use COMMAND for remote shell access (default '%(default)s').")
    prsr.add_argument('-k', '--kill', metavar='FILE',
                      default=os.path.join(os.environ['HOME'], 'stop_desi_transfer'),
                      help="Exit the script when FILE is detected (default %(default)s).")
    # prsr.add_argument('-n', '--nersc', default='cori', metavar='NERSC_HOST',
    #                   help="Trigger DESI pipeline on this NERSC system (default %(default)s).")
    # prsr.add_argument('-P', '--no-pipeline', action='store_false', dest='pipeline',
    #                   help="Only transfer files, don't start the DESI pipeline.")
    prsr.add_argument('-s', '--sleep', metavar='H', type=int, default=24,
                      help='In daemon mode, sleep H hours before checking for new data (default %(default)s hours).')
    # prsr.add_argument('-S', '--shadow', action='store_true',
    #                   help='Observe the actions of another data transfer script but do not make any changes.')
    return prsr.parse_args()


def main():
    """Entry point for :command:`desi_daily_transfer`.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    options = _options()
    while True:
        if os.path.exists(options.kill):
            # log.info("%s detected, shutting down daily transfer script.", options.kill)
            print("INFO: %s detected, shutting down daily transfer script." % options.kill)
            return 0
        for d in _config():
            log = d.destination + '.log'
            cmd = rsync(d.source, d.destination)
            with open(log, 'ab') as l:
                l.write(("DEBUG: %s\n" % stamp()).encode('utf-8'))
                l.write(("DEBUG: %s\n" % ' '.join(cmd)).encode('utf-8'))
                l.flush()
                p = sub.Popen(cmd, stdout=l, stderr=sub.STDOUT)
                status = p.wait()
            if status == 0:
                for dirpath, dirnames, filenames in os.walk(d.destination):
                    for d in dirnames:
                        os.chmod(os.path.join(dirpath, d), dir_perm)
                    for f in filenames:
                        os.chmod(os.path.join(dirpath, f), file_perm)
            else:
                print("ERROR: rsync problem detected for ${0.source} -> ${0.destination}!".format(d))
                return status
        if options.daemon:
            time.sleep(options.sleep*60*60)
        else:
            return 0
