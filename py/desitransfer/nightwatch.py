# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
desitransfer.nightwatch
=======================

Sync KPNO nightwatch.  This module will hopefully be integrated into
the standard transfer daemon.

Run as desi@dtn01.nersc.gov.

Catchup on a specific night::

    NIGHT=20200124 && rsync -rlvt --exclude-from ${DESITRANSFER}/py/desitransfer/data/desi_nightwatch_transfer_exclude.txt dts:/exposures/nightwatch/${NIGHT}/ /global/cfs/cdirs/desi/spectro/nightwatch/kpno/${NIGHT}/


Typical startup sequence (bash shell)::

    source /global/common/software/desi/desi_environment.sh datatran
    module load desitransfer
    nohup nice -19 ${DESITRANSFER}/bin/desi_nightwatch_transfer &> /dev/null &
    tail -f ${DESI_ROOT}/spectro/nightwatch/desi_nightwatch_transfer.log

The above sequence is for starting by hand.  A cronjob on dtn01 should ensure
that the script is running.
"""
import logging
import os
import stat
import subprocess as sub
import time
from argparse import ArgumentParser
from logging.handlers import RotatingFileHandler, SMTPHandler
from pkg_resources import resource_filename
from socket import getfqdn
from tempfile import TemporaryFile
from desiutil.log import get_logger
from .common import rsync, today
# from .daemon import _popen
from . import __version__ as dtVersion


log = None


def _options():
    """Parse command-line options for :command:`desi_nightwatch_transfer`.

    Returns
    -------
    :class:`argparse.Namespace`
        The parsed command-line options.
    """
    desc = "Transfer DESI nightwatch data files."
    prsr = ArgumentParser(description=desc)
    prsr.add_argument('-A', '--no-apache', action='store_false', dest='apache',
                      help='Do not set ACL for Apache httpd access.')
    # prsr.add_argument('-B', '--no-backup', action='store_false', dest='backup',
    #                   help="Skip NERSC HPSS backups.")
    # prsr.add_argument('-c', '--configuration', metavar='FILE',
    #                   help="Read configuration from FILE.")
    prsr.add_argument('-d', '--debug', action='store_true',
                      help='Set log level to DEBUG.')
    prsr.add_argument('-k', '--kill', metavar='FILE',
                      default=os.path.join(os.environ['HOME'], 'stop_desi_transfer'),
                      help="Exit the script when FILE is detected (default %(default)s).")
    # prsr.add_argument('-P', '--no-pipeline', action='store_false', dest='pipeline',
    #                   help="Only transfer files, don't start the DESI pipeline.")
    # prsr.add_argument('-S', '--shadow', action='store_true',
    #                   help='Observe the actions of another data transfer script but do not make any changes.')
    prsr.add_argument('-s', '--sleep', metavar='M', type=int, default=10,
                      help='Sleep M minutes before checking for new data (default %(default)s minutes).')
    prsr.add_argument('-V', '--version', action='version',
                      version='%(prog)s {0}'.format(dtVersion))
    return prsr.parse_args()


def _configure_log(debug):
    """Re-configure the default logger returned by ``desiutil.log``.

    Parameters
    ----------
    debug : :class:`bool`
        If ``True`` set the log level to ``DEBUG``.
    """
    global log
    # conf = self.conf['logging']
    log = get_logger(timestamp=True)
    h = log.parent.handlers[0]
    handler = RotatingFileHandler(os.path.join(os.environ['DESI_ROOT'], 'spectro', 'nightwatch', 'desi_nightwatch_transfer.log'),
                                  maxBytes=100000000,
                                  backupCount=100)
    handler.setFormatter(h.formatter)
    log.parent.removeHandler(h)
    log.parent.addHandler(handler)
    if debug:
        log.setLevel(logging.DEBUG)
    email_from = os.environ['USER'] + '@' + getfqdn()
    handler2 = SMTPHandler('localhost', email_from, ['desi-alarms-transfer@desi.lbl.gov', ],
                           'Critical error reported by desi_nightwatch_transfer!')
    fmt = """Greetings,

At %(asctime)s, desi_nightwatch_transfer failed with this message:

%(message)s

Kia ora koutou,
The DESI Collaboration Account
"""
    formatter2 = logging.Formatter(fmt, datefmt='%Y-%m-%d %H:%M:%S %Z')
    handler2.setFormatter(formatter2)
    handler2.setLevel(logging.CRITICAL)
    log.parent.addHandler(handler2)


def _popen(command):
    """Simple wrapper for :class:`subprocess.Popen` to avoid repeated code.

    Parameters
    ----------
    command : :class:`list`
        Command to pass to :class:`subprocess.Popen`.

    Returns
    -------
    :func:`tuple`
        The returncode, standard output and standard error.
    """
    log.debug(' '.join(command))
    with TemporaryFile() as tout, TemporaryFile() as terr:
        p = sub.Popen(command, stdout=tout, stderr=terr)
        p.wait()
        tout.seek(0)
        out = tout.read()
        terr.seek(0)
        err = terr.read()
    return (str(p.returncode), out.decode('utf-8'), err.decode('utf-8'))


def main():
    """Entry point for :command:`desi_nightwatch_transfer`.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    options = _options()
    _configure_log(options.debug)
    errcount = 0
    wait = options.sleep*60
    source = '/exposures/nightwatch'
    basedir = os.path.join(os.environ['DESI_ROOT'], 'spectro', 'nightwatch')
    kpnodir = os.path.join(basedir, 'kpno')
    # syncdir = os.path.join(basedir, 'sync')
    exclude = resource_filename('desitransfer', 'data/desi_nightwatch_transfer_exclude.txt')
    include = resource_filename('desitransfer', 'data/desi_nightwatch_transfer_include.txt')
    with open(include) as i:
        top_level_files = i.read().strip().split('\n')
    log.debug(', '.join(top_level_files))
    top_level_files_mode = stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH
    while True:
        log.info('Starting nightwatch transfer loop; desitransfer version = %s.',
                 dtVersion)
        if os.path.exists(options.kill):
            log.info("%s detected, shutting down nightwatch daemon.",
                     options.kill)
            return 0
        night = today()
        t0 = time.time()
        #
        # First check if there is any data for tonight yet.
        #
        log.info('Checking for nightwatch data from %s.', night)
        cmd = ['/bin/rsync', 'dts:{0}/'.format(source)]
        status, out, err = _popen(cmd)
        found = False
        if status != '0':
            errcount += 1
            log.error('Getting file list for %s; trying again in %d minutes.', night, options.sleep)
            time.sleep(wait)
            continue
        for line in out.split('\n'):
            if line.endswith(night):
                log.info(line)
                found = True
        if not found:
            log.info('No nightwatch data found for %s; trying again in %d minutes.', night, options.sleep)
            time.sleep(wait)
            continue
        #
        # Sync per-night directory.
        #
        nightdir = os.path.join(kpnodir, night)
        cmd = rsync(os.path.join(source, night), nightdir)
        cmd.insert(cmd.index('--omit-dir-times') + 1, '--exclude-from')
        cmd.insert(cmd.index('--exclude-from') + 1, exclude)
        log.info('Syncing %s.', night)
        status, out, err = _popen(cmd)
        if status != '0':
            errcount += 1
            log.error('Syncing %s.', night)
        #
        # Correct the permissions.
        #
        if options.apache:
            if os.path.exists(nightdir):
                log.info('Fixing permissions for DESI.')
                cmd = ['fix_permissions.sh', nightdir]
                status, out, err = _popen(cmd)
                if status != '0':
                    errcount += 1
                    log.error('Fixing permissions for %s.', nightdir)
            else:
                log.info('No data yet for night %s.', night)
        else:
            log.info("Skipping permission changes at user request.")
        #
        # Sync the top level files; skip the logs.
        #
        log.info('Syncing top level html/js files.')
        cmd = ['/bin/rsync', '--verbose', '--links', '--times', '--files-from',
               include,
               'dts:{0}/'.format(source),
               '{0}/'.format(kpnodir)]
        status, out, err = _popen(cmd)
        if status != '0':
            errcount += 1
            log.error('Syncing top level html files.')
        #
        # Hack: just add world read to those top level files since fix_permissions.sh
        # is recursive and we don't want to redo all nights.
        #
        for filename in top_level_files:
            log.debug("os.chmod('%s', 0o%o)",
                      os.path.join(kpnodir, filename),
                      top_level_files_mode)
            os.chmod(os.path.join(kpnodir, filename), top_level_files_mode)
        #
        # Check for accumulated errors.
        #
        if errcount > 10:
            log.critical('Transfer error count exceeded, shutting down.')
            return 1
        #
        # If all that took less than 10 minutes, sleep a bit.
        #
        dt = time.time() - t0
        if dt < wait:
            log.info('Sleeping for a bit.')
            time.sleep(wait - dt)
