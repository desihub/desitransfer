# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
desitransfer.nightwatch
=======================

Sync KPNO nightwatch. Due to differences in timing and directory structure,
this is kept separate from the raw data transfer daemon.

A cronjob running as desi@dtn01.nersc.gov ensures that this daemon is running.

Catchup on a specific night::

    NIGHT=20200124 && rsync -rlvt --exclude-from ${DESITRANSFER}/py/desitransfer/data/desi_nightwatch_transfer_exclude.txt \
        dts:/exposures/nightwatch/${NIGHT}/ /global/cfs/cdirs/desi/spectro/nightwatch/kpno/${NIGHT}/

By-hand startup sequence (bash shell)::

    source /global/common/software/desi/desi_environment.sh datatran
    module load desitransfer
    nohup nice -19 ${DESITRANSFER}/bin/desi_nightwatch_transfer &> /dev/null &
    tail -f ${DESI_ROOT}/spectro/nightwatch/desi_nightwatch_transfer.log

"""
import importlib.resources as ir
import logging
import os
import re
import stat
import time
from argparse import ArgumentParser
from logging.handlers import RotatingFileHandler, SMTPHandler
from socket import getfqdn
from desiutil.log import get_logger
from .common import rsync, today, idle_time
from .daemon import _popen
from . import __version__ as dtVersion

# Identify new night directory in a directory listing.
nightline = 'd[rwx-]{{9}} + [0-9,]+ [0-9]{{4}}/[0-9]{{2}}/[0-9]{{2}} [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}} {night}$'
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
    prsr.add_argument('-d', '--debug', action='store_true',
                      help='Set log level to DEBUG.')
    prsr.add_argument('-e', '--alert-after-errors', dest='maxerrors', metavar='N', type=int, default=10,
                      help='Send an alert after N serious transfer errors (default %(default)s).')
    prsr.add_argument('-k', '--kill', metavar='FILE',
                      default=os.path.join(os.environ['HOME'], 'stop_desi_transfer'),
                      help="Exit the script when FILE is detected (default %(default)s).")
    prsr.add_argument('-P', '--no-permission', action='store_false', dest='permission',
                      help='Do not set permissions for DESI collaboration access.')
    prsr.add_argument('-s', '--sleep', metavar='M', type=int, default=1,
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

At %(asctime)s, desi_nightwatch_transfer reported this message:

%(message)s

Kia ora koutou,
The DESI Collaboration Account
"""
    formatter2 = logging.Formatter(fmt, datefmt='%Y-%m-%d %H:%M:%S %Z')
    handler2.setFormatter(formatter2)
    handler2.setLevel(logging.CRITICAL)
    log.parent.addHandler(handler2)


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
    wait = options.sleep * 60
    source = '/exposures/nightwatch'
    basedir = os.path.join(os.environ['DESI_ROOT'], 'spectro', 'nightwatch')
    kpnodir = os.path.join(basedir, 'kpno')
    exclude = os.path.join(str(ir.files('desitransfer')), 'data', 'desi_nightwatch_transfer_exclude.txt')
    include = os.path.join(str(ir.files('desitransfer')), 'data', 'desi_nightwatch_transfer_include.txt')
    with open(include) as i:
        top_level_files = i.read().strip().split('\n')
    log.debug(', '.join(top_level_files))
    top_level_files_mode = stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH
    while True:
        #
        # See if we are in the idle period: 08:00 - 12:00 MST
        #
        idle_wait = idle_time()
        if idle_wait > 0:
            log.info('Idle time detected. Sleeping until approximately 12:00 MST.')
            time.sleep(idle_wait)
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
        log.debug(' '.join(cmd))
        status, out, err = _popen(cmd)
        found = False
        if status != '0':
            log.error('Error detected while syncing the list of nights; trying again in %d minutes.', night, options.sleep)
            log.error("STATUS = %s", status)
            log.error("STDOUT = \n%s", out)
            log.error("STDERR = \n%s", err)
            time.sleep(wait)
            continue
        for line in out.split('\n'):
            if re.match(nightline.format(night=night), line) is not None:
                log.debug(line)
                found = True
                break
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
        log.debug(' '.join(cmd))
        status, out, err = _popen(cmd)
        if status != '0':
            if 'file has vanished' in err:
                log.warning("File vanished while syncing %s; not serious.")
            else:
                errcount += 1
                log.error('Unknown error detected while syncing %s.', night)
                log.error("STATUS = %s", status)
                log.error("STDOUT = \n%s", out)
                log.error("STDERR = \n%s", err)
        #
        # Correct the permissions.
        #
        if options.permission:
            if os.path.exists(nightdir):
                log.info('Fixing permissions for DESI.')
                cmd = ['fix_permissions.sh', nightdir]
                log.debug(' '.join(cmd))
                status, out, err = _popen(cmd)
                if status != '0':
                    errcount += 1
                    log.error('Errror detected while fixing permissions for %s.', nightdir)
                    log.error("STATUS = %s", status)
                    log.error("STDOUT = \n%s", out)
                    log.error("STDERR = \n%s", err)
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
        log.debug(' '.join(cmd))
        status, out, err = _popen(cmd)
        if status != '0':
            log.error('Error detected while syncing top level html files.')
            log.error("STATUS = %s", status)
            log.error("STDOUT = \n%s", out)
            log.error("STDERR = \n%s", err)
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
        # Check for accumulated errors. Don't exit, but do send an alert email.
        #
        if errcount > options.maxerrors:
            log.critical('More than %d serious transfer errors detected, check the logs!', errcount)
            #
            # Reset the count so we don't get email every minute.
            #
            errcount = 0
        #
        # If all that took less than options.sleep minutes, sleep a bit.
        #
        dt = time.time() - t0
        if dt < wait:
            log.info('Sleeping for a bit.')
            time.sleep(wait - dt)
