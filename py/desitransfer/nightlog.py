# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
desitransfer.nightlog
=====================

Bi-directional sync of KPNO and NERSC nightlog data.

Run as a daemon on ``desi@dtn01.nersc.gov``.
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
from . import __version__ as dtVersion


log = None


def _options():
    """Parse command-line options for :command:`desi_nightlog_transfer`.

    Returns
    -------
    :class:`argparse.Namespace`
        The parsed command-line options.
    """
    desc = "Transfer DESI nightlog data files."
    prsr = ArgumentParser(description=desc)
    # prsr.add_argument('-B', '--no-backup', action='store_false', dest='backup',
    #                   help="Skip NERSC HPSS backups.")
    # prsr.add_argument('-c', '--configuration', metavar='FILE',
    #                   help="Read configuration from FILE.")
    prsr.add_argument('-d', '--debug', action='store_true',
                      help='Set log level to DEBUG.')
    prsr.add_argument('-k', '--kill', metavar='FILE',
                      default=os.path.join(os.environ['HOME'], 'stop_desi_transfer'),
                      help="Exit the script when FILE is detected (default %(default)s).")
    prsr.add_argument('-P', '--no-permission', action='store_false', dest='permission',
                      help='Do not set permissions for DESI collaboration access.')
    # prsr.add_argument('-S', '--shadow', action='store_true',
    #                   help='Observe the actions of another data transfer script but do not make any changes.')
    prsr.add_argument('-s', '--sleep', metavar='M', type=int, default=5,
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
    handler = RotatingFileHandler(os.path.join(os.environ['DESI_ROOT'], 'survey', 'ops', 'nightlog', 'desi_nightlog_transfer.log'),
                                  maxBytes=100000000,
                                  backupCount=100)
    handler.setFormatter(h.formatter)
    log.parent.removeHandler(h)
    log.parent.addHandler(handler)
    if debug:
        log.setLevel(logging.DEBUG)
    email_from = os.environ['USER'] + '@' + getfqdn()
    handler2 = SMTPHandler('localhost', email_from, ['desi-alarms-transfer@desi.lbl.gov', ],
                           'Critical error reported by desi_nightlog_transfer!')
    fmt = """Greetings,

At %(asctime)s, desi_nightlog_transfer failed with this message:

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
    """Entry point for :command:`desi_nightlog_transfer`.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    options = _options()
    _configure_log(options.debug)
    errcount = 0
    wait = options.sleep*60
    kpno_source = '/software/www2/html/nightlogs'
    nersc_source = os.path.join(os.environ['DESI_ROOT'], 'survey', 'ops', 'nightlog')
    while True:
        log.info('Starting nightlog transfer loop; desitransfer version = %s.',
                 dtVersion)
        if os.path.exists(options.kill):
            log.info("%s detected, shutting down nightlog daemon.",
                     options.kill)
            return 0
        night = today()
        t0 = time.time()
        #
        # First check if there is any data for tonight yet.
        #
        log.info('Checking for nightlog data from %s.', night)
        cmd = ['/bin/rsync', 'dts:{0}/'.format(kpno_source)]
        status, out, err = _popen(cmd)
        kpno_found = False
        if status != '0':
            errcount += 1
            log.error('Getting KPNO file list for %s; trying again in %d minutes.', night, options.sleep)
            time.sleep(wait)
            continue
        for line in out.split('\n'):
            if line.endswith(night):
                log.info(line)
                kpno_found = True
        nersc_found = os.path.exists(os.path.join(nersc_source, night))
        if not (kpno_found or nersc_found):
            log.info('No KPNO or NERSC nightlog data found for %s; trying again in %d minutes.', night, options.sleep)
            time.sleep(wait)
            continue
        #
        # Sync per-night directory.
        #
        if kpno_found:
            cmd = rsync(os.path.join(kpno_source, night),
                        os.path.join(nersc_source, night))
            log.info('Syncing %s KPNO -> NERSC.', night)
            status, out, err = _popen(cmd)
            if status != '0':
                errcount += 1
                log.error('Syncing %s KPNO -> NERSC.', night)
        if nersc_found:
            cmd = rsync(os.path.join(nersc_source, night),
                        os.path.join(kpno_source, night),
                        reverse=True)
            log.info('Syncing %s NERSC -> KPNO.', night)
            status, out, err = _popen(cmd)
            if status != '0':
                errcount += 1
                log.error('Syncing %s NERSC -> KPNO.', night)
        #
        # Correct the permissions.
        #
        if options.permission:
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
        # Check for accumulated errors.
        #
        if errcount > 10:
            log.critical('Transfer error count exceeded, check logs!')
            return 1
        #
        # If all that took less than sleep.wait minutes, sleep a bit.
        #
        dt = time.time() - t0
        if dt < wait:
            log.info('Sleeping for a bit.')
            time.sleep(wait - dt)
