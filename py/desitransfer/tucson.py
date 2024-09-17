# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
desitransfer.tucson
===================

Entry point for :command:`desi_tucson_transfer`.
"""
import logging
import os
import subprocess as sub
import time
from argparse import ArgumentParser
from logging.handlers import SMTPHandler
import requests
from . import __version__ as dtVersion
from .common import exclude_years
from desiutil.log import get_logger


log = None


static = ['cmx',
          'datachallenge',
          'engineering/2021_summer_illumination_checks',
          'engineering/donut',
          'engineering/fvc',
          'engineering/fvc_distortion',
          'engineering/gfa',
          'engineering/pfa2positioner',
          'engineering/platemaker',
          'engineering/spectrograph',
          'engineering/svn_export_focalplane_12302018',
          'engineering/umdata',
          'protodesi',
          'public/epo',
          'public/ets',
          'spectro/desi_spectro_calib',
          'spectro/redux/denali',
          'spectro/redux/everest',
          'spectro/templates/basis_templates',
          'survey/ops/surveyops/trunk',
          'sv',
          'target/catalogs',
          'target/secondary',
          'target/cmx_files']


dynamic = ['spectro/data',
           'spectro/nightwatch/kpno',
           'spectro/staging/lost+found',
           'spectro/redux/daily',
           'spectro/redux/daily/exposures',
           'spectro/redux/daily/preproc',
           'spectro/redux/daily/tiles',
           'engineering/focalplane',
           'engineering/focalplane/hwtables',
           'software/AnyConnect',
           'software/CiscoSecureClient']


includes = {'engineering/focalplane': ["--exclude", "archive", "--exclude", "hwtables",
                                       "--exclude", ".ipynb_checkpoints", "--exclude", "*.ipynb"],
            'engineering/focalplane/hwtables': ["--include", "*.csv", "--exclude", "*"],
            'spectro/desi_spectro_calib': ["--exclude", ".svn"],
            'spectro/data': exclude_years(2018),
            'spectro/nightwatch/kpno': exclude_years(2021),
            'spectro/redux/daily': ["--exclude", "*.tmp", "--exclude", "attic",
                                    "--exclude", "exposures", "--exclude", "preproc",
                                    "--exclude", "temp", "--exclude", "tiles"],
            'spectro/redux/daily/exposures': exclude_years(2019) + ["--exclude", "*.tmp"],
            'spectro/redux/daily/preproc': exclude_years(2019) +  ["--exclude", "*.tmp", "--exclude", "preproc-*.fits",
                                                                   "--exclude", "preproc-*.fits.gz"],
            'spectro/redux/daily/tiles': ["--exclude", "*.tmp", "--exclude", "temp"],
            'spectro/templates/basis_templates': ["--exclude", ".svn", "--exclude", "basis_templates_svn-old"],
            'survey/ops/surveyops/trunk': ["--exclude", ".svn", "--exclude", "cronupdate.log"],
            'target/catalogs': ["--include", "dr8", "--include", "dr9",
                                "--include", "gaiadr2", "--include", "subpriority", "--exclude", "*"]}


priority = ('spectro/data',
            'spectro/redux/daily',
            'spectro/redux/daily/exposures',
            'spectro/redux/daily/preproc',
            'spectro/redux/daily/tiles')


def _configure_log(debug):
    """Re-configure the default logger returned by ``desiutil.log``.

    Parameters
    ----------
    debug : :class:`bool`
        If ``True`` set the log level to ``DEBUG``.
    """
    global log
    log = get_logger(timestamp=True)
    if debug:
        log.setLevel(logging.DEBUG)
    email_from = 'NOIRLab Mirror Account <{0}>'.format(os.environ['MAILFROM'])
    email_to = [os.environ['MAILTO']]
    handler2 = SMTPHandler('localhost', email_from, email_to,
                           'Error reported by desi_tucson_transfer!')
    fmt = """Greetings,

At %(asctime)s, desi_tucson_transfer reported this serious error:

%(message)s

Kia ora koutou,
The DESI NOIRLab Mirror Account
"""
    formatter2 = logging.Formatter(fmt, datefmt='%Y-%m-%d %H:%M:%S %Z')
    handler2.setFormatter(formatter2)
    handler2.setLevel(logging.CRITICAL)
    log.parent.addHandler(handler2)


def _options():
    """Parse command-line options for :command:`desi_tucson_transfer`.

    Returns
    -------
    :class:`argparse.Namespace`
        The parsed command-line options.
    """
    desc = "Mirror DESI data from NERSC to NOIRLab."
    prsr = ArgumentParser(description=desc)
    prsr.add_argument('-c', '--checksum', action='store_true',
                      help='Pass -c, --checksum to rsync command.')
    prsr.add_argument('-d', '--debug', action='store_true',
                      help='Set log level to DEBUG.')
    prsr.add_argument('-D', '--destination', metavar='DIR',
                      help='Use DIR as destination directory. This overrides any value of $DESI_ROOT set.')
    prsr.add_argument('-e', '--exclude', metavar='DIR', nargs='*',
                      help='Exclude DIR from sync. Multiple directories may be specified.')
    prsr.add_argument('-l', '--log', metavar='DIR',
                      default=os.path.join(os.environ['HOME'], 'Documents', 'Logfiles'),
                      help='Use DIR for log files (default %(default)s).')
    prsr.add_argument('-p', '--processes', action='store', type=int,
                      dest='nproc', metavar="N", default=10,
                      help="Number of simultaneous downloads (default %(default)s).")
    prsr.add_argument('-s', '--static', action='store_true', dest='static',
                      help='Also sync static data sets.')
    prsr.add_argument('-S', '--sleep', metavar='TIME', default='15m', dest='sleep',
                      help='Sleep for TIME while waiting for daily transfer to finish (default %(default)s).')
    prsr.add_argument('-t', '--test', action='store_true',
                      help='Test mode. Do not make any changes. Implies -d.')
    prsr.add_argument('-V', '--version', action='version',
                      version='%(prog)s {0}'.format(dtVersion))
    return prsr.parse_args()


def _rsync(src, dst, d, checksum=False):
    """Construct an :command:`rsync` command to transfer `d`.

    Parameters
    ----------
    src : :class:`str`
        Root source directory.
    dst : :class:`str`
        Root destination directory.
    d : :class:`str`
        Directory to transfer relative to `src`, `dst`.
    checksum : :class:`bool`, optional
        If ``True``, pass the ``--checksum`` option to :command:`rsync`.
    """
    cmd = ['/usr/bin/rsync', '--archive', '--verbose',
           '--delete', '--delete-after', '--no-motd',
           '--password-file', os.path.join(os.environ['HOME'], '.desi')]
    if checksum:
        cmd.insert(cmd.index('--verbose'), '--checksum')
    if d in includes:
        cmd += includes[d]
    cmd += [f'{src}/{d}/', f'{dst}/{d}/']
    return cmd


def _get_proc(directories, exclude, src, dst, options, nice=5):
    """Prepare the next download directory for processing.

    Parameters
    ----------
    directories : :class:`list`
        A list of directories to process.
    exclude : :class:`set`
        Do not process directories in this set.
    src : :class:`str`
        Root source directory.
    dst : :class:`str`
        Root destination directory.
    options : :class:`argparse.Namespace`
        The parsed command-line options.
    nice : :class:`int`, optional.
        Lower-priority transfers will be run with this value passed to :func:`os.nice`,
        default 5.

    Returns
    -------
    :class:`tuple`
        A tuple containing information about the process.
    """
    global log

    def preexec_nice():  # pragma: no cover
        os.nice(nice)

    def preexec_pass():  # pragma: no cover
        pass

    try:
        d = directories.pop(0)
        while d in exclude:
            log.warning("%s skipped at user request.", d)
            d = directories.pop(0)
        log_file = os.path.join(options.log,
                                'desi_tucson_transfer_' + d.replace('/', '_') + '.log')
        command = _rsync(src, dst, d, checksum=options.checksum)
        if options.test:
            return (command, log_file, d)
        else:
            log.info(' '.join(command))
            LOG = open(log_file, 'ab')
            if d in priority:
                preexec_fn = preexec_pass
            else:
                log.info("Directory '%s' will be transferred with os.nice(%d)", d, nice)
                preexec_fn = preexec_nice
            return (sub.Popen(command, preexec_fn=preexec_fn, stdout=LOG, stderr=sub.STDOUT), LOG, d)
    except IndexError:
        return (None, None, None)


def running(pid_file):
    """Test for a duplicate process already running.

    Parameters
    ----------
    pid_file : :class:`str`
        Name of file containing a process id.

    Returns
    -------
    :class:`bool`
        ``True`` if a duplicate process is detected.
    """
    if os.path.exists(pid_file):
        with open(pid_file) as p:
            pid = p.read().strip()
        cmd = ['/usr/bin/ps', '-q', pid, '-o', 'comm=']
        log.debug(' '.join(cmd))
        proc = sub.Popen(cmd, stdout=sub.PIPE, stderr=sub.PIPE)
        out, err = proc.communicate()
        if out:
            pname = out.decode('utf-8').strip()
            log.debug(pname)
            log.critical("Running process detected (%s = %s), exiting.", pid, pname)
            return True
        else:
            log.debug("os.remove('%s')", pid_file)
            os.remove(pid_file)
    with open(pid_file, 'w') as p:
        p.write(str(os.getpid()))


def main():
    """Entry point for :command:`desi_tucson_transfer`.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    options = _options()
    debug = options.test or options.debug
    _configure_log(debug)
    #
    # Check for required environment variables
    #
    for e in ('DESISYNC_HOSTNAME', 'DESISYNC_STATUS_URL', 'SCRATCH'):
        try:
            foo = os.environ[e]
        except KeyError:
            log.critical("%s must be set!", e)
            return 1

    #
    # Check other options.
    #
    if options.nproc > 10:
        log.critical("Number of simultaneous transfers %d > 10!", options.nproc)
        return 1
    #
    # Source and destination.
    #
    src = "rsync://{DESISYNC_HOSTNAME}/desi".format(**os.environ)
    if options.destination is None:
        if 'DESI_ROOT' in os.environ:
            dst = os.environ['DESI_ROOT']
        else:
            log.critical("DESI_ROOT must be set, or destination directory set on the command-line (-d DIR)!")
            return 1
    else:
        dst = options.destination
    #
    # Pid file.
    #
    if not options.test:
        if running(os.path.join(options.log, 'desi_tucson_transfer.pid')):
            return 1
    #
    # Wait for daily KPNO -> NERSC transfer to finish.
    #
    try:
        sleepy_time = int(options.sleep)
    except ValueError:
        suffix = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
        for s in suffix:
            if options.sleep.endswith(s):
                try:
                    sleepy_time = int(options.sleep[0:-1]) * suffix[s]
                except ValueError:
                    log.critical("Invalid value for sleep interval: '%s'!", options.sleep)
                    return 1
    log.debug("requests.get('%s')", os.environ['DESISYNC_STATUS_URL'])
    if not options.test:
        while requests.get(os.environ['DESISYNC_STATUS_URL']).status_code != 200:
            log.debug("Daily transfer incomplete, sleeping %s.", options.sleep)
            time.sleep(sleepy_time)
    #
    # Main transfer
    #
    if options.exclude is None:
        exclude = set()
    else:
        exclude = set(options.exclude)
    if options.static:
        directories = static + dynamic
    else:
        directories = dynamic
    proc_pool = dict()
    for p in range(options.nproc):
        proc_key = 'proc{0:03d}'.format(p)
        proc_pool[proc_key] = _get_proc(directories, exclude, src, dst, options)
    while any([v[0] is not None for v in proc_pool.values()]):
        for proc_key in proc_pool:
            proc, LOG, d = proc_pool[proc_key]
            if proc is None:
                status = None
            else:
                if options.test:
                    log.debug("%s: %s -> %s", d, ' '.join(proc), LOG)
                    status = 0
                else:
                    status = proc.poll()
            if status is not None:
                if not options.test:
                    LOG.close()
                if status != 0:
                    log.critical("rsync error detected for %s/%s/! Check logs!", dst, d)
                proc_pool[proc_key] = _get_proc(directories, exclude, src, dst, options)
        if not options.test:
            log.debug("Waiting for jobs to complete, sleeping %s.", options.sleep)
            time.sleep(sleepy_time)
    return 0
