# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
desitransfer.tucson
===================

Entry point for :command:`desi_tucson_transfer`.
"""
import os
import subprocess as sub
from argparse import ArgumentParser
import requests
from . import __version__ as dtVersion

static = ['cmx',
          'datachallenge',
          'engineering/2021_summer_illumination_checks',
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
          'sv',
          'target/cmx_files',]


dynamic = ['engineering/donut',
           'engineering/focalplane'
           'software/AnyConnect'
           'spectro/data'
           'spectro/nightwatch/kpno'
           'spectro/redux/daily'
           'spectro/redux/daily/exposures'
           'spectro/redux/daily/preproc'
           'spectro/redux/daily/tiles'
           'spectro/staging/lost+found'
           'target/catalogs'
           'target/secondary',]


includes = {'spectro/desi_spectro_calib': ["--exclude", ".svn"],
            # 'spectro/nightwatch': ["--include", "kpno/***", "--exclude", "*"],
            'spectro/redux/daily': ["--exclude", "*.tmp", "--exclude", "preproc-*.fits", "--exclude", "attic", "--exclude", "exposures", "--exclude", "preproc", "--exclude", "temp", "--exclude", "tiles"],
            'spectro/redux/daily/exposures': ["--exclude", "*.tmp"],
            'spectro/redux/daily/preproc': ["--exclude", "*.tmp", "--exclude", "preproc-*.fits"],
            'spectro/redux/daily/tiles': ["--exclude", "*.tmp"],
            'spectro/templates/basis_templates': ["--exclude", ".svn", "--exclude", "basis_templates_svn-old"],}


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


def main():
    """Entry point for :command:`desi_tucson_transfer`.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    options = _options()
    debug = options.test or options.debug
    #
    # Check for required environment variables
    #
    for e in ('DESISYNC_HOSTNAME', 'DESISYNC_STATUS_URL', 'CSCRATCH'):
        try:
            foo = os.environ[e]
        except KeyError:
            print(f"ERROR: {e} must be set!")
            return 1
    #
    # Source and destination.
    #
    src = "rsync://{DESISYNC_HOSTNAME}/desi".format(**os.environ)
    if options.dst is None:
        if 'DESI_ROOT' in os.environ:
            dst = os.environ['DESI_ROOT']
        else:
            print("ERROR: DESI_ROOT must be set, or destination directory set on the command-line (-d DIR)!")
            return 1
    else:
        dst = options.dst
    #
    # Pid file.
    #
    pid_file = os.path.join(options.log, 'desi_tucson_transfer.pid')
    if os.path.exists(pid_file):
        with open(pid_file) as p:
            pid = p.read()
        cmd = ['/bin/ps', '-q', pid, '-o', 'comm=']
        proc = sup.Popen(cmd, stdout=sub.PIPE, stderr=sub.PIPE)
        out, err = proc.communicate()
        if out:
            print("WARNING: Running process detected ({pid} = {out}), exiting.")
            return 1
        else:
            os.remove(pid_file)
    with open(pid_file, 'w') as p:
        p.write(str(os.getpid()))
    #
    # Wait for daily KPNO -> NERSC transfer to finish.
    #
    if options.test:
        print("DEBUG: requests.get('{DESISYNC_STATUS_URL}')".format(**os.environ))
    else:
        while requests.get(os.environ['DESISYNC_STATUS_URL']).status_code != 200:
            if options.debug:
                print("DEBUG: Daily transfer incomplete, sleeping {0}.".format(options.sleep))
                time.sleep()
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
    for d in directories:
        if d in exclude:
            print(f"INFO: {d} skipped at user request.")
        else:
            command = _rsync(src, dst, d, checksum=options.checksum)
            if options.test:
                print("DEBUG: " + ' '.join(command))
    return 0