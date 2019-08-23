# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
desitransfer.daemon
===================

Entry point for :command:`desi_transfer_daemon`.
"""
import datetime as dt
import hashlib
import json
import logging
import os
import shutil
import stat
import subprocess as sub
import sys
from argparse import ArgumentParser
from logging.handlers import RotatingFileHandler, SMTPHandler
from socket import getfqdn
from tempfile import TemporaryFile
from pkg_resources import resource_filename
from desiutil.log import get_logger
from .common import DTSDir, dir_perm, file_perm, expand_environment, rsync, yesterday
from .status import TransferStatus


log = None


expected_files = ('desi-{exposure}.fits.fz',
                  'fibermap-{exposure}.fits',
                  'guider-{exposure}.fits.fz')


class PipelineCommand(object):
    """Simple object for generating pipeline commands.

    Parameters
    ----------
    host : :class:`str`
        Run the pipeline on this NERSC system.
    ssh : :class:`str`, optional
        SSH command to use.
    queue : :class:`str`, optional
        NERSC queue to use.
    nodes : :class:`int`, optional
        Value for the ``--nersc_maxnodes`` option.
    """
    desi_night = os.path.realpath(os.path.join(os.environ['HOME'],
                                               'bin',
                                               'wrap_desi_night.sh'))

    def __init__(self, host, ssh='ssh', queue='realtime', nodes=25):
        self.host = host
        self.ssh = ssh
        self.queue = queue
        self.nodes = str(nodes)
        return

    def command(self, night, exposure, command='update'):
        """Generate a ``desi_night`` command to pass to the pipeline.

        Parameters
        ----------
        night : :class:`str`
            Night of observation.
        exposure : :class:`str`
            Exposure number.
        command : :class:`str`, optional
            Specific command to pass to ``desi_night``.

        Returns
        -------
        :class:`list`
            A command suitable for passing to :class:`subprocess.Popen`.
        """
        cmd = command
        if command == 'science':
            cmd = 'redshifts'
        c = [self.ssh, '-q', self.host,
             self.desi_night, cmd,
             '--night', night,
             '--expid', exposure,
             '--nersc', self.host,
             '--nersc_queue', self.queue,
             '--nersc_maxnodes', self.nodes]
        log.debug(' '.join(c))
        return c


def _config():
    """Wrap configuration so that module can be imported without
    environment variables set.
    """
    return [DTSDir('/data/dts/exposures/raw',
                   os.path.realpath(os.path.join(os.environ['DESI_ROOT'], 'spectro', 'staging', 'raw')),
                   os.path.realpath(os.environ['DESI_SPECTRO_DATA']),
                   'desi/spectro/data'), ]


def _options(*args):
    """Parse command-line options for :command:`desi_transfer_daemon`.

    Parameters
    ----------
    args : iterable
        Arguments to the function will be parsed for testing purposes.

    Returns
    -------
    :class:`argparse.Namespace`
        The parsed command-line options.
    """
    desc = "Transfer DESI raw data files."
    prsr = ArgumentParser(prog=os.path.basename(sys.argv[0]), description=desc)
    prsr.add_argument('-b', '--backup', metavar='H', type=int, default=20,
                      help='UTC time in hours to trigger HPSS backups (default %(default)s:00 UTC). Disable this with an invalid hour, e.g. 30.')
    prsr.add_argument('-c', '--catchup', metavar='H', type=int, default=14,
                      help='UTC time in hours to look for delayed files (default %(default)s:00 UTC). Disable this with an invalid hour, e.g. 30.')
    prsr.add_argument('-d', '--debug', action='store_true',
                      help='Set log level to DEBUG.')
    prsr.add_argument('-e', '--rsh', metavar='COMMAND', dest='ssh', default='/bin/ssh',
                      help="Use COMMAND for remote shell access (default '%(default)s').")
    prsr.add_argument('-k', '--kill', metavar='FILE',
                      default=os.path.join(os.environ['HOME'], 'stop_desi_transfer'),
                      help="Exit the script when FILE is detected (default %(default)s).")
    prsr.add_argument('-n', '--nersc', default='cori', metavar='NERSC_HOST',
                      help="Trigger DESI pipeline on this NERSC system (default %(default)s).")
    prsr.add_argument('-P', '--no-pipeline', action='store_false', dest='pipeline',
                      help="Only transfer files, don't start the DESI pipeline.")
    prsr.add_argument('-s', '--sleep', metavar='M', type=int, default=10,
                      help='Sleep M minutes before checking for new data (default %(default)s minutes).')
    prsr.add_argument('-S', '--shadow', action='store_true',
                      help='Observe the actions of another data transfer script but do not make any changes.')
    return prsr.parse_args()


def _read_configuration():
    with open(resource_filename('desitransfer', 'data/desi_transfer_daemon.json')) as j:
        c = json.load(j)
    return [expand_environment(cc) for cc in c]


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


def _configure_log(debug, size=100000000, backups=100):
    """Re-configure the default logger returned by ``desiutil.log``.

    Parameters
    ----------
    debug : :class:`bool`
        If ``True`` set the log level to ``DEBUG``.
    size : :class:`int`, optional
        Rotate log file after N bytes.
    backups : :class:`int`, optional
        Keep N old log files.
    """
    global log
    log_filename = os.path.realpath(os.path.join(os.environ['DESI_ROOT'],
                                                 'spectro', 'staging', 'logs',
                                                 'desi_transfer_daemon.log'))
    log = get_logger(timestamp=True)
    h = log.parent.handlers[0]
    handler = RotatingFileHandler(log_filename, maxBytes=size,
                                  backupCount=backups)
    handler.setFormatter(h.formatter)
    log.parent.removeHandler(h)
    log.parent.addHandler(handler)
    if debug:
        log.setLevel(logging.DEBUG)
    email_from = os.environ['USER'] + '@' + getfqdn()
    email_to = ['desi-data@desi.lbl.gov', ]
    handler2 = SMTPHandler('localhost', email_from, email_to,
                           'Critical error reported by desi_transfer_daemon!')
    fmt = """Greetings,

At %(asctime)s, desi_transfer_daemon failed with this message:

%(message)s

Kia ora koutou,
The DESI Collaboration Account
"""
    formatter2 = logging.Formatter(fmt, datefmt='%Y-%m-%d %H:%M:%S %Z')
    handler2.setFormatter(formatter2)
    handler2.setLevel(logging.CRITICAL)
    log.parent.addHandler(handler2)


def check_exposure(destination, exposure):
    """Ensure that all files associated with an exposure have arrived.

    Parameters
    ----------
    destination : :class:`str`
        Delivery directory, typically ``DESI_SPECTRO_DATA/NIGHT``.
    exposure : :class:`str`
        Exposure number.

    Returns
    -------
    :class:`bool`
        ``True`` if all files have arrived.
    """
    return all([os.path.exists(os.path.join(destination,
                                            f.format(exposure=exposure)))
                for f in expected_files])


def verify_checksum(checksum_file, files):
    """Verify checksums supplied with the raw data.

    Parameters
    ----------
    checksum_file : :class:`str`
        The checksum file.
    files : :class:`list`
        The list of files in the directory containing the checksum file.

    Returns
    -------
    :class:`int`
        An integer that indicates the number of checksum mismatches.  A
        value of -1 indicates that the lines in the checksum file does not
        match the number of files in the exposure.
    """
    with open(checksum_file) as c:
        data = c.read()
    #
    # The trailing \n at the end of the file should make the length of
    # lines equal to the length of files.
    #
    lines = data.split('\n')
    errors = 0
    if len(lines) == len(files):
        digest = dict([(l.split()[1], l.split()[0]) for l in lines if l])
        d = os.path.dirname(checksum_file)
        for f in files:
            ff = os.path.join(d, f)
            if ff != checksum_file:
                with open(ff, 'rb') as fp:
                    h = hashlib.sha256(fp.read()).hexdigest()
                try:
                    hh = digest[f]
                except KeyError:
                    hh = ''
                    log.error("%s does not appear in %s!", ff, checksum_file)
                    errors += 1
                if hh == h:
                    log.debug("%s is valid.", ff)
                elif hh == '':
                    pass
                else:
                    log.error("Checksum mismatch for %s!", ff)
                    errors += 1
        return errors
    else:
        log.error("%s does not match the number of files!", checksum_file)
        return -1


def unlock_directory(directory, test=False):
    """Set a directory and its contents user-writeable.

    Parameters
    ----------
    directory : :class:`str`
        Directory to unlock.
    test : :class:`bool`, optional
        If ``True``, only print the commands.
    """
    for dirpath, dirnames, filenames in os.walk(directory):
        log.debug("os.chmod('%s', 0o%o)", dirpath, dir_perm | stat.S_IWUSR)
        if not test:
            os.chmod(dirpath, dir_perm | stat.S_IWUSR)
        for f in filenames:
            log.debug("os.chmod('%s', 0o%o)", os.path.join(dirpath, f),
                      file_perm | stat.S_IWUSR)
            if not test:
                os.chmod(os.path.join(dirpath, f), file_perm | stat.S_IWUSR)


def lock_directory(directory, test=False):
    """Set a directory and its contents read-only.

    Parameters
    ----------
    directory : :class:`str`
        Directory to lock.
    test : :class:`bool`, optional
        If ``True``, only print the commands.
    """
    for dirpath, dirnames, filenames in os.walk(directory):
        log.debug("os.chmod('%s', 0o%o)", dirpath, dir_perm)
        if not test:
            os.chmod(dirpath, dir_perm)
        for f in filenames:
            log.debug("os.chmod('%s', 0o%o)", os.path.join(dirpath, f), file_perm)
            if not test:
                os.chmod(os.path.join(dirpath, f), file_perm)


def rsync_night(source, destination, night, test=False):
    """Run an rsync command on an entire `night`, for example, to pick up
    delayed files.

    Parameters
    ----------
    source : :class:`str`
        Source directory.
    destination : :class:`str`
        Destination directory.
    night : :class:`str`
        Night directory.
    test : :class:`bool`, optional
        If ``True``, only print the commands.
    """
    #
    # Unlock files.
    #
    unlock_directory(os.path.join(destination, night), test)
    #
    # Run rsync.
    #
    cmd = rsync(os.path.join(source, night),
                os.path.join(destination, night))
    if test:
        log.debug(' '.join(cmd))
    else:
        rsync_status, out, err = _popen(cmd)
    #
    # Lock files.
    #
    lock_directory(os.path.join(destination, night), test)


def transfer_directory(d, options, pipeline):
    """Data transfer operations for a single destination directory.

    Parameters
    ----------
    d : :class:`desitransfer.common.DTSDir`
        Configuration for the destination directory.
    options : :class:`argparse.Namespace`
        The command-line options.
    pipeline : :class:`desitransfer.daemon.PipelineCommand`
        The pipeline command construction object.
    """
    status = TransferStatus(os.path.join(os.path.dirname(d.staging), 'status'))
    #
    # Find symlinks at KPNO.
    #
    cmd = [options.ssh, '-q', 'dts', '/bin/find', d.source, '-type', 'l']
    _, out, err = _popen(cmd)
    links = sorted([x for x in out.split('\n') if x])
    if links:
        for l in links:
            transfer_exposure(d, options, l, status, pipeline)
    else:
        log.warning('No links found, check connection.')
    #
    # WARNING: some of the auxilliary files below were created under
    # the assumption that only one source directory exists at KPNO and
    # only one destination directory exists at NERSC.  This should be
    # fixed now, but watch out for this.
    #
    # Do a "catch-up" transfer to catch delayed files in the morning,
    # rather than at noon.
    # 07:00 MST = 14:00 UTC.
    # This script can do nothing about exposures that were never linked
    # into the DTS area at KPNO in the first place.
    #
    yst = yesterday()
    now = int(dt.datetime.utcnow().strftime('%H'))
    if now >= options.catchup:
        if os.path.isdir(os.path.join(d.destination, yst)):
            ketchup_file = d.destination.replace('/', '_')
            sync_file = os.path.join(os.environ['CSCRATCH'],
                                     'ketchup_{0}_{1}.txt'.format(ketchup_file, yst))
            if options.shadow:
                sync_file.replace('.txt', '.shadow.txt')
            if os.path.exists(sync_file):
                log.debug("%s detected, catch-up transfer is done.", sync_file)
            else:
                cmd = rsync(os.path.join(d.source, yst),
                            os.path.join(d.destination, yst), test=True)
                rsync_status, out, err = _popen(cmd)
                with open(sync_file, 'w') as sf:
                    sf.write(out)
                if empty_rsync(out):
                    log.info('No files appear to have changed in %s.', yst)
                else:
                    log.warning('New files detected in %s!', yst)
                    rsync_night(d.source, d.destination, yst, options.shadow)
        else:
            log.warning("No data from %s detected, skipping catch-up transfer.", yst)
    #
    # Are any nights eligible for backup?
    # 12:00 MST = 19:00 UTC.
    # Plus one hour just to be safe, so after 20:00 UTC.
    #
    if now >= options.backup:
        if os.path.isdir(os.path.join(d.destination, yst)):
            hpss_file = d.hpss.replace('/', '_')
            ls_file = os.path.join(os.environ['CSCRATCH'], hpss_file + '.txt')
            if options.shadow:
                ls_file = ls_file.replace('.txt', '.shadow.txt')
            log.debug("os.remove('%s')", ls_file)
            os.remove(ls_file)
            cmd = ['/usr/common/mss/bin/hsi', '-O', ls_file,
                   'ls', '-l', d.hpss]
            _, out, err = _popen(cmd)
            #
            # Both a .tar and a .tar.idx file should be present.
            #
            with open(ls_file) as l:
                data = l.read()
            backup_files = [l.split()[-1] for l in data.split('\n') if l]
            backup_file = hpss_file + '_' + yst + '.tar'
            if backup_file in backup_files and backup_file + '.idx' in backup_files:
                log.debug("Backup of %s already complete.", yst)
            else:
                #
                # Run a final sync of the night and see if anything changed.
                # This isn't supposed to be necessary, but during
                # commissioning, all kinds of crazy stuff might happen.
                #
                # sync_file = sync_file.replace('ketchup', 'final_sync')
                cmd = rsync(os.path.join(d.source, yst),
                            os.path.join(d.destination, yst), test=True)
                rsync_status, out, err = _popen(cmd)
                if empty_rsync(out):
                    log.info('No files appear to have changed in %s.', yst)
                else:
                    log.warning('New files detected in %s!', yst)
                    rsync_night(d.source, d.destination, yst, options.shadow)
                #
                # Issue HTAR command.
                #
                start_dir = os.getcwd()
                log.debug("os.chdir('%s')", d.destination)
                os.chdir(d.destination)
                cmd = ['/usr/common/mss/bin/htar',
                       '-cvhf', os.path.join(d.hpss, backup_file),
                       '-H', 'crc:verify=all',
                       yst]
                if options.shadow:
                    log.debug(' '.join(cmd))
                else:
                    _, out, err = _popen(cmd)
                log.debug("os.chdir('%s')", start_dir)
                os.chdir(start_dir)
                status.update(night, 'all', 'backup')
        else:
            log.warning("No data from %s detected, skipping HPSS backup.", yst)


def transfer_exposure(d, options, link, status, pipeline):
    """Data transfer operations for a single exposure.

    Parameters
    ----------
    d : :class:`desitransfer.common.DTSDir`
        Configuration for the destination directory.
    options : :class:`argparse.Namespace`
        The command-line options.
    link : :class:`str`
        The exposure path.
    status : :class:`desitransfer.status.TransferStatus`
        The status object associated with `d`.
    pipeline : :class:`desitransfer.daemon.PipelineCommand`
        The pipeline command construction object.
    """
    exposure = os.path.basename(link)
    night = os.path.basename(os.path.dirname(link))
    staging_night = os.path.join(d.staging, night)
    destination_night = os.path.join(d.destination, night)
    staging_exposure = os.path.join(staging_night, exposure)
    destination_exposure = os.path.join(destination_night, exposure)
    #
    # New night detected?
    #
    if not os.path.isdir(staging_night):
        log.debug("os.makedirs('%s', exist_ok=True)", staging_night)
        if not options.shadow:
            os.makedirs(staging_night, exist_ok=True)
    #
    # Has exposure already been transferred?
    #
    if not os.path.isdir(staging_exposure) and not os.path.isdir(destination_exposure):
        cmd = rsync(os.path.join(d.source, night, exposure), staging_exposure)
        if options.shadow:
            log.debug(' '.join(cmd))
            rsync_status = '0'
        else:
            rsync_status, out, err = _popen(cmd)
    else:
        log.info('%s already transferred.', staging_exposure)
        rsync_status = 'done'
    #
    # Transfer complete.
    #
    if rsync_status == '0':
        status.update(night, exposure, 'rsync')
        #
        # Check permissions.
        #
        lock_directory(staging_exposure, options.shadow)
        #
        # Verify checksums.
        #
        exposure_files = os.listdir(staging_exposure)
        checksum_file = os.path.join(staging_exposure, "checksum-{0}-{1}.sha256sum".format(night, exposure))
        if os.path.exists(checksum_file):
            checksum_status = verify_checksum(checksum_file, exposure_files)
        else:
            log.warning("No checksum file for %s/%s!", night, exposure)
            checksum_status = 0
        #
        # Did we pass checksums?
        #
        if checksum_status == 0:
            status.update(night, exposure, 'checksum')
            #
            # Set up DESI_SPECTRO_DATA.
            #
            if not os.path.isdir(destination_night):
                log.debug("os.makedirs('%s', exist_ok=True)", destination_night)
                if not options.shadow:
                    os.makedirs(destination_night, exist_ok=True)
            #
            # Move data into DESI_SPECTRO_DATA.
            #
            if not os.path.isdir(destination_exposure):
                log.debug("shutil.move('%s', '%s')", staging_exposure, destination_night)
                if not options.shadow:
                    shutil.move(staging_exposure, destination_night)
            #
            # Is this a "realistic" exposure?
            #
            if options.pipeline and check_exposure(destination_exposure, exposure):
                #
                # Run update
                #
                cmd = pipeline.command(night, exposure)
                if not options.shadow:
                    _, out, err = _popen(cmd)
                done = False
                for k in ('flats', 'arcs', 'science'):
                    if os.path.exists(os.path.join(destination_exposure, '{0}-{1}-{2}.done'.format(k, night, exposure))):
                        cmd = pipeline.command(night, exposure, command=k)
                        if not options.shadow:
                            _, out, err = _popen(cmd)
                        status.update(night, exposure, 'pipeline', last=k)
                        done = True
                if not done:
                    status.update(night, exposure, 'pipeline')
            else:
                log.info("%s/%s appears to be test data. Skipping pipeline activation.", night, exposure)
        else:
            log.error("Checksum problem detected for %s/%s!", night, exposure)
            status.update(night, exposure, 'checksum', failure=True)
    elif rsync_status == 'done':
        #
        # Do nothing, successfully.
        #
        pass
    else:
        log.error('rsync problem detected!')
        status.update(night, exposure, 'rsync', failure=True)


def main():
    """Entry point for :command:`desi_transfer_daemon`.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    options = _options()
    _configure_log(options.debug)
    pipeline = PipelineCommand(options.nersc, ssh=options.ssh)
    transfer_directories = _config()
    while True:
        log.info('Starting transfer loop.')
        if os.path.exists(options.kill):
            log.info("%s detected, shutting down transfer daemon.", options.kill)
            return 0
        for d in transfer_directories:
            transfer_directory(d, options, pipeline)
        time.sleep(options.sleep*60)
    return 0
