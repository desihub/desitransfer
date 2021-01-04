# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
desitransfer.daemon
===================

Entry point for :command:`desi_transfer_daemon`.
"""
import datetime as dt
import hashlib
import logging
import os
import re
import shutil
import stat
import subprocess as sub
import sys
import time
import traceback
from argparse import ArgumentParser
from collections import namedtuple
from configparser import ConfigParser, ExtendedInterpolation
from logging.handlers import RotatingFileHandler, SMTPHandler
from socket import getfqdn
from tempfile import TemporaryFile
from pkg_resources import resource_filename
from desiutil.log import get_logger
from .common import dir_perm, file_perm, rsync, yesterday, empty_rsync, ensure_scratch
from .status import TransferStatus
from . import __version__ as dtVersion

log = None


def _options():
    """Parse command-line options for :command:`desi_transfer_daemon`.

    Returns
    -------
    :class:`argparse.Namespace`
        The parsed command-line options.
    """
    desc = "Transfer DESI raw data files."
    prsr = ArgumentParser(description=desc)
    prsr.add_argument('-B', '--no-backup', action='store_false', dest='backup',
                      help="Skip NERSC HPSS backups.")
    prsr.add_argument('-c', '--configuration', metavar='FILE',
                      help="Read configuration from FILE.")
    prsr.add_argument('-d', '--debug', action='store_true',
                      help='Set log level to DEBUG.')
    prsr.add_argument('-k', '--kill', metavar='FILE',
                      default=os.path.join(os.environ['HOME'], 'stop_desi_transfer'),
                      help="Exit the script when FILE is detected (default %(default)s).")
    prsr.add_argument('-P', '--no-pipeline', action='store_false', dest='pipeline',
                      help="Only transfer files, don't start the DESI pipeline.")
    prsr.add_argument('-S', '--shadow', action='store_true',
                      help='Observe the actions of another data transfer script but do not make any changes.')
    prsr.add_argument('-V', '--version', action='version',
                      version='%(prog)s {0}'.format(dtVersion))
    return prsr.parse_args()


class TransferDaemon(object):
    """Manage data transfer configuration, options, and operations.

    Parameters
    ----------
    options : :class:`argparse.Namespace`
        The parsed command-line options.
    """
    _link_re = re.compile(r'[0-9]{8}/[0-9]{8}$')
    _directory = namedtuple('_directory', 'source, staging, destination, hpss, expected, checksum')
    _default_configuration = resource_filename('desitransfer', 'data/desi_transfer_daemon.ini')

    def __init__(self, options):
        if options.configuration is None:
            self._ini = self._default_configuration
        else:
            self._ini = options.configuration
        self.test = options.shadow
        self.tape = options.backup
        self.run = options.pipeline
        getlist = lambda x: x.split(',')
        getdict = lambda x: dict([tuple(i.split(':')) for i in x.split(',')])
        self.conf = ConfigParser(defaults=os.environ,
                                 interpolation=ExtendedInterpolation(),
                                 converters={'list': getlist, 'dict': getdict})
        files = self.conf.read(self._ini)
        # assert files[0] == self._ini
        self.sections = [s for s in self.conf.sections()
                         if s not in ('common', 'logging', 'pipeline')]
        self.directories = [self._directory(self.conf[s]['source'],
                                            self.conf[s]['staging'],
                                            self.conf[s]['destination'],
                                            self.conf[s]['hpss'],
                                            self.conf[s].getlist('expected_files'),
                                            self.conf[s]['checksum_file'])
                            for s in self.sections]
        self.scratch = ensure_scratch(self.conf['common']['scratch'], self.conf['common']['alternate_scratch'].split(','))
        self._configure_log(options.debug)
        return

    def _configure_log(self, debug):
        """Re-configure the default logger returned by ``desiutil.log``.

        Parameters
        ----------
        debug : :class:`bool`
            If ``True`` set the log level to ``DEBUG``.
        """
        global log
        conf = self.conf['logging']
        log = get_logger(timestamp=True)
        h = log.parent.handlers[0]
        handler = RotatingFileHandler(conf['filename'],
                                      maxBytes=conf.getint('size'),
                                      backupCount=conf.getint('backups'))
        handler.setFormatter(h.formatter)
        log.parent.removeHandler(h)
        log.parent.addHandler(handler)
        if debug:
            log.setLevel(logging.DEBUG)
        email_from = os.environ['USER'] + '@' + getfqdn()
        handler2 = SMTPHandler('localhost', email_from, conf.getlist('to'),
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

    def pipeline(self, night, exposure, command=None):
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
        rename = self.conf['pipeline'].getdict('commands')
        if command is None:
            cmd = self.conf['pipeline']['exposure']
        else:
            if command in rename:
                cmd = rename[command]
            else:
                cmd = command
        c = [self.conf['pipeline']['ssh'],
             '-q',
             self.conf['pipeline']['host'],
             self.conf['pipeline']['desi_night'],
             cmd,
             '--night', night,
             '--expid', exposure,
             '--nersc', self.conf['pipeline']['host'],
             '--nersc_queue', self.conf['pipeline']['queue'],
             '--nersc_maxnodes', self.conf['pipeline']['nodes']]
        if command is not None:
            c = c[:7] + c[9:]
        log.debug(' '.join(c))
        return c

    def transfer(self):
        """Loop over and transfer all configured directories.
        """
        if self.checksum_lock():
            return
        for d in self.directories:
            log.info('Looking for new data in %s.', d.source)
            try:
                self.directory(d)
            except Exception as e:
                log.critical("Exception detected in transfer of %s!\n\n%s",
                             d.source, traceback.format_exc())

    def checksum_lock(self):
        """See if checksums are being computed at KPNO.

        Returns
        -------
        :class:`bool`
            ``True`` if checksums are being computed.
        """
        cmd = [self.conf['pipeline']['ssh'], '-q', 'dts',
               '/bin/ls', self.conf['common']['checksum_lock']]
        _, out, err = _popen(cmd)
        if out:
            log.info('Checksums are being computed at KPNO.')
            return True
        return False

    def directory(self, d):
        """Data transfer operations for a single destination directory.

        Parameters
        ----------
        d : :func:`collections.namedtuple`
            Configuration for the destination directory.
        """
        status = TransferStatus(os.path.join(os.path.dirname(d.staging),
                                             'status'))
        #
        # Find symlinks at KPNO.
        #
        cmd = [self.conf['pipeline']['ssh'], '-q', 'dts',
               '/bin/find', d.source, '-type', 'l']
        _, out, err = _popen(cmd)
        links = sorted([x for x in out.split('\n') if x])
        if links:
            for l in links:
                if self._link_re.search(l) is None:
                    log.warning("Malformed symlink detected: %s. Skipping.", l)
                else:
                    self.exposure(d, l, status)
        else:
            log.warning('No links found, check connection.')
        #
        # Check for delayed files.
        #
        yst = yesterday()
        now = int(dt.datetime.utcnow().strftime('%H'))
        if now >= self.conf['common'].getint('catchup'):
            self.catchup(d, yst)
        #
        # Are any nights eligible for backup?
        #
        if now >= self.conf['common'].getint('backup'):
            s = self.backup(d, yst)
            if s and self.tape:
                log.debug("status.update('%s', 'all', 'backup')", yst)
                status.update(yst, 'all', 'backup')

    def exposure(self, d, link, status):
        """Data transfer operations for a single exposure.

        Parameters
        ----------
        d : :class:`desitransfer.common.DTSDir`
            Configuration for the destination directory.
        link : :class:`str`
            The exposure path.
        status : :class:`desitransfer.status.TransferStatus`
            The status object associated with `d`.
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
            if not self.test:
                os.makedirs(staging_night, exist_ok=True)
        #
        # Has exposure already been transferred?
        #
        if not os.path.isdir(staging_exposure) and not os.path.isdir(destination_exposure):
            cmd = rsync(os.path.join(d.source, night, exposure), staging_exposure)
            if self.test:
                log.debug(' '.join(cmd))
                rsync_status = '0'
            else:
                rsync_status, out, err = _popen(cmd)
        else:
            log.debug('%s already transferred.', staging_exposure)
            rsync_status = 'done'
        #
        # Transfer complete.
        #
        if rsync_status == '0':
            log.debug("status.update('%s', '%s', 'rsync')", night, exposure)
            status.update(night, exposure, 'rsync')
            #
            # Check permissions.
            #
            lock_directory(staging_exposure, self.test)
            #
            # Verify checksums.
            #
            checksum_file = os.path.join(staging_exposure,
                                         d.checksum.format(night=night, exposure=exposure))
            if os.path.exists(checksum_file):
                checksum_status = verify_checksum(checksum_file)
            elif not os.path.exists(staging_exposure):
                #
                # This can happen in shadow mode.
                #
                log.debug("%s does not exist, ignore checksum error.", staging_exposure)
                checksum_status = 1
            else:
                log.warning("No checksum file for %s/%s!", night, exposure)
                checksum_status = 0
            #
            # Did we pass checksums?
            #
            if checksum_status == 0:
                log.debug("status.update('%s', '%s', 'checksum')", night, exposure)
                status.update(night, exposure, 'checksum')
                #
                # Set up DESI_SPECTRO_DATA.
                #
                if not os.path.isdir(destination_night):
                    log.debug("os.makedirs('%s', exist_ok=True)", destination_night)
                    log.debug("os.chmod('%s', 0o%o)", destination_night, dir_perm)
                    if not self.test:
                        os.makedirs(destination_night, exist_ok=True)
                        os.chmod(destination_night, dir_perm)
                #
                # Move data into DESI_SPECTRO_DATA.
                #
                if not os.path.isdir(destination_exposure):
                    log.debug("shutil.move('%s', '%s')", staging_exposure, destination_night)
                    if not self.test:
                        shutil.move(staging_exposure, destination_night)
                #
                # Is this a "realistic" exposure?
                #
                if (self.run and
                    all([os.path.exists(os.path.join(destination_exposure,
                                                     f.format(exposure=exposure)))
                         for f in d.expected])):
                    #
                    # Run update
                    #
                    cmd = self.pipeline(night, exposure)
                    if not self.test:
                        _, out, err = _popen(cmd)
                    log.debug("status.update('%s', '%s', 'pipeline')", night, exposure)
                    status.update(night, exposure, 'pipeline')
                    for k in ('flats', 'arcs', 'science'):
                        if os.path.exists(os.path.join(destination_exposure,
                                                       '{0}-{1}-{2}.done'.format(k, night, exposure))):
                            cmd = self.pipeline(night, exposure, command=k)
                            if not self.test:
                                _, out, err = _popen(cmd)
                            log.debug("status.update('%s', '%s', 'pipeline', last='%s')",
                                      night, exposure, k)
                            status.update(night, exposure, 'pipeline', last=k)
                else:
                    log.info("%s/%s appears to be test data. Skipping pipeline activation.", night, exposure)
            else:
                log.error("Checksum problem detected for %s/%s!", night, exposure)
                log.debug("status.update('%s', '%s', 'checksum', failure=True)", night, exposure)
                status.update(night, exposure, 'checksum', failure=True)
        elif rsync_status == 'done':
            #
            # Do nothing, successfully.
            #
            pass
        else:
            log.error('rsync problem detected for %s/%s!', night, exposure)
            log.debug("status.update('%s', '%s', 'rsync', failure=True)", night, exposure)
            status.update(night, exposure, 'rsync', failure=True)

    def catchup(self, d, night):
        """Do a "catch-up" transfer to catch delayed files in the morning, rather than at noon.

        Parameters
        ----------
        d : :func:`collections.namedtuple`
            Configuration for the destination directory.
        night : :class:`str`
            Night to check.

        Notes
        -----
        * 07:00 MST = 14:00 UTC.
        * This script can do nothing about exposures that were never linked
          into the DTS area at KPNO in the first place.
        """
        if os.path.isdir(os.path.join(d.destination, night)):
            ketchup_file = d.destination.replace('/', '_')
            sync_file = os.path.join(self.scratch,
                                     'ketchup_{0}_{1}.txt'.format(ketchup_file, night))
            if self.test:
                sync_file = sync_file.replace('.txt', '.shadow.txt')
            if os.path.exists(sync_file):
                log.debug("%s detected, catch-up transfer is done.", sync_file)
            else:
                cmd = rsync(os.path.join(d.source, night),
                            os.path.join(d.destination, night), test=True)
                rsync_status, out, err = _popen(cmd)
                with open(sync_file, 'w') as sf:
                    sf.write(out)
                if empty_rsync(out):
                    log.info('No files appear to have changed in %s.', night)
                else:
                    log.warning('New files detected in %s!', night)
                    rsync_night(d.source, d.destination, night, self.test)
        else:
            log.warning("No data from %s detected, skipping catch-up transfer.", night)

    def backup(self, d, night):
        """Final sync and backup for a specific night.

        Parameters
        ----------
        d : :func:`collections.namedtuple`
            Configuration for the destination directory.
        night : :class:`str`
            Night to check.

        Returns
        -------
        :class:`bool`
            ``True`` indicates the backup ran to completion and the
            the transfer status should be updated to reflect that.

        Notes
        -----
        * 12:00 MST = 19:00 UTC, plus one hour just to be safe, so after 20:00 UTC.
        """
        if os.path.isdir(os.path.join(d.destination, night)):
            hpss_file = d.hpss.replace('/', '_')
            ls_file = os.path.join(self.scratch, hpss_file + '.txt')
            if self.test:
                ls_file = ls_file.replace('.txt', '.shadow.txt')
            log.debug("os.remove('%s')", ls_file)
            try:
                os.remove(ls_file)
            except FileNotFoundError:
                log.debug("Failed to remove %s because it didn't exist. That's OK.", ls_file)
            cmd = ['/usr/common/mss/bin/hsi', '-O', ls_file,
                   'ls', '-l', d.hpss]
            if self.tape:
                _, out, err = _popen(cmd)
                with open(ls_file) as l:
                    data = l.read()
                backup_files = [l.split()[-1] for l in data.split('\n') if l]
            else:
                backup_files = []
            backup_file = hpss_file + '_' + night + '.tar'
            #
            # Both a .tar and a .tar.idx file should be present.
            #
            if backup_file in backup_files and backup_file + '.idx' in backup_files:
                log.debug("Backup of %s already complete.", night)
                return False
            else:
                #
                # Run a final sync of the night and see if anything changed.
                # This isn't supposed to be necessary, but during
                # commissioning, all kinds of crazy stuff might happen.
                #
                # sync_file = sync_file.replace('ketchup', 'final_sync')
                cmd = rsync(os.path.join(d.source, night),
                            os.path.join(d.destination, night), test=True)
                rsync_status, out, err = _popen(cmd)
                if empty_rsync(out):
                    log.info('No files appear to have changed in %s.', night)
                else:
                    log.warning('New files detected in %s!', night)
                    rsync_night(d.source, d.destination, night, self.test)
                #
                # Issue HTAR command.
                #
                if self.tape:
                    start_dir = os.getcwd()
                    log.debug("os.chdir('%s')", d.destination)
                    os.chdir(d.destination)
                    cmd = ['/usr/common/mss/bin/htar',
                           '-cvhf', os.path.join(d.hpss, backup_file),
                           '-H', 'crc:verify=all',
                           night]
                    if self.test:
                        log.debug(' '.join(cmd))
                    else:
                        _, out, err = _popen(cmd)
                    log.debug("os.chdir('%s')", start_dir)
                    os.chdir(start_dir)
                else:
                    log.info('Tape backup disabled by user request.')
                return True
        else:
            log.warning("No data from %s detected, skipping HPSS backup.", night)
            return False


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


def verify_checksum(checksum_file):
    """Verify checksums supplied with the raw data.

    Parameters
    ----------
    checksum_file : :class:`str`
        The checksum file.

    Returns
    -------
    :class:`int`
        An integer that indicates the number of checksum mismatches.  A
        negative integer indicates that the checksum file lists files
        that are not present.  This is considered more serious than the
        case where files are present but not listed in the checksum file.
    """
    with open(checksum_file) as c:
        data = c.read()
    #
    # The trailing \n at the end of the file should make the length of
    # lines equal to the length of files.
    #
    lines = data.split('\n')
    d = os.path.dirname(checksum_file)
    files = os.listdir(d)
    errors = 0
    n_lines = len(lines) - len(files)
    if n_lines > 0:
        log.error("%s lists %d file(s) that are not present!",
                  checksum_file, n_lines)
        return -1*n_lines
    if n_lines < 0:
        log.error("%d files are not listed in %s!", -1*n_lines, checksum_file)
    digest = dict([(l.split()[1], l.split()[0]) for l in lines if l])
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
                log.error("Checksum mismatch for %s in %s!", ff, checksum_file)
                errors += 1
    return errors


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


def main():
    """Entry point for :command:`desi_transfer_daemon`.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    options = _options()
    transfer = TransferDaemon(options)
    sleep = transfer.conf['common'].getint('sleep')
    while True:
        log.info('Starting transfer loop; desitransfer version = %s.',
                 dtVersion)
        if os.path.exists(options.kill):
            log.info("%s detected, shutting down transfer daemon.",
                     options.kill)
            return 0
        transfer.transfer()
        time.sleep(sleep*60)
    return 0
