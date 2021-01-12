# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
desitransfer.common
===================

Code needed by all scripts.
"""
import datetime as dt
import os
import re
import stat
import pytz


dir_perm = (stat.S_ISGID |
            stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
            stat.S_IRGRP | stat.S_IXGRP)  # 0o2750
file_perm = stat.S_IRUSR | stat.S_IRGRP    # 0o0440


def empty_rsync(out):
    """Scan rsync output for files to be transferred.

    Parameters
    ----------
    out : :class:`str`
        Output from :command:`rsync`.

    Returns
    -------
    :class:`bool`
        ``True`` if there are no files to transfer.
    """
    rr = re.compile(r'(receiving|sent [0-9]+ bytes|total size)')
    return all([rr.match(l) is not None for l in out.split('\n') if l])


def new_exposures(out):
    """Scan rsync output for exposures to be transferred.

    Parameters
    ----------
    out : :class:`str`
        Output from :command:`rsync`.

    Returns
    -------
    :class:`set`
        The unique exposure numbers detected in `out`.
    """
    e = set()
    e_re = re.compile(r'([0-9]{8})/?')
    for l in out.split('\n'):
        m = e_re.match(l)
        if m is not None:
            e.add(m.groups()[0])
    return e


def rsync(s, d, test=False, config='dts'):
    """Set up rsync command.

    Parameters
    ----------
    s : :class:`str`
        Source directory.
    d : :class:`str`
        Destination directory.
    test : :class:`str`, optional
        If ``True``, add ``--dry-run`` to the command.
    config : :class:`str`, optional
        Pass this configuration to the ssh command.

    Returns
    -------
    :class:`list`
        A list suitable for passing to :class:`subprocess.Popen`.
    """
    c = ['/bin/rsync', '--verbose', '--recursive',
         '--copy-dirlinks', '--times', '--omit-dir-times',
         config + ':' + s + '/', d + '/']
    if test:
        c.insert(1, '--dry-run')
    return c


def stamp(zone='US/Pacific'):
    """Simple timestamp.

    Parameters
    ----------
    zone : :class:`str`, optional
        Operational timezone.

    Returns
    -------
    :class:`str`
        A nicely-formatted timestamp.
    """
    tz = pytz.timezone(zone)
    n = dt.datetime.utcnow().replace(tzinfo=pytz.utc)
    return n.astimezone(tz).strftime('%Y-%m-%d %H:%M:%S %Z')


def ensure_scratch(directories):
    """Try an alternate temporary directory if the primary temporary directory
    is unavilable.

    Parameters
    ----------
    directories : :class:`list`
        A list of candidate directories.

    Returns
    -------
    :class:`str`
        The first available temporary directory found.
    """
    for d in directories:
        try:
            l = os.listdir(d)
        except FileNotFoundError:
            continue
        return d


def yesterday():
    """Yesterday's date in DESI "NIGHT" format, YYYYMMDD.
    """
    return (dt.datetime.now() - dt.timedelta(seconds=86400)).strftime('%Y%m%d')


def today():
    """Today's date in DESI "NIGHT" format, YYYYMMDD.
    """
    return (dt.datetime.now() - dt.timedelta(7/24+0.5)).strftime('%Y%m%d')
