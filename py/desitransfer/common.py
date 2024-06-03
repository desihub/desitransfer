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
import time
import pytz

MST = pytz.timezone('America/Phoenix')

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
    return all([rr.match(out_line) is not None for out_line in out.split('\n') if out_line])


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
    for out_line in out.split('\n'):
        m = e_re.match(out_line)
        if m is not None:
            e.add(m.groups()[0])
    return e


def rsync(s, d, test=False, config='dts', reverse=False):
    """Set up rsync command.

    Parameters
    ----------
    s : :class:`str`
        Source directory.
    d : :class:`str`
        Destination directory.
    test : :class:`bool`, optional
        If ``True``, add ``--dry-run`` to the command.
    config : :class:`str`, optional
        Pass this configuration to the ssh command.
    reverse : :class:`bool`
        If ``True``, attach `config` to `d` instead of `s`.

    Returns
    -------
    :class:`list`
        A list suitable for passing to :class:`subprocess.Popen`.
    """
    c = ['/bin/rsync', '--verbose', '--recursive',
         '--copy-dirlinks', '--times', '--omit-dir-times']
    if reverse:
        c += [s + '/', config + ':' + d + '/']
    else:
        c += [config + ':' + s + '/', d + '/']
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
            dir_list = os.listdir(d)
        except FileNotFoundError:
            continue
        return d


def yesterday():
    """Yesterday's date in DESI "NIGHT" format, YYYYMMDD.
    """
    return (dt.datetime.now() - dt.timedelta(seconds=86400)).strftime('%Y%m%d')


def today():
    """Today's date in DESI "NIGHT" format, YYYYMMDD.

    This formulation, with the offset ``7/24+0.5``, is inherited from previous
    nightwatch transfer scripts.
    """
    return (dt.datetime.utcnow() - dt.timedelta(7 / 24 + 0.5)).strftime('%Y%m%d')


def idle_time(start=8, end=12, tz=None):
    """Determine whether we are in an idle time during the day.

    Parameters
    ----------
    start : :class:`int`, optional
        Start time in hours.
    end : :class:`int`, optional
        End time in hours.
    tz : :class:`str`, optional
        Time zone to use.

    Returns
    -------
    :class:`int`
        Number of seconds to wait until the end of the idle period.
        If outside the idle period, this number will be negative.
    """
    if tz is None:
        tz = MST
    else:
        tz = pytz.timezone(tz)
    i = dt.datetime.now(tz=tz)
    s = dt.datetime(i.year, i.month, i.day, start, 0, 0, tzinfo=tz)
    if i < s:
        return (i - s) // dt.timedelta(seconds=1)
    e = dt.datetime(i.year, i.month, i.day, end, 0, 0, tzinfo=tz)
    return (e - i) // dt.timedelta(seconds=1)


def exclude_years(start_year):
    """Generate rsync ``--exclude`` statements of the form ``--exclude 2020*``.

    Parameters
    ----------
    start_year : :class:`int`
        First year to exclude.

    Returns
    -------
    :class:`list`
        A list suitable for appending to a command.
    """
    return (' '.join([f'--exclude {y:d}*' for y in range(start_year, time.localtime().tm_year)])).split()
