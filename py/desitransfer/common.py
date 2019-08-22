# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
desitransfer.common
===================

Code needed by all scripts.
"""
import datetime as dt
import stat
from collections import namedtuple
import pytz

DTSDir = namedtuple('DTSDir', 'source, staging, destination, hpss')


dir_perm = (stat.S_ISGID |
            stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
            stat.S_IRGRP | stat.S_IXGRP)  # 0o2750
file_perm = stat.S_IRUSR | stat.S_IRGRP    # 0o0440


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
    c =  ['/bin/rsync', '--verbose', '--recursive',
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


def yesterday():
    """Yesterday's date in DESI "NIGHT" format, YYYYMMDD.
    """
    return (dt.datetime.now() - dt.timedelta(seconds=86400)).strftime('%Y%m%d')
