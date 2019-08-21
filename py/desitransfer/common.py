# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
desitransfer.common
===================

Code needed by all scripts.
"""
import stat
from collections import namedtuple


DTSDir = namedtuple('DTSDir', 'source, staging, destination, hpss')


dir_perm = (stat.S_ISGID |
            stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
            stat.S_IRGRP | stat.S_IXGRP)  # 0o2750
file_perm = stat.S_IRUSR | stat.S_IRGRP    # 0o0440
