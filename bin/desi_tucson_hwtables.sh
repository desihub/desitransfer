#!/bin/bash
#
# Copy hwtables data over ssh.
#
# This script assumes that ssh access has already been set up.
#
if [[ -z "${NERSC_USER}" ]]; then
    echo "ERROR: NERSC_USER is undefined!"
    exit 1
fi
if [[ -z "${NERSC_HOST}" ]]; then
    echo "ERROR: NERSC_USER is undefined!"
    exit 1
fi
if [[ -z "${DESI_ROOT}" ]]; then
    echo "ERROR: DESI_ROOT is undefined!"
    exit 1
fi
#
# Do not expand globs, pass them on to rsync.
#
set -o noglob
#
# Configuration
#
src=${NERSC_USER}@${NERSC_HOST}:/global/cfs/cdirs/desi/engineering/focalplane/hwtables
dst=${DESI_ROOT}/engineering/focalplane/hwtables
/usr/bin/rsync --verbose --archive --delete --delete-after --include *.csv --exclude * \
    --rsh='ssh -i ~/.ssh/nersc -o IdentitiesOnly=yes' ${src}/ ${dst}/
