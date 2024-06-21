#!/bin/bash
#
# Parallel copy DESI mirror data, to catch up after outages.
#
if [[ -z "${DESISYNC_HOSTNAME}" ]]; then
    echo "ERROR: DESISYNC_HOSTNAME is undefined!"
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
# Configuration.
#
syn="/usr/bin/rsync --archive --verbose --delete --delete-after --no-motd --password-file ${HOME}/.desi"
src=rsync://${DESISYNC_HOSTNAME}/desi
dst=${DESI_ROOT}
log_root=${HOME}/Documents/Logfiles
#
# Execute rsync commands.
#
for d in engineering/focalplane engineering/focalplane/hwtables \
    spectro/data \
    spectro/redux/daily spectro/redux/daily/exposures spectro/redux/daily/preproc spectro/redux/daily/tiles \
    spectro/nightwatch/kpno spectro/staging/lost+found; do
    case ${d} in
        engineering/focalplane) priority='nice'; exclude='--exclude archive --exclude hwtables --exclude *.ipynb --exclude .ipynb_checkpoints' ;;
        engineering/focalplane/hwtables) priority='nice'; exclude='--include *.csv --exclude *' ;;
        spectro/data) priority=''; exclude='--exclude 2018* --exclude 2019* --exclude 2020* --exclude 2021* --exclude 2022* --exclude 2023*' ;;
        spectro/nightwatch/kpno) priority='nice'; exclude='--exclude 2021* --exclude 2022* --exclude 2023*' ;;
        spectro/redux/daily) priority=''; exclude='--exclude *.tmp --exclude attic --exclude exposures --exclude preproc --exclude temp --exclude tiles' ;;
        spectro/redux/daily/exposures) priority=''; exclude='--exclude *.tmp' ;;
        spectro/redux/daily/preproc) priority=''; exclude='--exclude *.tmp --exclude preproc-*.fits --exclude preproc-*.fits.gz' ;;
        spectro/redux/daily/tiles) priority=''; exclude='--exclude *.tmp --exclude temp' ;;
        *) priority='nice'; exclude='' ;;
    esac
    log=${log_root}/catchup_$(tr '/' '_' <<<${d}).log
    [[ -f ${log} ]] || touch ${log}
    echo "${priority} ${syn} ${exclude} ${src}/${d}/ ${dst}/${d}/ &>> ${log} &"
    ${priority} ${syn} ${exclude} ${src}/${d}/ ${dst}/${d}/ &>> ${log} &
done
