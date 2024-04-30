#!/bin/bash
#
# Parallel copy DESI mirror data to Utah.
#
#
# Help!
#
function usage() {
    local execName=$(basename $0)
    (
    echo "${execName} [-h] [-t] [-v]"
    echo ""
    echo "Parallel copy DESI mirror data to Utah."
    echo ""
    echo "     -h = Print this message and exit."
    echo "     -t = Test mode.  Do not make any changes. Implies -v."
    echo "     -v = Verbose mode. Print extra information."
    echo ""
    ) >&2
}
#
# Do not expand globs, pass them on to rsync.
#
set -o noglob
#
# Environment variables.
#
if [[ -z "${DESITRANSFER}" ]]; then
    echo "ERROR: DESITRANSFER is undefined!"
    exit 1
fi
if [[ -z "${DESISYNC_HOSTNAME}" ]]; then
    echo "ERROR: DESISYNC_HOSTNAME is undefined!"
    exit 1
fi
if [[ -z "${DESI_ROOT}" ]]; then
    echo "ERROR: DESI_ROOT is undefined!"
    exit 1
fi
if [[ -z "${DAILY_TILES_CUMULATIVE_OUTPUT}" ]]; then
    echo "ERROR: DAILY_TILES_CUMULATIVE_OUTPUT is undefined!"
    exit 1
fi
#
# Configuration.
#
syn="/usr/bin/rsync --archive --verbose --delete --delete-after --no-motd --password-file ${HOME}/.desi"
src=rsync://${DESISYNC_HOSTNAME}/desi
dst=${DESI_ROOT}
log_root=${HOME}/Documents/Logfiles
verbose=/usr/bin/false
test=/usr/bin/false
while getopts htv argname; do
    case ${argname} in
        h) usage; exit 0 ;;
        t) test=/usr/bin/true; verbose=/usr/bin/true ;;
        v) verbose=/usr/bin/true ;;
        *) usage; exit 1 ;;
    esac
done
shift $((OPTIND - 1))
#
# Set user-write on some files.
#
${verbose} && echo "chmod -R u+w ${dst}/spectro/redux/daily/tiles/cumulative"
${test}    || chmod -R u+w ${dst}/spectro/redux/daily/tiles/cumulative
#
# Copy the daily/tiles/cumulative description file from NERSC.
#
wget --quiet --unlink --output-document=${DESI_ROOT}/spectro/redux/daily_tiles_cumulative.txt ${DAILY_TILES_CUMULATIVE_OUTPUT}
#
# Execute rsync commands.
#
for d in spectro/redux/daily spectro/redux/daily/calibnight \
    spectro/redux/daily/exposure_tables spectro/redux/daily/exposures \
    spectro/redux/daily/preproc spectro/redux/daily/processing_tables \
    spectro/redux/daily/tiles/archive spectro/redux/daily/tiles/cumulative \
    survey/GFA; do
    case ${d} in
        spectro/redux/daily) priority='nice'; exclude="--include-from ${DESITRANSFER}/py/desitransfer/data/desi_utah_daily.txt --exclude *" ;;
        spectro/redux/daily/calibnight) priority='nice'; exclude='--include 202403?? --include *.fits --include *.fits.gz --include *.csv --include tmp --include old --exclude *' ;;
        spectro/redux/daily/exposures) priority='nice'; exclude='--include 202403?? --include 202403??/???????? --include *.fits --include *.fits.gz --include *.csv --exclude *' ;;
        spectro/redux/daily/preproc) priority='nice'; exclude='--include 202403?? --include 202403??/???????? --include fibermap-*.fits --include preproc-*.fits.gz --include tilepix-*.json --exclude *' ;;
        spectro/redux/daily/tiles/cumulative) priority='nice'; exclude="--files-from ${DESI_ROOT}/spectro/redux/daily_tiles_cumulative.txt" ;;
        *) priority=''; exclude='' ;;
    esac
    log=${log_root}/utah_$(tr '/' '_' <<<${d}).log
    [[ -f ${log} ]] || touch ${log}
    ${verbose} && echo "${priority} ${syn} ${exclude} ${src}/${d}/ ${dst}/${d}/ &>> ${log} &"
    ${test}    || ${priority} ${syn} ${exclude} ${src}/${d}/ ${dst}/${d}/ &>> ${log} &
done
