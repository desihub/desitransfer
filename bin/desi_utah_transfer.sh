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
    echo "${execName} [-A] [-h] [-R] [-T] [-t] [-v]"
    echo ""
    echo "Parallel copy DESI mirror data to Utah."
    echo ""
    echo "     -A = Do NOT start a sync of daily/tiles/archive."
    echo "     -h = Print this message and exit."
    echo "     -R = Do NOT check for running jobs before starting new ones."
    echo "     -T = Do NOT execute commands via the `time` command."
    echo "     -t = Test mode.  Do not make any changes. Implies -v."
    echo "     -v = Verbose mode. Print extra information."
    echo ""
    echo "The following environment variables are required:"
    echo ""
    echo "    DESITRANSFER = location of configuration data."
    echo "    DESISYNC_HOSTNAME = defines the rsync server information."
    echo "    DESI_ROOT = defines the local destination directory."
    echo "    DAILY_TILES_CUMULATIVE_OUTPUT = location of tiles data to transfer."
    echo ""
    echo "The following environment variable(s) are optional:"
    echo ""
    echo "    RSYNC_RSH = if detected, use rsync+ssh."
    echo ""
    ) >&2
}
#
# Create a local include file with a running set of dates.
#
function local_include_file() {
    local path=$1
    local now=$(date +%s)
    local include_file=${DESI_ROOT}/spectro/redux/daily_${path}.txt
    truncate -s 0 ${include_file}
    for _day in $(seq 30); do
        _past=$(( now - (_day * 86400) ))
        _night=$(date -d @${_past} +%Y%m%d)
        echo "${_night}" >> ${include_file}
        if [[ "${path}" != "calibnight" ]]; then
            echo "${_night}/????????" >> ${include_file}
        fi
    done
    if [[ "${path}" == "preproc" ]]; then
        echo "fibermap-*.fits" >> ${include_file}
        echo "preproc-*.fits.gz" >> ${include_file}
        echo "tilepix-*.json" >> ${include_file}
    else
        echo "*.fits" >> ${include_file}
        echo "*.fits.gz" >> ${include_file}
        echo "*.csv" >> ${include_file}
        if [[ "${path}" == "calibnight" ]]; then
            echo "tmp" >> ${include_file}
            echo "old" >> ${include_file}
        fi
    fi
}
#
# Do not expand globs, pass them on to rsync.
#
set -o noglob
#
# Configuration.
#
Archive=/usr/bin/true
Check=/usr/bin/true
Time='time'
Test=/usr/bin/false
Verbose=/usr/bin/false
while getopts AhRtv argname; do
    case ${argname} in
        A) Archive=/usr/bin/false ;;
        h) usage; exit 0 ;;
        R) Check=/usr/bin/false ;;
        T) Time='' ;;
        t) Test=/usr/bin/true; Verbose=/usr/bin/true ;;
        v) Verbose=/usr/bin/true ;;
        *) usage; exit 1 ;;
    esac
done
shift $((OPTIND - 1))
#
# Check for required environment variables.
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
# Check for running rsync process.
#
if ${Check}; then
    n_rsync=$(/usr/bin/ps -U ${USER} -u ${USER} -o args= 2>/dev/null | /usr/bin/grep /usr/bin/rsync | /usr/bin/grep -v grep | /usr/bin/wc -l)
    if (( n_rsync > 0 )); then
        echo "ERROR: Some running rsync processes detected, exiting!"
        exit 1
    fi
fi
#
# Set up rsync commands.
#
syn="/usr/bin/rsync --archive --verbose --delete --delete-after --no-motd"
src=${DESISYNC_HOSTNAME}:/global/cfs/cdirs/desi
if [[ -z "${RSYNC_RSH}" ]]; then
    syn="${syn} --password-file ${HOME}/.desi"
    src=rsync://${DESISYNC_HOSTNAME}/desi
fi
dst=${DESI_ROOT}
log_root=${HOME}/Documents/Logfiles
#
# Set user-write on some files.
#
${Verbose} && echo "chmod -R u+w ${dst}/spectro/redux/daily/tiles/cumulative"
${Test}    || chmod -R u+w ${dst}/spectro/redux/daily/tiles/cumulative
#
# Copy the daily/tiles/cumulative description file from NERSC.
#
${Verbose} && echo "wget --quiet --unlink --output-document=${DESI_ROOT}/spectro/redux/daily_tiles_cumulative.txt ${DAILY_TILES_CUMULATIVE_OUTPUT}"
${Test}    || wget --quiet --unlink --output-document=${DESI_ROOT}/spectro/redux/daily_tiles_cumulative.txt ${DAILY_TILES_CUMULATIVE_OUTPUT}
#
# Prepare local include files.
#
for path in calibnight exposures preproc; do
    ${Verbose} && echo "local_include_file ${path}"
    local_include_file ${path}
done
#
# Execute rsync commands.
#
directories=(spectro/redux/daily \
             spectro/redux/daily/calibnight \
             spectro/redux/daily/exposure_tables \
             spectro/redux/daily/exposures \
             spectro/redux/daily/preproc \
             spectro/redux/daily/processing_tables \
             spectro/redux/daily/tiles/cumulative \
             survey/GFA)
if ${Archive}; then
    directories+=(spectro/redux/daily/tiles/archive)
fi
for d in ${directories[*]}; do
    case ${d} in
        spectro/redux/daily) priority='nice'; exclude="--include-from ${DESITRANSFER}/py/desitransfer/data/desi_utah_daily.txt --exclude *" ;;
        spectro/redux/daily/calibnight) priority='nice'; exclude="--delete-excluded --include-from ${DESI_ROOT}/spectro/redux/daily_calibnight.txt --exclude *" ;;
        spectro/redux/daily/exposures) priority='nice'; exclude="--delete-excluded --include-from ${DESI_ROOT}/spectro/redux/daily_exposures.txt --exclude *" ;;
        spectro/redux/daily/preproc) priority='nice'; exclude="--delete-excluded --include-from ${DESI_ROOT}/spectro/redux/daily_preproc.txt --exclude *" ;;
        spectro/redux/daily/tiles/cumulative) priority='nice'; exclude="--delete-excluded --files-from ${DESI_ROOT}/spectro/redux/daily_tiles_cumulative.txt" ;;
        *) priority=''; exclude='' ;;
    esac
    log=${log_root}/utah_$(tr '/' '_' <<<${d}).log
    [[ -f ${log} ]] || touch ${log}
    ${Verbose} && echo "${priority} ${Time} ${syn} ${exclude} ${src}/${d}/ ${dst}/${d}/ &>> ${log} &"
    ${Test}    || ${priority} ${Time} ${syn} ${exclude} ${src}/${d}/ ${dst}/${d}/ &>> ${log} &
done

