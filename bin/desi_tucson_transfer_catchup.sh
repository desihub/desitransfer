#!/bin/bash
#
# Parallel copy DESI mirror data, to catch up after outages.
#
function usage() {
    local execName=$(basename $0)
    (
    echo "${execName} [-h] [-t] [-v]"
    echo ""
    echo "Parallel copy DESI mirror data, to catch up after outages."
    echo ""
    echo "     -h = Print this message and exit."
    echo "     -t = Test mode. Do not execute any commands. Implies -v."
    echo "     -v = Verbose mode. Print extra information."
    echo ""
    echo "The following environment variables are required:"
    echo ""
    echo "    DESISYNC_HOSTNAME = defines the rsync server information."
    echo "    DESI_ROOT = defines the local destination directory."
    echo ""
    echo "The following environment variable(s) are optional:"
    echo ""
    echo "    RSYNC_RSH = if detected, use rsync+ssh."
    echo ""
    ) >&2
}
#
# Do not expand globs, pass them on to rsync.
#
set -o noglob
#
# Configuration.
#
Test=/usr/bin/false
Verbose=/usr/bin/false
while getopts htv argname; do
    case ${argname} in
        h) usage; exit 0 ;;
        t) Test=/usr/bin/true; Verbose=/usr/bin/true ;;
        v) Verbose=/usr/bin/true ;;
        *) usage; exit 1 ;;
    esac
done
shift $((OPTIND - 1))
#
# Check for required environment variables.
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
# Set up rsync commands.
#
syn='/usr/bin/rsync --archive --verbose --delete --delete-after --no-motd'
src=${DESISYNC_HOSTNAME}:/global/cfs/cdirs/desi
if [[ -z "${RSYNC_RSH}" ]]; then
    syn="${syn} --password-file ${HOME}/.desi"
    src=rsync://${DESISYNC_HOSTNAME}/desi
fi
dst=${DESI_ROOT}
log_root=${HOME}/Documents/Logfiles
#
# Execute rsync commands.
# Typically the rsync service is set to scale to 8 instances with two
# connections allowed per instance.
#
for d in engineering/focalplane \
    spectro/data \
    spectro/redux/daily spectro/redux/daily/exposures spectro/redux/daily/preproc spectro/redux/daily/tiles \
    spectro/nightwatch/kpno spectro/staging/lost+found; do
    case ${d} in
        engineering/focalplane) priority='nice'; exclude='--exclude archive --exclude hwtables --exclude *.ipynb --exclude .ipynb_checkpoints' ;;
        spectro/data) priority=''; exclude='--exclude 2018* --exclude 2019* --exclude 2020* --exclude 2021* --exclude 2022* --exclude 2023* --exclude 2024*' ;;
        spectro/nightwatch/kpno) priority='nice'; exclude='--exclude 2021* --exclude 2022* --exclude 2023* --exclude 2024*' ;;
        spectro/redux/daily) priority=''; exclude='--exclude *.tmp --exclude attic --exclude dark_preproc --exclude exposures --exclude preproc --exclude temp --exclude tiles' ;;
        spectro/redux/daily/exposures) priority=''; exclude='--exclude 2019* --exclude 2020* --exclude 2021* --exclude 2022* --exclude 2023* --exclude 2024* --exclude *.tmp' ;;
        spectro/redux/daily/preproc) priority=''; exclude='--exclude 2019* --exclude 2020* --exclude 2021* --exclude 2022* --exclude 2023* --exclude 2024* --exclude *.tmp --exclude preproc-*.fits --exclude preproc-*.fits.gz' ;;
        spectro/redux/daily/tiles) priority=''; exclude='--exclude *.tmp --exclude temp' ;;
        *) priority='nice'; exclude='' ;;
    esac
    log=${log_root}/catchup_$(tr '/' '_' <<<${d}).log
    [[ -f ${log} ]] || touch ${log}
    ${Verbose} && echo "${priority} ${syn} ${exclude} ${src}/${d}/ ${dst}/${d}/ &>> ${log} &"
    if ${Test}; then
        #
        # Do nothing, successfully.
        :
    else
        #
        # Don't do this as a one-liner because of redirects, etc.
        #
        ${priority} ${syn} ${exclude} ${src}/${d}/ ${dst}/${d}/ &>> ${log} &
    fi
done
