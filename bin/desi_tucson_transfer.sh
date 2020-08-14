#!/bin/bash
#
# This script is used to update the NOAO mirror.
#
function usage() {
    local execName=$(basename $0)
    (
    echo "${execName} [-d DIR] [-e DIR] [-h] [-l DIR] [-s] [-t] [-v]"
    echo ""
    echo "Sync DESI data to Tucson mirror site."
    echo ""
    echo "-d DIR = Use DIR as destination directory."
    echo "-e DIR = Exclude DIR from sync."
    echo "    -h = Print this message and exit."
    echo "-l DIR = Use DIR for log files."
    echo "    -s = Also sync static data sets."
    echo "    -t = Test mode.  Do not make any changes. Implies -v."
    echo "    -v = Verbose mode. Print extra information."
    echo ""
    ) >&2
}
#
# Disable glob expansion, bail out if anything goes wrong.
#
set -o noglob
# set -o errexit
# set -o verbose
# set -o xtrace
#
# Static data sets don't need to be updated as frequently.
#
static='protodesi spectro/redux/andes spectro/redux/minisv2 spectro/redux/oak1'
#
# Dynamic data sets may change daily.
#
dynamic='cmx datachallenge engineering spectro/data spectro/nightwatch/kpno spectro/redux/daily spectro/staging/lost+found sv target/catalogs target/cmx_files target/secondary'
#
# Get options.
#
dst=''
exclude=NONE
log=${HOME}/Documents/Logfiles
test=false
verbose=false
while getopts d:e:hstv argname; do
    case ${argname} in
        d) dst=${OPTARG} ;;
        e) exclude=${OPTARG} ;;
        h) usage; exit 0 ;;
        l) log=${OPTARG} ;;
        s) dynamic="${dynamic} ${static}" ;;
        t) test=true; verbose=true ;;
        v) verbose=true ;;
        *) usage; exit 1 ;;
    esac
done
shift $((OPTIND - 1))
#
# Top level source.
#
if [[ -z "${DESISYNC_HOSTNAME}" ]]; then
    echo "DESISYNC_HOSTNAME must be set!" >&2
    exit 1
fi
src=rsync://${DESISYNC_HOSTNAME}/desi
#
# Check if top-level destination is set.
#
if [[ -z "${dst}" ]]; then
    if [[ -z "${DESI_ROOT}" ]]; then
        echo "DESI_ROOT must be set, or destination directory set on the command-line (-d DIR)!" >&2
        exit 1
    else
        dst=${DESI_ROOT}
    fi
fi
#
# Pid file.
#
p=${log}/desi_tucson_transfer.pid
if [[ -f ${p} ]]; then
    pid=$(<${p})
    comm=$(/bin/ps -q ${pid} -o comm=)
    if [[ -n "${comm}" ]]; then
        echo "Running process detected (${pid}=${comm}), exiting." >&2
        exit 1
    else
        /bin/rm -f ${p}
    fi
fi
echo $$ > ${p}
#
# Log file.
#
l=${log}/desi_tucson_transfer.log
[[ -f ${l} ]] || /bin/touch ${l}
#
# Run rsync.
#
rsync="/usr/bin/rsync --archive --verbose --delete --delete-after --no-motd --password-file ${HOME}/.desi"
for d in ${dynamic}; do
    case ${d} in
    #     spectro/nightwatch) inc="--include kpno/*** --exclude *" ;;
    #     spectro/redux) inc="--include oak1/*** --include daily/*** --exclude *" ;;
        *) inc='' ;;
    esac
    if [[ ${d} == ${exclude} ]]; then
        ${verbose} && echo "${exclude} skipped at user request." >> ${l}
    else
        ${verbose} && echo ${rsync} ${inc} ${src}/${d}/ ${dst}/${d}/ >> ${l}
        ${test}    || ${rsync} ${inc} ${src}/${d}/ ${dst}/${d}/ >> ${l} 2>&1
    fi
done
