#!/bin/bash
#
# This script is used to update the NOAO mirror.
#
function usage() {
    local execName=$(basename $0)
    (
    echo "${execName} [-c] [-d DIR] [-e DIR[,DIR]] [-h] [-l DIR] [-s] [-S TIME] [-t] [-v]"
    echo ""
    echo "Sync DESI data to Tucson mirror site."
    echo ""
    echo "     -c = Pass -c, --checksum to rsync command."
    echo " -d DIR = Use DIR as destination directory."
    echo " -e DIR = Exclude DIR from sync (comma-separated)."
    echo "     -h = Print this message and exit."
    echo " -l DIR = Use DIR for log files."
    echo "     -s = Also sync static data sets."
    echo "-S TIME = Sleep for TIME while waiting for daily transfer to finish."
    echo "     -t = Test mode.  Do not make any changes. Implies -v."
    echo "     -v = Verbose mode. Print extra information."
    echo ""
    ) >&2
}
#
# Disable glob expansion.
#
set -o noglob
# set -o errexit
# set -o verbose
# set -o xtrace
#
# Static data sets don't need to be updated as frequently.
#
static=$(cat <<EOT
cmx
datachallenge
engineering/2021_summer_illumination_checks
engineering/donut
engineering/fvc
engineering/fvc_distortion
engineering/gfa
engineering/pfa2positioner
engineering/platemaker
engineering/spectrograph
engineering/svn_export_focalplane_12302018
engineering/umdata
protodesi
public/epo
spectro/desi_spectro_calib
spectro/redux/denali
spectro/redux/everest
spectro/templates/basis_templates
sv
target/cmx_files

EOT
)
#
# Dynamic data sets may change daily.
#
dynamic=$(cat <<EOT
engineering/focalplane
software/AnyConnect
spectro/data
spectro/nightwatch/kpno
spectro/redux/daily
spectro/redux/daily/exposures
spectro/redux/daily/preproc
spectro/redux/daily/tiles
spectro/staging/lost+found
target/catalogs
target/secondary
EOT
)
#
# Get options.
#
checksum=''
dst=''
exclude=NONE
log=${HOME}/Documents/Logfiles
sleepTime=15m
stampFormat='+%Y-%m-%dT%H:%M:%S%z'
test=/bin/false
verbose=/bin/false
while getopts cd:e:hsS:tv argname; do
    case ${argname} in
        c) checksum='--checksum' ;;
        d) dst=${OPTARG} ;;
        e) exclude=${OPTARG} ;;
        h) usage; exit 0 ;;
        l) log=${OPTARG} ;;
        s) dynamic="${dynamic} ${static}" ;;
        S) sleepTime=${OPTARG} ;;
        t) test=/bin/true; verbose=/bin/true ;;
        v) verbose=/bin/true ;;
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
if [[ -z "${DESISYNC_STATUS_URL}" ]]; then
    echo "DESISYNC_STATUS_URL must be set!" >&2
    exit 1
fi
if [[ -z "${CSCRATCH}" ]]; then
    echo "CSCRATCH must be set!" >&2
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
# Wait for daily KPNO -> NERSC transfer to finish.
#
l=${log}/desi_tucson_transfer.log
[[ -f ${l} ]] || /bin/touch ${l}
stamp=$(/bin/date ${stampFormat})
if ${test}; then
    ${verbose} && echo "DEBUG:${stamp}: /usr/bin/wget -q -O ${CSCRATCH}/daily.txt ${DESISYNC_STATUS_URL}" >> ${l}
    ${verbose} && echo "DEBUG:${stamp}: Skipping NERSC wait due to test mode." >> ${l}
else
    until /usr/bin/wget -q -O ${CSCRATCH}/daily.txt ${DESISYNC_STATUS_URL}; do
        stamp=$(/bin/date ${stampFormat})
        ${verbose} && echo "DEBUG:${stamp}: Daily transfer incomplete, sleeping ${sleepTime}." >> ${l}
        /bin/sleep ${sleepTime}
    done
fi
#
# Run rsync.
#
rsync="/usr/bin/rsync --archive ${checksum} --verbose --delete --delete-after --no-motd --password-file ${HOME}/.desi"
for d in ${dynamic}; do
    #
    # Check for subdirectories to include.
    #
    case ${d} in
        spectro/desi_spectro_calib) inc="--exclude .svn" ;;
        # spectro/nightwatch) inc="--include kpno/*** --exclude *" ;;
        spectro/redux/daily) inc="--exclude *.tmp --exclude preproc-*.fits --exclude attic --exclude exposures --exclude preproc --exclude temp --exclude tiles" ;;
        spectro/redux/daily/exposures) inc="--exclude *.tmp --exclude preproc-*.fits" ;;
        spectro/redux/daily/preproc) inc="--exclude *.tmp --exclude preproc-*.fits" ;;
        spectro/redux/daily/tiles) inc="--exclude *.tmp --exclude preproc-*.fits" ;;
        spectro/templates/basis_templates) inc="--exclude .svn --exclude basis_templates_svn-old" ;;
        *) inc='' ;;
    esac
    #
    # Log file.
    #
    l=${log}/desi_tucson_transfer_$(/usr/bin/tr '/' '_' <<<${d}).log
    [[ -f ${l} ]] || /bin/touch ${l}
    #
    # rsync command.
    #
    skipDir=''
    for e in $(/usr/bin/tr ',' ' ' <<<${exclude}); do
        [[ ${d} == ${e} ]] && skipDir=${e}
    done
    stamp=$(/bin/date ${stampFormat})
    if [[ -n "${skipDir}" ]]; then
        ${verbose} && echo "DEBUG:${stamp}: ${skipDir} skipped at user request." >> ${l}
    else
        ${verbose} && echo "DEBUG:${stamp}: ${rsync} ${inc} ${src}/${d}/ ${dst}/${d}/" >> ${l}
        ${test}    || ${rsync} ${inc} ${src}/${d}/ ${dst}/${d}/ >> ${l} 2>&1 || \
            echo "rsync error detected for ${dst}/${d}/!  Check logs!" >&2
    fi
done
