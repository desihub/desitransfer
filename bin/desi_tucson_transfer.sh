#!/bin/bash
#
# This script is used to update the NOAO mirror.
#
# $Id: desisync.sh 130540 2020-03-04 22:15:29Z baweaver $
function usage() {
    local execName=$(basename $0)
    (
    echo "${execName} [-d DIR] [-e DIR] [-h] [-s] [-t] [-v]"
    echo ""
    echo "Sync DESI data to Tucson mirror site."
    echo ""
    echo "-d DIR = Use DIR as destination directory."
    echo "-e DIR = Exclude DIR from sync."
    echo "    -h = Print this message and exit."
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
# Top-level source and destination.
#
src=rsync://desi@app.desisync.dev-cattle.stable.spin.nersc.org:60023/desi
dst=/net/mss1/desi-stage
#
# Static data sets don't need to be updated as frequently.
#
static='protodesi spectro/redux/oak1'
#
# Dynamic data sets may change daily.
#
dynamic='cmx datachallenge engineering spectro/data spectro/nightwatch/kpno spectro/redux/daily spectro/staging/lost+found sv target/catalogs target/cmx_files target/secondary'
#
# Get options.
#
exclude=NONE
test=false
verbose=false
while getopts d:e:hstv argname; do
    case ${argname} in
        d) dst=${OPTARG} ;;
        e) exclude=${OPTARG} ;;
        h) usage; exit 0 ;;
        s) dynamic="${dynamic} ${static}" ;;
        t) test=true; verbose=true ;;
        v) verbose=true ;;
        *) usage; exit 1 ;;
    esac
done
shift $((OPTIND - 1))
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
        ${verbose} && echo "${exclude} skipped at user request."
    else
        ${verbose} && echo ${rsync} ${inc} ${src}/${d}/ ${dst}/${d}/
        ${test}    || ${rsync} ${inc} ${src}/${d}/ ${dst}/${d}/
    fi
done
