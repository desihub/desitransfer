#!/bin/bash
#
# Help!
#
function usage() {
    local execName=$(basename $0)
    (
    echo "${execName} [-d DIR] [-h] [-I] [-t] [-v]"
    echo ""
    echo "Unpack DESI data tarballs."
    echo ""
    echo " -d DIR = Tarballs are on DIR, e.g. \${SCRATCH}"
    echo "     -h = Print this message and exit."
    echo "     -I = Ignore checksum errors on tarballs."
    echo "     -t = Test mode.  Do not make any changes. Implies -v."
    echo "     -v = Verbose mode. Print extra information."
    echo ""
    ) >&2
}
#
# Make sure we are running on datatran.
#
if [[ "${NERSC_HOST}" != "datatran" ]]; then
    echo "ERROR: This script should be executed on a DTN node!" >&2
    exit 1
fi
#
# Get options.
#
src_dir=${PWD}
dst_dir=/global/cfs/cdirs/desi/spectro/staging/raw
test=false
verbose=false
ignore_errors=false
while getopts d:hItv argname; do
    case ${argname} in
        d) src_dir=${OPTARG} ;;
        h) usage; exit 0 ;;
        I) ignore_errors=true ;;
        t) test=true; verbose=true ;;
        v) verbose=true ;;
        *) usage; exit 1 ;;
    esac
done
shift $((OPTIND - 1))
#
# Set up environment.
#
[[ -z "${DESIUTIL}" ]] && source /global/common/software/desi/desi_environment.sh datatran-test
[[ -z "${DESITRANSFER}" ]] && module load desitransfer/main
#
# Find tarballs.
#
declare -a nights
for kk in ${src_dir}/*.sha256sum; do
    k=$(basename ${kk})
    d=$(dirname ${kk})
    if [[ "${k}" == "*.sha256sum" ]]; then
        echo "ERROR: No tarballs found!" >&2
        exit 1
    fi
    #
    # Verify tarball.
    #
    night=$(cut -d. -f1 <<<${k})
    ${verbose} && echo "(cd ${d} && sha256sum --quiet --check ${k})"
    ${test}    || (cd ${d} && sha256sum --quiet --check ${k})
    if [[ $? != 0 ]]; then
        if ${ignore_errors}; then
            echo "WARNING: Checksum failure when verifying ${night}.tar, continuing anyway." >&2
        else
            echo "ERROR: Checksum failure when verifying ${night}.tar!" >&2
            exit 1
        fi
    fi
    #
    # Unpack tarball.
    #
    ${verbose} && echo tar -xf ${d}/${night}.tar -C ${dst_dir}
    ${test}    || tar -xf ${d}/${night}.tar -C ${dst_dir}
    #
    # Set permissions.
    #
    ${verbose} && echo "find ${night} -type f -exec chmod 0440 {} ;"
    ${test}    || find ${night} -type f -exec chmod 0440 \{\} \;
    ${verbose} && echo "find ${night} -type d -exec chmod 2550 {} ;"
    ${test}    || find ${night} -type d -exec chmod 2550 \{\} \;
    ${verbose} && echo chmod u+w ${night}
    ${test}    || chmod u+w ${night}
    #
    # Move into place.
    #
    ${verbose} && echo mv ${night} ../../data
    ${test}    || mv ${night} ../../data
    ${verbose} && echo chmod u-w ../../data/${night}
    ${test}    || chmod u-w ../../data/${night}
    #
    # Clean up.
    #
    ${verbose} && echo rm ${d}/${night}.tar ${d}/${night}.sha256sum
    ${test}    || rm ${d}/${night}.tar ${d}/${night}.sha256sum
    ${verbose} && echo "nights+=( ${night} )"
    nights+=( ${night} )
done
#
# Update transfer status after all nights are in place.
#
for night in "${nights[@]}"; do
    for e in ../../data/${night}/*; do
        expid=$(basename ${e})
        ${verbose} && echo desi_transfer_status ${night} ${expid} rsync
        ${test}    || desi_transfer_status ${night} ${expid} rsync
        ${verbose} && echo "(cd ${e} && sha256sum --quiet --check checksum-${expid}.sha256sum)"
        ${test}    || (cd ${e} && sha256sum --quiet --check checksum-${expid}.sha256sum)
        if [[ $? == 0 ]]; then
            ${verbose} && echo desi_transfer_status ${night} ${expid} checksum
            ${test}    || desi_transfer_status ${night} ${expid} checksum
        else
            ${verbose} && echo desi_transfer_status --failure ${night} ${expid} checksum
            ${test}    || desi_transfer_status --failure ${night} ${expid} checksum
        fi
    done
    ${verbose} && echo desi_transfer_status ${night} all backup
    ${test}    || desi_transfer_status ${night} all backup
done
