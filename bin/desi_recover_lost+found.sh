#!/bin/bash
# Licensed under a 3-clause BSD style license - see LICENSE.rst.
# -*- coding: utf-8 -*-
#
# Recover exposures containing a specific file type from lost+found.
#
function usage() {
    local execName=$(basename $0)
    (
    echo "usage ${execName} [-e EXT] [-h] [-n YYYYMMDD[,...]] [-T FILETYPE] [-t] [-v]"
    echo "    -e EXT       - Files have this extension (default .fits.fz)."
    echo "    -h           - Print usage information and exit."
    echo "    -n YYYYMMDD  - Restrict search to one or more (comma-separated) nights."
    echo "    -T FILETYPE  - Look for files of this type (default desi)."
    echo "    -t           - Test mode.  Do not do anything."
    echo "    -v           - Verbose mode. Print every little detail."
    ) >&2
}
#
# Get options
#
TRUE=/usr/bin/true
FALSE=/usr/bin/false
extension='.fits.fz'
data=${DESI_SPECTRO_DATA}
LF=${DESI_ROOT}/spectro/staging/lost+found
nights=''
fileType=desi
testMode=${FALSE}
verboseMode=${TRUE}
while getopts e:hn:T:tv argname; do
    case ${argname} in
        e) extension=${OPTARG} ;;
        h) usage && exit 0 ;;
        n) nights=${OPTARG} ;;
        T) fileType=${OPTARG} ;;
        t) testMode=${TRUE} ;;
        v) verboseMode=${TRUE} ;;
        *)
            echo "Unknown option!" >&2
            usage
            exit 1
            ;;
    esac
done
shift $((OPTIND - 1))
#
# Find nights.
#
if [[ -z "${nights}" ]]; then
    nights=$(/usr/bin/ls ${LF} | /usr/bin/grep -v README)
else
    nights=$(/usr/bin/tr ',' ' ' <<<${nights})
fi
${verboseMode} && echo "Searching: ${nights}"
#
# Find exposures.
#
for n in ${nights}; do
    night=${LF}/${n}
    exposuresFound=${FALSE}
    for e in ${night}/*; do
        exposure=$(/usr/bin/basename ${e})
        filename=${e}/${fileType}-${exposure}${extension}
        if [[ -f ${filename} ]]; then
            ${verboseMode} && echo "Detected ${filename}"
            exposuresFound=${TRUE}
            checksum=${e}/checksum-${n}-${exposure}.sha256sum
            if [[ -f ${checksum} ]]; then
                ${verboseMode} && echo "Detected ${checksum}"
            else
                # ${verboseMode} && echo "Creating ${checksum}"
                cc=$(/usr/bin/basename ${checksum})
                ${verboseMode} && echo "(cd ${e} && /usr/bin/sha256sum * > ${HOME}/${cc} && /bin/mv ${HOME}/${cc} . && /usr/bin/chmod 0440 ${cc})"
                ${testMode}    || (cd ${e} && /usr/bin/sha256sum * > ${HOME}/${cc} && /bin/mv ${HOME}/${cc} . && /usr/bin/chmod 0440 ${cc})
            fi
            if [[ ! -d ${data}/${n} ]]; then
                ${verboseMode} && echo /usr/bin/mkdir ${data}/${n}
                ${testMode}    || /usr/bin/mkdir ${data}/${n}
                ${verboseMode} && echo /usr/bin/chmod 2750 ${data}/${n}
                ${testMode}    || /usr/bin/chmod 2750 ${data}/${n}
            fi
            ${verboseMode} && echo /usr/bin/mv ${e} ${data}/${n}
            ${testMode}    || /usr/bin/mv ${e} ${data}/${n}
        fi
    done
    if ${exposuresFound}; then
        ${verboseMode} && echo "(cd ${data} && htar -cvf desi/spectro/data/desi_spectro_data_${n}.tar -H crc:verify=all ${n})"
        ${testMode}    || (cd ${data} && htar -cvf desi/spectro/data/desi_spectro_data_${n}.tar -H crc:verify=all ${n})
    fi
done
