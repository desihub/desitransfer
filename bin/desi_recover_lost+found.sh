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
extension='.fits.fz'
data=${DESI_SPECTRO_DATA}
LF=${DESI_ROOT}/spectro/staging/lost+found
nights=''
fileType=desi
testMode=/bin/false
verboseMode=/bin/false
while getopts hr:tv argname; do
    case ${argname} in
        h) usage && exit 0 ;;
        n) nights=${OPTARG} ;;
        T) fileType=${OPTARG} ;;
        t) testMode=/bin/true ;;
        v) verboseMode=/bin/true ;;
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
    for e in ${night}/*; do
        exposure=$(/usr/bin/basename ${e})
        filename=${e}/${fileType}-${exposure}${extension}
        if [[ -f ${filename} ]]; then
            ${verboseMode} && "Detected ${filename}"
        fi
        checksum=${e}/checksum-${n}-${exposure}.sha256sum
        if [[ -f ${checksum} ]]; then
            ${verboseMode} && "Detected ${checksum}"
        else
            ${verboseMode} && "Creating ${checksum}"
        fi
    done
done