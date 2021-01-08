#!/bin/bash
#
# Check Raw data checksum files.
#
[[ -z "${DESI_SPECTRO_DATA}" ]] && echo "DESI_SPECTRO_DATA must be set!" && exit 1
data=${DESI_SPECTRO_DATA}
for c in $(/usr/bin/find ${data} -name \*.sha256sum); do
    d=$(/usr/bin/dirname ${c})
    b=$(/usr/bin/basename ${c})
    echo "DEBUG: cd ${d}"
    cd ${d}
    n=$(/usr/bin/wc -l ${b} | /usr/bin/cut -d' ' -f1)
    l=$(/usr/bin/find . -type f | /usr/bin/wc -l)
    if ((n == l - 1)); then
        echo "DEBUG: correct number of files listed in ${c}."
    elif ((n > l - 1)); then
        echo "ERROR: missing files listed in ${c}!"
    else
        echo "ERROR: extra files listed in ${c}!"
    fi
    echo "DEBUG: /usr/bin/sha256sum --check --status ${b}"
    /usr/bin/sha256sum --check --status ${b}
    if [[ $? != 0 ]]; then
        echo "ERROR: checksum error(s) reported for ${b}!"
        /usr/bin/sha256sum --check ${b}
    fi
done
