#!/bin/bash
#
# Program or script you want to run
#
PROGRAM=${DESITRANSFER}/bin/desi_transfer_daemon
PRGFILE=$(basename ${PROGRAM})
PRGDIR=$(dirname ${PROGRAM})
#
# Command line options for PRGFILE
#
if [[ -z "${NERSC_HOST}" ]]; then
    PRGOPTS='--no-backup'
else
    PRGOPTS=''
fi
#
# Common initialization code.
#
source ${PRGDIR}/desi_common_init.sh
#
# Main program.
#
case "$1" in
    start | stop | status | restart)
        $1
        ;;
    *)
        echo "Usage: $0 {start|stop|status|restart}"
        exit 2
        ;;
esac
exit $?
