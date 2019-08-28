#!/bin/bash
# Standalone init script to run any shell command
# Sean McManus 2015-11-17
# Adapted for DESI: Benjamin Weaver 2018-06-27
# Migrated to desitransfer: 2019-08-05
#
# DESI Environment: Moved this to crontab
# source /global/common/software/desi/desi_environment.sh ${DESIMODULES_VERSION}
# Program or script you want to run
PROGRAM=${DESITRANSFER}/bin/desi_transfer_daemon
# Command line options for PRGFILE
PRGOPTS="--no-pipeline"
NICE="nice -n 19"
# LOCKDIR=${CSCRATCH}/run
#
# rm -rf ${LOCKDIR}; mkdir -p ${LOCKDIR}
#
PRGFILE=$(basename ${PROGRAM})
# PIDFILE=${LOCKDIR}/${PRGFILE}.pid
#
THISHOST=$(hostname -s)
#
# The existence of this file will shut down data transfers.
kill_switch=${HOME}/stop_desi_transfer
#
#
#
start() {
    if [[ -f ${kill_switch} ]]; then
        echo "${kill_switch} detected, will not attempt to start ${PRGFILE}."
        return 0
    fi
    if [[ -n "$(pgrep --full ${PRGFILE} 2> /dev/null)" ]]; then
        echo "${THISHOST} ${PRGFILE} is already started."
        return 1
    fi
    #
    # Daemonize: You must disconnect stdin, stdout, and stderr, and make it ignore the hangup signal (SIGHUP).
    #
    nohup ${NICE} ${PROGRAM} ${PRGOPTS}  &>/dev/null &
    #
    # Alternatively, use double background
    #
    # (/bin/bash -c "echo $$ >${PIDFILE} && exec ${NICE} ${PROGRAM} ${PRGOPTS}" &) &
    if [[ $? == 0 ]]; then
        echo "${PRGFILE} started."
        return 0
    else
        echo "Failed to start ${PRGFILE}."
        return 1
    fi
}
#
#
#
kill_it() {
    local PRGFILE=$(basename $1)
    local SIGNAL="SIGTERM"
    local PPLIST=$(pgrep --full ${PRGFILE} --delimiter ' ')
    for PID in ${PPLIST}; do
        local CHILDPIDS=$(pgrep --parent ${PID} --delimiter ' ')
        echo killing ${PRGFILE} ${PID} child processes: ${CHILDPIDS}
        while true; do
            kill -s SIGTERM ${PID} ${CHILDPIDS} >& /dev/null
            if [[ $? != 0 ]]; then
                break
            fi
            sleep 2
            echo -n "."
        done
    done
    #
    # Save the big hammer for last.
    #
    pkill --full --signal 9 ${PRGFILE}
}
#
#
#
stop() {
    if [[ -z $(pgrep --full ${PRGFILE} 2> /dev/null) ]]; then
        echo "${THISHOST} ${PRGFILE} is not running."
        return 1
    fi
    echo -n "Stopping ${PRGFILE}..."
    kill_it ${PRGFILE} #>& /dev/null
    # if [[ $? != 0 ]]; then
    #     echo "Operation not permitted."
    #     return 1
    # fi
    echo -e "\n${PRGFILE} stopped."
    # rm -f ${PIDFILE} 2> /dev/null
    return 0
}
#
#
#
status() {
    if [[ -n $(pgrep --full ${PRGFILE} 2> /dev/null) ]]; then
        echo "${THISHOST} ${PRGFILE} is running."
        return 0
    else
        echo "${THISHOST} ${PRGFILE} is not running"
        return 0
    fi
}
#
#
#
restart() {
    stop
    sleep 1
    start
    return $?
}
#
#
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
