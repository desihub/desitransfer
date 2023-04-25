# Standalone init script to run any shell command
# Sean McManus 2015-11-17
# Adapted for DESI: Benjamin Weaver 2018-06-27
# Migrated to desitransfer: 2019-08-05
#
# DESI Environment: Moved this to crontab
# source /global/common/software/desi/desi_environment.sh ${DESIMODULES_VERSION}
#
# Common variables.
#
NICE='nice -n 19'
THISHOST=$(hostname -s)
#
# The existence of this file will shut down data transfers.
#
kill_switch=${HOME}/stop_desi_transfer
#
# Determine the version of pgrep to select options.
#
PGREP_VERSION=$(pgrep -V | sed -r 's/[^0-9]+([0-9]+)\.([0-9]+)\.([0-9]+)[^0-9]*/\1\2/')
PGREP_OPTIONS='-a -f'
[[ ${PGREP_VERSION} == "32" ]] && PGREP_OPTIONS='-l -f'
#
#
#
start() {
    if [[ -f ${kill_switch} ]]; then
        echo "${kill_switch} detected, will not attempt to start ${PRGFILE}."
        return 0
    fi
    local process=$(pgrep ${PGREP_OPTIONS} ${PRGFILE} 2> /dev/null)
    if [[ -n "$(grep ${DESITRANSFER} <<<${process})" ]]; then
        echo "${THISHOST} ${PRGFILE} is already started."
        return 1
    fi
    if [[ -n "$(grep $(dirname ${DESITRANSFER}) <<<${process})" ]]; then
        echo "${THISHOST} another version of ${PRGFILE} may already be running."
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
    local SIGNAL='SIGTERM'
    local PPLIST=$(pgrep -f ${PRGFILE} -d ' ')
    for PID in ${PPLIST}; do
        local CHILDPIDS=$(pgrep -P ${PID} -d ' ')
        echo killing ${PRGFILE} ${PID} child processes: ${CHILDPIDS}
        while true; do
            kill -s SIGTERM ${PID} ${CHILDPIDS} >& /dev/null
            if [[ $? != 0 ]]; then
                break
            fi
            sleep 2
            echo -n '.'
        done
    done
    #
    # Save the big hammer for last.
    #
    pkill -f -9 ${PRGFILE}
}
#
#
#
stop() {
    if [[ -z "$(pgrep ${PGREP_OPTIONS} ${PRGFILE} 2> /dev/null | grep ${DESITRANSFER})" ]]; then
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
    if [[ -n "$(pgrep ${PGREP_OPTIONS} ${PRGFILE} 2> /dev/null | grep ${DESITRANSFER})" ]]; then
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
