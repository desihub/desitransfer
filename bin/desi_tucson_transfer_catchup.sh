#!/bin/bash
#
# Parallel copy DESI mirror data, to catch up after outages.
#
if [[ -z "${DESISYNC_HOSTNAME}" ]]; then
    echo "ERROR: DESISYNC_HOSTNAME is undefined!"
    exit 1
fi
if [[ -z "${DESI_ROOT}" ]]; then
    echo "ERROR: DESI_ROOT is undefined!"
    exit 1
fi
syn="/usr/bin/rsync --archive --verbose --delete --delete-after --no-motd --password-file ${HOME}/.desi"
src=rsync://${DESISYNC_HOSTNAME}/desi
dst=${DESI_ROOT}
log_root=${HOME}/Documents/Logfiles

for d in engineering/focalplane engineering/focalplane/hwtables \
    spectro/data \
    spectro/redux/daily spectro/redux/daily/exposures spectro/redux/daily/preproc spectro/redux/daily/tiles \
    spectro/nightwatch/kpno spectro/staging/lost+found; do
    case ${d} in
        engineering/focalplane) priority='nice'; exclude='--exclude archive --exclude hwtables --exclude *.ipynb --exclude .ipynb_checkpoints' ;;
        engineering/focalplane/hwtables) priority='nice'; exclude='--include *.csv --exclude *' ;;
        spectro/data) priority=''; exclude='--exclude 2018* --exclude 2019* --exclude 2020* --exclude 2021* --exclude 2022* --exclude 2023*' ;;
        spectro/redux/daily) priority=''; exclude='--exclude *.tmp --exclude attic --exclude exposures --exclude preproc --exclude temp --exclude tiles' ;;
        spectro/redux/daily/exposures) priority=''; exclude='--exclude *.tmp' ;;
        spectro/redux/daily/preproc) priority=''; exclude='--exclude *.tmp --exclude preproc-*.fits --exclude preproc-*.fits.gz' ;;
        spectro/redux/daily/tiles) priority=''; exclude='--exclude *.tmp --exclude temp' ;;
        *) priority='nice'; exclude='' ;;
    esac
    log=${log_root}/catchup_$(tr '/' '_' <<<${d}).log
    [[ -f ${log} ]] || touch ${log}
    echo "${priority} ${syn} ${exclude} ${src}/${d}/ ${dst}/${d}/ &>> ${log} &"
    ${priority} ${syn} ${exclude} ${src}/${d}/ ${dst}/${d}/ &>> ${log} &
done

# log=${log_root}/catchup_engineering_focalplane.log
# [[ -f ${log} ]] || touch ${log}
# nice ${syn} --exclude archive --exclude hwtables --exclude \*.ipynb --exclude .ipynb_checkpoints \
#     ${src}/engineering/focalplane/ ${dst}/engineering/focalplane/ &>> ${log} &

# log=${log_root}/catchup_engineering_focalplane_hwtables.log
# [[ -f ${log} ]] || touch ${log}
# nice ${syn} --include \*.csv --exclude \* \
#     ${src}/engineering/focalplane/hwtables/ ${dst}/engineering/focalplane/hwtables/ &>> ${log} &

# log=${log_root}/catchup_spectro_data.log
# [[ -f ${log} ]] || touch ${log}
# ${syn} --exclude 2018\* --exclude 2019\* --exclude 2020\* --exclude 2021\* --exclude 2022\* \
#     ${src}/spectro/data/ ${dst}/spectro/data/ &>> ${log} &

# log=${log_root}/catchup_spectro_redux_daily.log
# [[ -f ${log} ]] || touch ${log}
# ${syn} --exclude \*.tmp --exclude attic --exclude exposures --exclude preproc --exclude temp --exclude tiles \
#     ${src}/spectro/redux/daily/ ${dst}/spectro/redux/daily/ &>> ${log} &

# log=${log_root}/catchup_spectro_redux_daily_exposures.log
# [[ -f ${log} ]] || touch ${log}
# ${syn} --exclude \*.tmp \
#     ${src}/spectro/redux/daily/exposures/ ${dst}/spectro/redux/daily/exposures/ &>> ${log} &

# log=${log_root}/catchup_spectro_redux_daily_preproc.log
# [[ -f ${log} ]] || touch ${log}
# ${syn} --exclude \*.tmp --exclude preproc-\*.fits --exclude preproc-\*.fits.gz \
#     ${src}/spectro/redux/daily/preproc/ ${dst}/spectro/redux/daily/preproc/ &>> ${log} &

# log=${log_root}/catchup_spectro_redux_daily_tiles.log
# [[ -f ${log} ]] || touch ${log}
# ${syn} --exclude \*.tmp --exclude temp \
#     ${src}/spectro/redux/daily/tiles/ ${dst}/spectro/redux/daily/tiles/ &>> ${log} &

# log=${log_root}/catchup_spectro_nightwatch_kpno.log
# [[ -f ${log} ]] || touch ${log}
# nice ${syn} \
#     ${src}/spectro/nightwatch/kpno/ ${dst}/spectro/nightwatch/kpno/ &>> ${log} &

# log=${log_root}/catchup_spectro_staging_lost+found.log
# [[ -f ${log} ]] || touch ${log}
# nice ${syn} \
#     ${src}/spectro/staging/lost+found/ ${dst}/spectro/staging/lost+found/ &>> ${log} &
