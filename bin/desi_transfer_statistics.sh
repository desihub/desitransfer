#!/bin/bash
#
# Collect (meta) data on files transferred for annual reporting purposes.
#
# Total amount of raw data.
#
year=$1
next_year=$(( year + 1 ))
# fy=${year}1[012][0-9][0-9] ${next_year}0[0-9][0-9][0-9]
number_of_nights=0
number_of_exposures=0
total_data=0
for n in ${DESI_SPECTRO_DATA}/${year}1[012][0-9][0-9] ${DESI_SPECTRO_DATA}/${next_year}0[0-9][0-9][0-9]; do
    # echo ${n}
    night=$(basename ${n})
    number_of_nights=$(( number_of_nights + 1 ))
    for e in ${DESI_SPECTRO_DATA}/${night}/*; do
        # echo ${e}
        expid=$(basename ${e})
        number_of_exposures=$(( number_of_exposures + 1 ))
        expid_size=$(du -k -s ${e} | awk '{print $1}')
        total_data=$(( total_data + expid_size ))
    done
done
echo "Number of nights = ${number_of_nights}."
echo "Number of exposures = ${number_of_exposures}."
echo "Total data = ${total_data} KB."
#
# Statistics during fire recovery period.
#
if [[ "${year}" == "2021" ]]; then
    number_of_fire_nights=0
    number_of_fire_exposures=0
    total_fire_data=0
    for n in ${DESI_SPECTRO_DATA}/${next_year}0[89][0-9][0-9]; do
        # echo ${n}
        night=$(basename ${n})
        number_of_fire_nights=$(( number_of_fire_nights + 1 ))
        for e in ${DESI_SPECTRO_DATA}/${night}/*; do
            # echo ${e}
            expid=$(basename ${e})
            expid_size=$(du -k -s ${e} | awk '{print $1}')
            number_of_fire_exposures=$(( number_of_fire_exposures + 1 ))
            total_fire_data=$(( total_fire_data + expid_size ))
        done
    done
    echo "Number of nights during fire recovery = ${number_of_fire_nights}."
    echo "Number of exposures during fire recovery = ${number_of_fire_exposures}."
    echo "Total data during fire recovery = ${total_fire_data} KB."
fi
#
# Nightwatch data.
#
NIGHTWATCH=${DESI_ROOT}/spectro/nightwatch/kpno
number_of_nightwatch_nights=0
total_nightwatch_data=0
for n in ${NIGHTWATCH}/${year}1[012][0-9][0-9] ${NIGHTWATCH}/${next_year}0[0-9][0-9][0-9]; do
    # echo ${n}
    # night=$(basename ${n})
    number_of_nightwatch_nights=$(( number_of_nightwatch_nights + 1 ))
    night_size=$(du -k -s ${n} | awk '{print $1}')
    total_nightwatch_data=$(( total_nightwatch_data + night_size ))
done
echo "Number of nightwatch nights = ${number_of_nightwatch_nights}."
# echo "Number of exposures = ${number_of_exposures}."
echo "Total nightwatch data = ${total_nightwatch_data} KB."
#
# Engineering data.
#
# After the clean-up of the donut data, the focalplane directory is the
# largest contributor. So even though as a whole the engineering data increased
# by about 100 GB, the focalplane directory increased by about 1 TB, while the
# donut directory shrank considerably.
#
# Reduced daily data transferred to Tucson.
#
# This is slightly tricky because we don't transfer *every* file.
# But in practice we should also count KPNO nightwatch data transferred to
# Tucson.
# - The majority of daily reduction data is in the exposures and tiles
#   directories.  This adds up to (102.9 - 46.6) + (51.7 - 20) = 88 TB.
# - KPNO nightwatch data (57.5 - 28.5) = 29 TB at transferred.
#
# Fire recovery:
#
# - We are transferring raw data on disk.
# - We are transferring a small amount of engineering files ~ 10 MB/night.
# - We are *not* transferring nightwatch data.
# - We are streaming to the database replica at NERSC.  This is the biggest share.
#