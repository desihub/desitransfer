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
    echo ${n}
    night=$(basename ${n})
    number_of_nights=$(( number_of_nights + 1 ))
    for e in ${DESI_SPECTRO_DATA}/${night}/*; do
        echo ${e}
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
        echo ${n}
        night=$(basename ${n})
        number_of_fire_nights=$(( number_of_fire_nights + 1 ))
        for e in ${DESI_SPECTRO_DATA}/${night}/*; do
            echo ${e}
            expid=$(basename ${e})
            expid_size=$(du -k -s ${e} | awk '{print $1}')
            number_of_fire_exposures=$(( number_of_fore_exposures + 1 ))
            total_data=$(( total_fire_data + expid_size ))
    done
    done
    echo "Number of nights during fire recovery = ${number_of_fire_nights}."
    echo "Number of exposures during fire recovery = ${number_of_fire_exposures}."
    echo "Total data during fire recovery = ${total_fire_data} KB."
fi