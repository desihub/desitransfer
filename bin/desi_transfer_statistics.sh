#!/bin/bash
#
# Collect (meta) data on files transferred for annual reporting purposes.
#
year=$1
next_year=$(( year + 1 ))
# fy=${year}1[012][0-9][0-9] ${next_year}0[0-9][0-9][0-9]
number_of_nights=0
number_of_exposures=0
for n in ${DESI_SPECTRO_DATA}/${year}1[012][0-9][0-9] ${DESI_SPECTRO_DATA}/${next_year}0[0-9][0-9][0-9]; do
    night=$(basename ${n})
    number_of_nights=$(( number_of_nights + 1 ))
    for e in ${DESI_SPECTRO_DATA}/${night}/*; do
        expid=$(basename ${e})
        number_of_exposures=$(( number_of_exposures + 1 ))
    done
done
echo "Number of nights = ${number_of_nights}."
echo "Number of exposures = ${number_of_exposures}."