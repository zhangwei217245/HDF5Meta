#/bin/bash


for i in $(seq 0 16); do echo "$i";./bin/hdf5_set_2_index /global/cscratch1/sd/houhun/h5boss_v1 8 $i ;done
