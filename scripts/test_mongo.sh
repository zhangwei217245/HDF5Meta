#!/bin/bash

nohup ./bin/hdf5_set_2_mongo /global/cscratch1/sd/houhun/h5boss_v1 100 16 > mongotest_100_16.txt &
#nohup ./sim_mongo.sh > mongotest_100_16.txt &

fakeidx=1
old_num_file=0
echo ""> mongo_mem.txt
while [ $fakeidx -eq 1 ];do
    if [ $num_file -eq 100 ];then
        break
    fi
    num_file=`grep -c "[PARSEFILE]" mongotest_100_16.txt`
    if [ $num_file -gt $old_num_file  ];then
        mongo mongodb03.nersc.gov/HDF5MetadataTest -u HDF5MetadataTest_admin -p ekekek19294jdwss2k --eval 'db.runCommand({dbStats:1, scale:1024})' | egrep "(indexSize|dataSize)" | xargs echo >> mongo_mem.txt
        old_num_file=$num_file
    fi
    sleep 3s
done


    #for i in $(seq 0 16); do 
    #    ./bin/hdf5_set_2_mongo /global/cscratch1/sd/houhun/h5boss_v1 8 $i
    #    mongo mongodb03.nersc.gov/HDF5MetadataTest -u HDF5MetadataTest_admin -p ekekek19294jdwss2k --eval 'db.runCommand({dbStats:1, scale:1024})' | egrep "(indexSize|dataSize)" | xargs echo    
    #done

