#!/bin/bash -l


#SBATCH -q debug
#SBATCH -N 4
#SBATCH -t 0:30:00
#SBATCH -L SCRATCH
#SBATCH -C haswell
#SBATCH -J INSERT_MONGO_20
#SBATCH -A m2621
#SBATCH --mem=40GB
#SBATCH -o /global/cscratch1/sd/wzhang5/data/miqs/o%j.insert_mongo_20
#SBATCH -e /global/cscratch1/sd/wzhang5/data/miqs/o%j.insert_mongo_20
# #DW jobdw capacity=2000GB access_mode=striped type=scratch pool=sm_pool


N_NODE=4

DATASET_NAME="/global/cscratch1/sd/houhun/h5boss_v1"
COUNT=$N_NODE
ATTRNUM=16
TASK=0;
LOG_NAME="${SCRATCH}/data/miqs/insert_mongo_20.txt"

PROC_CMD="--cpu_bind=cores --ntasks-per-node=1 -c 64 --mem=40960 --gres=craynetwork:1"

# ./bin/hdf5_set_2_mongo /global/cscratch1/sd/houhun/h5boss_v1 100 16

PROC=/global/homes/w/wzhang5/software/HDF5Meta/build/bin/hdf5_set_2_mongo

srun -N $N_NODE -n $N_NODE $PROC_CMD $PROC $DATASET_NAME $COUNT $ATTRNUM $TASK