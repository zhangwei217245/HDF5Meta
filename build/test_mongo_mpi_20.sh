#!/bin/bash -l


#SBATCH -q regular
#SBATCH -N 20
#SBATCH --gres=craynetwork:2
#SBATCH --time-min=00:10:00 
#SBATCH --time=4:0:00
#SBATCH -L SCRATCH
#SBATCH -C haswell
#SBATCH -J INSERT_MONGO_20
#SBATCH -A m2621
#SBATCH --mem=40GB
#SBATCH -o /global/cscratch1/sd/wzhang5/data/miqs/o%j.insert_mongo_20
#SBATCH -e /global/cscratch1/sd/wzhang5/data/miqs/o%j.insert_mongo_20
# #DW jobdw capacity=2000GB access_mode=striped type=scratch pool=sm_pool


N_NODE=20

DATASET_NAME="/global/cscratch1/sd/houhun/h5boss_v1"
COUNT=10
ATTRNUM=16
TASK=0;

PROC_CMD="--cpu_bind=cores --ntasks-per-node=1 -c 1 --mem=40960 --gres=craynetwork:1"

PROC=/global/homes/w/wzhang5/software/HDF5Meta/build/bin/hdf5_set_2_mongo

srun -N $N_NODE -n $N_NODE $PROC_CMD $PROC $DATASET_NAME $COUNT $ATTRNUM $TASK