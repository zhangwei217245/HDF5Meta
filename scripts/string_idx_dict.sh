#!/bin/bash -l


#SBATCH -p debug
#SBATCH -N 4
#SBATCH -t 0:30:00
#SBATCH --gres=craynetwork:2
#SBATCH -L SCRATCH
#SBATCH -C haswell
#SBATCH -J STRING_IDX_DICT
#SBATCH -A m2621
#SBATCH -o o%j.dict_4
#SBATCH -e o%j.dict_4
# #DW jobdw capacity=2000GB access_mode=striped type=scratch pool=sm_pool


N_NODE=4

DATASET_NAME="DICT"
COUNT=""


PROC_CMD="--cpu_bind=cores --ntasks-per-node=1 -c 1 --mem=10240 --gres=craynetwork:1"

PROC=/global/homes/w/wzhang5/software/MIQS/build/bin/hdf5_string_index_test

srun -N $N_NODE -n $N_NODE $PROC_CMD $PROC $DATASET_NAME $COUNT 