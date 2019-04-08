#!/bin/bash -l


#SBATCH -q premium
#SBATCH -N 80
#SBATCH --gres=craynetwork:2
#SBATCH -t 3:00:00
#SBATCH -L SCRATCH
#SBATCH -C haswell
#SBATCH -J INSERT_INDEX_80
#SBATCH -A m2621
#SBATCH --mem=40GB
#SBATCH -o /global/cscratch1/sd/wzhang5/data/miqs/o%j.insert_index_80
#SBATCH -e /global/cscratch1/sd/wzhang5/data/miqs/o%j.insert_index_80
# #DW jobdw capacity=2000GB access_mode=striped type=scratch pool=sm_pool


N_NODE=80

DATASET_NAME="/global/cscratch1/sd/houhun/h5boss_v1"
COUNT=$N_NODE
ATTRNUM=0
PERSISTENCE_TYPE=1
INDEX_DIR=/global/cscratch1/sd/wzhang5/data/miqs/idx80

PROC_CMD="--cpu_bind=cores --ntasks-per-node=1 -c 1 --mem=40960 --gres=craynetwork:1"

PROC=/global/homes/w/wzhang5/software/MIQS/build/bin/hdf5_set_2_index

srun -N $N_NODE -n $N_NODE $PROC_CMD $PROC $DATASET_NAME $COUNT $ATTRNUM $PERSISTENCE_TYPE $INDEX_DIR