#!/bin/bash -l


#SBATCH -N 20
#SBATCH --gres=craynetwork:2
#SBATCH -C haswell
#SBATCH -q regular
#SBATCH -J TEST_MOGO
#SBATCH --mail-user=zhangwei217245@gmail.com
#SBATCH --mail-type=ALL
#SBATCH -t 02:00:00
#SBATCH -A m2621
#SBATCH -o o%j.mongo_10
#SBATCH -e o%j.mongo_10


# SBATCH -N 10
# SBATCH -t 0:30:00
# SBATCH --gres=craynetwork:2
# SBATCH -L SCRATCH
# SBATCH -C haswell
# SBATCH -J TEST_MONGO_10
# SBATCH -A m2621
# SBATCH -o o%j.mongo_10
# SBATCH -e o%j.mongo_10
# #DW jobdw capacity=2000GB access_mode=striped type=scratch pool=sm_pool


N_NODE=100

DATASET_NAME="/global/cscratch1/sd/houhun/h5boss_v1"
FILECOUNT=12
ATTRNUM=16

PROC_CMD="--cpu_bind=cores --ntasks-per-node=1 -c 1 --mem=10240 --gres=craynetwork:1"


PROC=/global/homes/w/wzhang5/software/HDF5Meta/build/bin/hdf5_set_2_mongo

srun -N $N_NODE -n $N_NODE $PROC_CMD $PROC $DATASET_NAME $FILECOUNT $ATTRNUM