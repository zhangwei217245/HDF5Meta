#!/bin/bash
#SBATCH --job-name=string0CHECKSUM
#SBATCH --output=%x.o%j
#SBATCH --error=%x.e%j
#SBATCH --partition nocona
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --time=48:00:00
##SBATCH --mem-per-cpu=8192MB  ##3.9GB, Modify based on needs

module load gcc
module load cmake
module load openmpi
module load hdf5

export MIQS_HOME=/home/zhang56/software/miqs
export LD_LIBRARY_PATH=${MIQS_HOME}/target/debug/lib/:${LD_LIBRARY_PATH}
mpirun --prefix /home/zhang56/software/miqs/target/debug ./HDF5_string_index_test CHECKSUM 1000