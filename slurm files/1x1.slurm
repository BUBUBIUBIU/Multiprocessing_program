#!/bin/bash
#SBATCH -J assignment_1_1x1
#SBATCH -p cloud
#SBATCH -t 0-00:10:00
#SBATCH -N 1
#SBATCH --account=COMP90024
#SBATCH --ntasks=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --output=1_1.txt

module load Python/3.6.4-intel-2017.u2

time mpirun python3 Assignment1_beta1.py bigTwitter.json melbGrid.json
