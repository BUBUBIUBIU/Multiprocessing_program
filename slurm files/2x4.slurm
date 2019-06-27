#!/bin/bash
#SBATCH -J assignment_1_2x4
#SBATCH -p physical
#SBATCH -t 0-00:10:00
#SBATCH -N 2
#SBATCH --account=COMP90024
#SBATCH --ntasks=8
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=1
#SBATCH --output=2_4.txt

module load Python/3.6.4-intel-2017.u2

time mpirun python3 Assignment1_beta1.py bigTwitter.json melbGrid.json
