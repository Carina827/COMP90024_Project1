#!/bin/bash
#SBATCH --partition=physical
#SBATCH --time=0-00:05:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --output=1n1c.out

module load Python/3.5.2-goolf-2015a

time mpiexec python3 Assignment1.py
