#!/bin/bash
#SBATCH --partition=physical
#SBATCH --time=0-00:05:00
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4

#SBATCH --output=2n8c.out

# Load required modules
module load Python/3.5.2-goolf-2015a

time mpiexec python3 Assignment1.py
