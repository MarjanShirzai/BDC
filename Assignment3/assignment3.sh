#!/bin/bash

source ~/../../commons/conda/conda_load.sh

#SBATCH --time 2:00:00
#SBATCH --cpus-per-task=4
#SBATCH --nodes=1  
#SBATCH --mem=4000  

if [ -z "$*" ]; then echo "please provide one or more fastq file as argument";fi

python3 assignment3.py -n 2 $1

