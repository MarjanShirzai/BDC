#!/bin/bash

source ~/../../commons/conda/conda_load.sh

#SBATCH --time 2:00:00
#SBATCH --cpus-per-task=4
#SBATCH --nodes=1  
#SBATCH --mem=4000  

python3 assignment3.py -n 2 fastqfile.fastq > testOutput.txt
