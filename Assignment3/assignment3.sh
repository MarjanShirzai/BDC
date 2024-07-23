#!/bin/bash


#SBATCH --time 2:00:00
#SBATCH --cpus-per-task=4
#SBATCH --nodes=2  
#SBATCH --mem=4000  
#SBATCH --output=output.csv
#SBATCH --partition=short

#module load python/3.9-2022.05 # load python
output="output.csv"
cores=4

parallel -j $cores  python3 assignment3.py -n $cores ::: fastqfile.fastq fastqfile2.fastq > $output
