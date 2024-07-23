#!/bin/bash


#SBATCH --time 2:00:00
#SBATCH --cpus-per-task=3
#SBATCH --nodes=2  
#SBATCH --mem=4000  
#SBATCH --output=output.csv
#SBATCH --partition=short

#module load python/3.9-2022.05 # load python


parallel -j 2 python3 assignment3.py -j 2 ::: fastqfile.fastq > $output
