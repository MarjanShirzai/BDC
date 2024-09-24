#!/bin/bash


#SBATCH --time 2:00:00
#SBATCH --ntasks=5 
#SBATCH --nodes=2  
#SBATCH --mem=4000  
#SBATCH --output=assignment4-%j.out


mpirun --oversubscribe -np 5 python3 assignment4.py /commons/Themas/Thema12/HPC/rnaseq_selection.fastq 