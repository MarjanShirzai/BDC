#!/usr/bin/env python3

"""
This python script calculates the average PHRED scores per base from the given fastq file.
The file will be spread over networks to speed up the calculation. 

usage:
      python3 assignment2.py -s rnaseqfile.fastq --host <een workstation> --port <een poort> --chunks <een getal>
      python3 assignment2.py -c --host <diezelfde host als voor server> --port <diezelfde poort als voor server> -n <aantal cpus in client computer>

"""

__author__ = "Marjan Shirzai"
__status__ = "In progress"
__version__ = "0.1"

import argparse
import time
import multiprocessing as mp
import csv
import os



def main():
    """
    Arguments are passed and results are processed in the main.
    """




if __name__ == "__main__":
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
