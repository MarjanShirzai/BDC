#!/usr/bin/env python3

"""
This python script calculates PHRED value from the given fastq file
 and calculates the average PHRED scores per base with help of MPI.

usage:
mpirun -np <Processors> python3 /commons/Themas/Thema12/HPC/rnaseq_selection.fastq

Bron: https://earthinversion.com/data-science/mastering-large-data-processing-with-mpi4py-in-python/

"""

__author__ = "Marjan Shirzai"
__status__ = "Finished"
__version__ = "0.1"

import argparse
import os
from mpi4py import MPI


def phred_score(inputfile, start_size, end_size):
    """
    Calculating phred score
    """
    # Open the given input file
    with open(inputfile, "r") as fastq:
        fastq.seek(start_size)
        phred_score = []
        counters = []

        # The tell() method returns the current file position in a file stream.
        while fastq.tell() <= end_size:
            line = fastq.readline()
            if line == "":
                break
            if line.startswith("+"):
                file_lines = fastq.readline()
                quality_string = [ord(character) - 33 for character in file_lines[:-1]]
                phred_score_len = len(quality_string)

                # Adding zero(s) to the lists is the lists are not the same length
                for phred in range(phred_score_len):
                    try:
                        if len(phred_score) < len(quality_string):
                            phred_score[phred] = 0
                            counters[phred] = 0
                    except IndexError:
                        phred_score.append(0)
                        counters.append(0)
                    phred_score[phred] += quality_string[phred]
                    counters[phred] += 1
        return phred_score, counters

def main():
    """
    Arguments are passed and results are processed in the main.
    """
    comm = MPI.COMM_WORLD
    comm_size = comm.Get_size()
    my_rank = comm.Get_rank()

    parser = argparse.ArgumentParser(description=
    'Process FastQ file to produce average PHRED scores per base')
    parser.add_argument("fastqFile", help= "The fastq file")

    args = parser.parse_args()

    #First rank makes chucks
    if my_rank == 0:

        file_information = os.stat(args.fastqFile)
        chunks = round(file_information.st_size / comm_size)
        chunk_positions = []
        start_position = 0
        end_position = chunks

        for process_number in range(comm_size):
            chunk_positions.append([start_position, end_position])
            start_position = end_position
            end_position = start_position + chunks
        #print(chunk_positions)
    else:
         chunk_positions = None

    #The data is spread to processes
    start_size, end_size = comm.scatter(chunk_positions, root=0)
    process_results, process_counts = phred_score(args.fastqFile, start_size, end_size)

    #The results are gathered together
    numbers = comm.gather(process_results, root=0)
    counters = comm.gather(process_counts, root=0)

    if my_rank == 0:
        results = [sum(x) for x in zip(*numbers)]
        counts = [sum(x) for x in zip(*counters)]

        average = []
        for average_num in range(0, len(results)):
            num = results[average_num] / counts[average_num]
            average.append(num)
        print("{}".format(args.fastqFile))
        print(average)

    # for count, files in enumerate(args.fastqFile):
    #
    #     # Creating chunks
    #     file_information = os.stat(files)
    #     chunks = round(file_information.st_size / args.cores)
    #
    #     start_position = 0
    #     end_position = chunks
    #     numbers = []
    #     counters = []
    #     # creating the processes
    #     for process_number in range(0, args.cores):
    #         scores, counts = phred_score(files, start_position,end_position )
    #         numbers.append(scores)
    #         counters.append(counts)
    #         start_position += chunks
    #         end_position += chunks
    #
        # results = [sum(x) for x in zip(*numbers)]
        # counts = [sum(x) for x in zip(*counters)]
    #     average = []
    #     for average_num in range(0, len(results)):
    #         num = results[average_num] / counts[average_num]
    #         average.append(num)
    #     print("{}".format(files))
    #     print(average)

if __name__ == "__main__":
    #start_time = time.time()
    main()
