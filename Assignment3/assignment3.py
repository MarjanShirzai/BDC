#!/usr/bin/env python3

"""
This python script calculates PHRED value from the given fastq file
 and calculates the average PHRED scores per base

usage:
python3 assignment1.py -n <aantal chuncks> fastabestand1.fastq [fastabestand2.fastq ... fastabestandN.fastq]

"""

__author__ = "Marjan Shirzai"
__status__ = "Finished"
__version__ = "0.1"

import argparse
import os

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
    parser = argparse.ArgumentParser(description=
    'Process FastQ file to produce average PHRED scores per base')
    parser.add_argument("fastqFile", help= "The fastq file", nargs="*")
    parser.add_argument('-n',"--cores" ,
    help = "The number of cores you want to use", type=int, required=True)

    args = parser.parse_args()

    for count, files in enumerate(args.fastqFile):

        # Creating chunks
        file_information = os.stat(files)
        chunks = round(file_information.st_size / args.cores)

        start_position = 0
        end_position = chunks
        numbers = []
        counters = []
        # creating the processes
        for process_number in range(0, args.cores):
            scores, counts = phred_score(files, start_position,end_position )
            numbers.append(scores)
            counters.append(counts)
            start_position += chunks
            end_position += chunks

        results = [sum(x) for x in zip(*numbers)]
        counts = [sum(x) for x in zip(*counters)]
        average = []
        for average_num in range(0, len(results)):
            num = results[average_num] / counts[average_num]
            average.append(num)
        print("{}".format(files))
        print(average)


if __name__ == "__main__":
    #start_time = time.time()
    main()
    #print("--- %s seconds ---" % (time.time() - start_time))
