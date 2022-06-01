#!/usr/bin/env python3

"""
This python script uses the SeqIO module to get the PHRED value from the given fastq file
 and calculates the average PHRED scores per base

usage: 
    python3 script.py -fastq {fastq file name} -n {process number} -o {optional csv output file}
"""

__author__ = "Marjan Shirzai"
__status__ = "Finished"
__version__ = "0.1"

import argparse
import time
import multiprocessing as mp
import csv
import os

# Defining an output queue
output = mp.Queue()

def phredScore(inputfile, output,  startSize, endSize ):
    # Open the given input file
    with open(inputfile, "r") as fastq:
        fastq.seek(startSize)
        phredscore = []
        counters = []

        # The tell() method returns the current file position in a file stream.
        while fastq.tell() <= endSize:
            line = fastq.readline()
            if line == "":
                break
            if line.startswith("+"):
                fileLines = fastq.readline()
                qualityString = [ord(character) - 33 for character in fileLines[:-1]]
                phredScoreLen = len(qualityString)

                # Adding zero(s) to the lists is the lists are not the same length
                for phred in range(phredScoreLen):
                    try:
                        if len(phredscore) < len(qualityString):
                            phredscore[phred] = 0
                            counters[phred] = 0
                    except IndexError:
                        phredscore.append(0)
                        counters.append(0)
                    phredscore[phred] += qualityString[phred]
                    counters[phred] += 1
        output.put([phredscore, counters])

def main():
    parser = argparse.ArgumentParser(description='Process FastQ file to produce average PHRED scores per base')
    parser.add_argument("-fastq", "--fastqFile", help= "The fastq file", required=True)
    parser.add_argument("-o", "--output",help="Directs the output to the given output name")
    parser.add_argument('-n',"--cores" ,help = "The number of cores you want to use", type=int, required=True)

    args = parser.parse_args()

    processes = []

    # Creating chunks
    fileInformation = os.stat(args.fastqFile)
    chunks = round(fileInformation.st_size / args.cores)

    startPosition = 0
    endPosition = chunks

    # creating the processes
    for processNumber in range(0, args.cores):
        print("Process started")
        processRange = mp.Process(target=phredScore, args=(args.fastqFile, output, startPosition, endPosition))
        processes.append(processRange)
        processRange.start()
        startPosition += chunks
        endPosition += chunks

    numbers = []
    counters = []
    average = []

    #Closing processes and
    for process in processes:
        process.join()
        scores, index = output.get()
        numbers.append(scores)
        counters.append(index)

    results = [sum(x) for x in zip(*numbers)]
    counts = [sum(x) for x in zip(*counters)]

    for av in range(0, len(results)):
        num = results[av] / counts[av]
        average.append(num)

    # If the output file is given as argument than print the results to the output file
    if args.output:
        with open(args.output, "w") as myFile:
            toWrite = csv.writer(myFile)
            toWrite.writerow(["Base nr","Average PHRED value"])
            for index, score in enumerate(average):
                toWrite.writerow([index, score])

    print("Finished")
if __name__ == "__main__":
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))