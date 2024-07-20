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

POISONPILL = "MEMENTOMORI"
ERROR = "DOH"
IP = ''
PORTNUM = 5381
AUTHKEY = b'whathasitgotinitspocketsesss?'
data = ["Always", "look", "on", "the", "bright", "side", "of", "life!"]

def make_server_manager(port, authkey):
    """ Create a manager for the server, listening on the given port.
        Return a manager object with get_job_q and get_result_q methods.
    """
    job_q = queue.Queue()
    result_q = queue.Queue()

    # This is based on the examples in the official docs of multiprocessing.
    # get_{job|result}_q return synchronized proxies for the actual Queue
    # objects.
    class QueueManager(BaseManager):
        pass

    QueueManager.register('get_job_q', callable=lambda: job_q)
    QueueManager.register('get_result_q', callable=lambda: result_q)

    manager = QueueManager(address=('', port), authkey=authkey)
    manager.start()
    print('Server started at port %s' % port)
    return manager

def runserver(fn, data):
    print("In def server")
    # Start a shared manager server and access its queues
    manager = make_server_manager(PORTNUM, b'whathasitgotinitspocketsesss?')
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()

    if not data:
        print("Gimme something to do here!")
        return
    print("Sending data!")
    for d in data:
        shared_job_q.put({'fn': fn, 'arg': d})
    time.sleep(2)



def phred_score(inputfile, output, start_size, end_size):
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
        output.put([phred_score, counters])



def main():
    """
    Arguments are passed and results are processed in the main.
    """
    
    argparser = ap.ArgumentParser(
        description="Script voor Opdracht 2 van Big Data Computing;  Calculate PHRED scores over the network.")
    mode = argparser.add_mutually_exclusive_group(required=True)
    mode.add_argument("-s", action="store_true", dest="server",help="Run the program in Server mode; see extra options needed below")
    mode.add_argument("-c", action="store_true", dest="client",help="Run the program in Client mode; see extra options needed below")


    server_args = argparser.add_argument_group(title="Arguments when run in server mode")
    server_args.add_argument("-o", action="store", dest="csvfile", type=ap.FileType('w', encoding='UTF-8'),
                             required=False,
                             help="CSV file om de output in op te slaan. Default is output naar terminal STDOUT")
    server_args.add_argument("fastq_files", action="store", type=ap.FileType('r'), nargs='*',
                             help="Minstens 1 Illumina Fastq Format file om te verwerken")
    server_args.add_argument("--chunks", action="store", type=int, required=True)

    client_args = argparser.add_argument_group(title="Arguments when run in client mode")
    client_args.add_argument("-n", action="store",
                             dest="n", required=False, type=int,
                             help="Aantal cores om te gebruiken per host.")
    argparser.add_argument("--host", action="store", type=str, help="The hostname where the Server is listening")
    argparser.add_argument("--port", action="store", type=int, help="The port on which the Server is listening")
    args = argparser.parse_args()

    if args.server:
        runserver(phred_score, args.fastq_files)
    elif args.client:
        print("In the client")



if __name__ == "__main__":
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))