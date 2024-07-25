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

import argparse as ap
import time
import multiprocessing as mp
from multiprocessing.managers import BaseManager, SyncManager
import os, sys, time, queue
import csv
import os

POISONPILL = "MEMENTOMORI"
ERROR = "DOH"
IP = ''
portum = 5381
AUTHKEY = b'whathasitgotinitspocketsesss?'
data = ["Always", "look", "on", "the", "bright", "side", "of", "life!"]



def peon(job_q, result_q):
    my_name = mp.current_process().name
    while True:
        try:
            job = job_q.get_nowait()
            if job == POISONPILL:
                job_q.put(POISONPILL)
                print("Aaaaaaargh", my_name)
                return
            else:
                try:
                    #result = job['fn'](job['arg'])
                    result = job['fn'](job['files'], job['start_position'], job['end_position'])
                    print("Peon %s Workwork on %s!" % (
                    my_name, (job['files'], job['start_position'], job['end_position'])))
                    result_q.put({'job': job, 'result': result})
                except NameError:
                    print("Can't find yer fun Bob!")
                    result_q.put({'job': job, 'result': ERROR})

        except queue.Empty:
            print("sleepytime for", my_name)
            time.sleep(1)

def run_workers(job_q, result_q, num_processes):
    processes = []
    for p in range(num_processes):
        temP = mp.Process(target=peon, args=(job_q, result_q))
        processes.append(temP)
        temP.start()
    print("Started %s workers!" % len(processes))
    for temP in processes:
        temP.join()


def make_client_manager(ip, port, authkey):
    """ Create a manager for a client. This manager connects to a server on the
        given address and exposes the get_job_q and get_result_q methods for
        accessing the shared queues from the server.
        Return a manager object.
    """
    class ServerQueueManager(BaseManager):
        pass

    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')

    manager = ServerQueueManager(address=(ip, port), authkey=authkey)
    manager.connect()

    print('Client connected to %s:%s' % (ip, port))
    return manager

def runclient(num_processes, portum):
    manager = make_client_manager(IP, portum, AUTHKEY)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    run_workers(job_q, result_q, num_processes)

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


def phred_score(inputfile, start_size, end_size):
    print("In the phred!")

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
        return [phred_score,counters]


def runserver(fn, data, given_chunks, portum):
    print("In def server")
    # Start a shared manager server and access its queues
    manager = make_server_manager(portum, b'whathasitgotinitspocketsesss?')
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()
    

    if not data:
        print("Gimme something to do here!")
        return
    print("Sending data!")

    for count, files in enumerate(data):
        file_information = os.stat(files.name)
        chunks = round(file_information.st_size / given_chunks)
        print("MAKING chunckssssssssss")
        start_position = 0
        end_position = chunks

# Creating the processes
    for process_number in range(given_chunks):
        job_data = {'fn': fn, 'files': files.name, 'start_position': start_position, 'end_position': end_position}
        shared_job_q.put(job_data)
        start_position += chunks
        end_position += chunks
        time.sleep(2)
    
    results = []
    while True:
        try:
            result = shared_result_q.get_nowait()
            results.append(result)
            print("Got result!")
            if len(results) == len(data):
                print("Got all results!")
                break
        except queue.Empty:
            time.sleep(1)
            continue
    # Tell the client process no more data will be forthcoming
    print("Time to kill some peons!")
    shared_job_q.put(POISONPILL)
    # Sleep a bit before shutting down the server - to give clients time to
    # realize the job queue is empty and exit in an orderly way.
    time.sleep(5)
    print("Aaaaaand we're done for the server!")
    manager.shutdown()

    #print(results)
    numbers = []
    counters = []
    average = []

    # Closing processes and
    # for process in processes:
    #     process.join()
    #     scores, index = output.get()
    #     numbers.append(scores)
    #     counters.append(index)
    #
    # results = [sum(x) for x in zip(*numbers)]
    # counts = [sum(x) for x in zip(*counters)]
    #
    # for average_num in range(0, len(results)):
    #     num = results[average_num] / counts[average_num]
    #     average.append(num)


    for item in results:
        job = item['job']
        file_name = job['files']
        start_position = job['start_position']
        end_position = job['end_position']
        result = item['result']
        scores, index = result
        numbers.append(scores)
        counters.append(index)

    print(file_name)
    results_sum = [sum(x) for x in zip(*numbers)]
    counts_sum = [sum(x) for x in zip(*counters)]
    for average_num in range(0, len(results_sum)):
        num = results_sum[average_num] / counts_sum[average_num]
        average.append(num)

    print(average)


def main():
    """
    Arguments are passed and results are processed in the main.
    """
    #Make the portum global so it can be changed by the input.
    global portum

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
    server_args.add_argument("--chunks", action="store", type=int, required=False)

    client_args = argparser.add_argument_group(title="Arguments when run in client mode")
    client_args.add_argument("-n", action="store",
                             dest="n", required=False, type=int,
                             help="Aantal cores om te gebruiken per host.")
    argparser.add_argument("--host", action="store", type=str, help="The hostname where the Server is listening")
    argparser.add_argument("--port", action="store", type=int, help="The port on which the Server is listening")
    args = argparser.parse_args()

    if args.port:
        portum = args.port

    if args.server:
        runserver(phred_score, args.fastq_files, args.chunks, portum)
        #server = mp.Process(target=runserver, args=(phred_score, args.fastq_files, args.chunks ))
        #server.start()
        #time.sleep(1)
        #server.join()
    elif args.client:
        print("In client")
        if args.n is None:
            argparser.error("Please give the core -n<number>")
        else:
            runclient(args.n, portum)
        # client = mp.Process(target=runclient, args=(4,))
        # client.start()
        #runclient(args.n, portum)


if __name__ == "__main__":
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
