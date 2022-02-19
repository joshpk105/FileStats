#!/usr/local/bin/python
import pandas
import argparse
import sys
import os.path
from subprocess import Popen, PIPE
from math import ceil
import Processor
import inspect

class Cluster:
    def __init__(self):
        self.processes = []
        self.keywords = {}
        self.stats = pandas

    def add_process(self, p : Popen):
        self.processes.append(p)
        p.run()


def NaiveSchedule(args):
    # naive scheduling
    cluster = Cluster()
    processors = []
    split = ceil(len(args.files) / args.processors)
    start = 0
    for i in range(args.processors):
        end = min(len(args.files), start+split)
        input_files = '\n'.join(args.files[start:end])
        cluster.add_process(Popen([inspect(Processor), "--keywords", args.keywords], 
            stderr=PIPE, stdout=PIPE, stdin=input_files))
        start = end
    return cluster

def main():
    parser = argparse.ArgumentParser(description="Manage multiple FileStats Processors")
    parser.add_argument("--processors", dest="processors", type=int, 
        default=1, help="Number of processors to spawn.")
    parser.add_argument("--keywords", type=str, required=True, 
        help="File with keywords to count.")
    parser.add_argument('files', metavar='file', type=str, nargs='+', 
        help='Files to be processed')
    args = parser.parse_args()

    # Read files from stdin, on file per line
    for l in sys.stdin:
        args.files.append(l.strip())

    NaiveSchedule(args)
    

if __name__ == "__main__":
    main()