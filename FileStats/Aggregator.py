#!/usr/local/bin/python
import pandas
import argparse
import sys
import os
import subprocess
from math import ceil
import Processor
import csv
from shutil import rmtree
from time import sleep

class Cluster:
    def __init__(self, processors, key_file, report, chunk, files):
        self.files_per_job = chunk
        self.files = files
        self.key_file = key_file
        self.report_folder = report
        self.statistics = os.path.join(report, "statistics.txt")
        self.processors = processors
        self.popen = []
        self.jobs = []

    def wait_all(self):
        for p in self.popen:
            p.wait()

    def write_report(self):
        line_stats_res = []
        key_count = None
        for i in range(max(self.processors, len(self.jobs))):
            stats_in = os.path.join(self.report_folder, str(i), "line.stats.csv")
            key_in = os.path.join(self.report_folder, str(i), "keywords.counts.csv")
            if key_count is None:
                key_count = pandas.read_csv(key_in)
            else:
                curr = pandas.read_csv(key_in)
                key_count = key_count + curr
            line_stats_res.append(pandas.read_csv(stats_in))
        
        line_stats = pandas.concat(line_stats_res)
        unique_count = line_stats["LineHash"].nunique()
        dup_count = len(line_stats)-unique_count
        medians = line_stats.median(numeric_only=True)
        stds = line_stats.std(numeric_only=True)
        
        with open(self.statistics, "w") as stats_out:
            stats_writer = csv.writer(stats_out, delimiter='\t')
            stats_writer.writerow(["num dupes", dup_count])
            stats_writer.writerow(["med length", medians.loc["CharCount"]])
            stats_writer.writerow(["std length", stds.loc["CharCount"]])
            stats_writer.writerow(["med tokens", medians.loc["TokenCount"]])
            stats_writer.writerow(["std tokens", stds.loc["TokenCount"]])
            key_count.transpose().to_csv(stats_out, sep='\t', header=False)

    # Delete no longer needed Processor output
    def cleanup(self):
        for i in range(max(self.processors, len(self.jobs))):
            rmtree(os.path.join(self.report_folder,str(i)))

    # Split input files amoung all processors evenly and blindly
    def naive_schedule(self):
        split = ceil(len(self.files) / self.processors)
        start = 0
        for i in range(self.processors):
            end = min(len(self.files), start+split)
            input_files = self.files[start:end]
            params = [Processor.__file__, "--keywords", self.key_file,
                "--report", os.path.join(self.report_folder, str(i))]
            params.extend(input_files)
            self.popen.append(subprocess.Popen(params))
            start = end

    def chunk_schedule_jobs(self):
        jobs = ceil(len(self.files) / self.files_per_job)
        start = 0
        for i in range(jobs):
            end = min(len(self.files), start+self.files_per_job)
            input_files = self.files[start:end]
            params = [Processor.__file__, "--keywords", self.key_file,
                "--report", os.path.join(self.report_folder, str(i))]
            params.extend(input_files)
            print("{}: {}".format(i, input_files))
            self.jobs.append(params)
            start = end
    
    def run_jobs(self):
        current_job = 0
        for i in range(self.processors):
            self.popen.append(subprocess.Popen(self.jobs[current_job]))
            current_job += 1
        print("Current_job: {}".format(current_job))
        while current_job < len(self.jobs):
            for i in range(len(self.popen)):
                if current_job >= len(self.jobs):
                    break
                if self.popen[i].poll() is not None:
                    self.popen[i] = subprocess.Popen(self.jobs[current_job])
                    current_job += 1
                    print("Current_job: {}".format(current_job))
        self.wait_all()



def main():
    parser = argparse.ArgumentParser(description="Manage multiple FileStats Processors")
    parser.add_argument("--processors", dest="processors", type=int, 
        default=1, help="Number of processors to spawn.")
    parser.add_argument("--keywords", type=str, required=True, 
        help="File with keywords to count.")
    parser.add_argument("--report", type=str, required=True, 
        help="Folder to save results.")
    parser.add_argument('--file', type=str, nargs='+',
        help='File to be processed')
    parser.add_argument('--chunk', type=int, default=10)
    parser.add_argument('--file_list', type=str, 
        help="File containing one filepath per line to be processed.")
    args = parser.parse_args()

    files = []
    if args.file is not None:
        files.extend(args.file)

    # Read file list
    if args.file_list is not None:
        with open(args.file_list, "r") as fl_in:
            for f in fl_in:
                files.append(f.strip())

    cluster = Cluster(args.processors, args.keywords, 
        args.report, args.chunk, files)
    cluster.chunk_schedule_jobs()
    cluster.run_jobs()
    cluster.write_report()
    cluster.cleanup()

if __name__ == "__main__":
    main()