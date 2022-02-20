#!/usr/local/bin/python
import argparse
import sys
import os
import re
from hashlib import md5
from base64 import b64encode
import json
import csv

whitespace = re.compile('\s+')

# Handles the statistics building and writing
class LineStats:
    def __init__(self, keyword_file, report):
        os.makedirs(report, exist_ok=True)
        self.print_cmd(os.path.join(report, "cmd.sh"))
        self.count_out = open(os.path.join(report, "keywords.counts.csv"), "w")
        self.count_writer = csv.writer(self.count_out)
        self.stats_out = open(os.path.join(report, "line.stats.csv"), 'w')
        self.stats_writer = csv.writer(self.stats_out)
        self.stats_writer.writerow(["CharCount", "TokenCount", "LineHash"])
        self.key_count = {}
        with open(keyword_file, "r") as k_in:
            for key in k_in:
                self.key_count[key.strip()] = 0

    # Print command line info
    def print_cmd(self, cmd_file):
        with open(cmd_file, "w") as cmd_out:
            cmd_out.write(" ".join(sys.argv)+"\n")

    # Could be memoryoptimized by iterating over the line 
    # characters instead of splitting.
    # Memory complexity O(3*line+hash)
    def process_line(self, line : str):
        char_count = len(line)
        # Change hashing algorithm for more accurate duplicate line counting
        m = md5()
        m.update(line.encode("utf-8"))
        line_hash = str(b64encode(m.digest()))
        line = line.strip()
        tokens = whitespace.split(line)
        token_count = len(tokens)
        self.process_tokens(tokens)
        self.stats_writer.writerow([char_count, token_count, line_hash])
    
    # Memory complexity O(2*tokens)
    # Time complexity O(tokens)
    # Do text normalization for better word identification
    def process_tokens(self, tokens : list):
        counted = {}
        for t in tokens:
            if t not in counted and t in self.key_count:
                self.key_count[t] += 1
            counted[t] = True
    
    # Write counts to json file to avoid conflict of line stats in stdout
    def report(self):
        header = []
        counts = []
        for k in sorted(self.key_count.keys()):
            header.append(k)
            counts.append(self.key_count[k])
        self.count_writer.writerow(header)
        self.count_writer.writerow(counts)
        self.count_out.close()
        self.stats_out.close()

def main():
    parser = argparse.ArgumentParser(description="Process files into raw stats.")
    parser.add_argument('file', metavar="file", type=str, nargs='+',
        help='Files to be processed')
    parser.add_argument('--keywords', required=True, dest="keywords", type=str, 
        help='File with keywords to be counted.')
    parser.add_argument('--report', required=True, dest="report", type=str,
        help="The folder path to save results to.")
    args = parser.parse_args()

    files = []
    if args.file is not None:
        files.extend(args.file)

    if not os.path.isfile(args.keywords):
        sys.stderr.write("Keyword file not found: {}\n".format(args.keywords))
        return
    ls = LineStats(args.keywords, args.report)

    for f in files:
        if not os.path.isfile(f):
            sys.stderr.write("Provided path is not found: {}\n".format(f))
            continue
        # Assuming all files are utf-8
        with open(f, "r", encoding="utf-8") as f_in:
            for line in f_in:
                ls.process_line(line)
    ls.report()

if __name__ == "__main__":
    main()