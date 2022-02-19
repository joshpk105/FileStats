import argparse
import sys
import os.path
import re
from hashlib import md5
from base64 import b64encode
import json

whitespace = re.compile('\s+')

class KeywordCounter:
    def __init__(self, keyword_file):
        self.key_count = {}
        with open(keyword_file, "r") as k_in:
            for key in k_in:
                self.key_count[key.strip()] = 0
    
    # Memory complexity O(tokens)
    # Time complexity O(tokens)
    # Do text normalization for better word identification
    def process_tokens(self, tokens : list):
        counted = {}
        for t in tokens:
            if t not in counted and t in self.key_count:
                print("Counted: {}".format(t))
                self.key_count[t] += 1
            counted[t] = True

    def __str__(self):
        return json.dumps(self.key_count)

def main():
    parser = argparse.ArgumentParser(description="Process files into raw stats.")
    parser.add_argument('files', metavar='N', type=str, nargs='+', help='Files to be processed')
    parser.add_argument('--keywords', dest="keywords", type=str, help='File with keywords to be counted.')
    args = parser.parse_args()

    if not os.path.isfile(args.keywords):
        sys.stderr.write("Keyword file not found: {}\n".format(args.keywords))
    kc = KeywordCounter(args.keywords)

    for f in args.files:
        print("Processing: {}".format(f))
        if not os.path.isfile(f):
            sys.stderr.write("Provided path is not found: {}\n".format(f))
            continue
        # Assuming all files are utf-8
        with open(f, "r", encoding="utf-8") as f_in:
            for line in f_in:
                print(ProcessLine(line, kc))
    print(str(kc))

# Could be optimized by iterating over the line characters instead of splitting
def ProcessLine(line : str, kc : KeywordCounter):
    char_count = len(line)
    # Change hashing algorithm for more accurate duplicate line counting
    m = md5()
    m.update(line.encode("utf-8"))
    line_hash = b64encode(m.digest())
    line = line.strip()
    tokens = whitespace.split(line)
    token_count = len(tokens)
    kc.process_tokens(tokens)
    return [char_count, token_count, line_hash]

if __name__ == "__main__":
    main()