
# This reads the whole file upfront, and then iterates lines in reverse.
# The advantage is less churn in d, as only the last (now first seen) addition
# or deletion decides each positions fate, all others can be ignored.
#
# Given enough memory and fast IO this appears to be somewhat faster than the
# original code.

import sys

def process(filename):
    d = {}
    seen = set()
    with open(filename, 'r') as f:
        lines = f.readlines()
    for l in reversed(lines):
        v = l.split(" ")
        b = int(v[0])
        i = int(v[2])
        o = int(v[3])
        for x in range(4, 4+i):
            k = v[x]
            if k not in seen:
                seen.add(k)
        for x in range(4+i, 4+i+o):
            k = v[x+o]
            if k not in seen:
                seen.add(k)
                d[k] = (b, float(v[x]))
    return d

def main():
    argc = len(sys.argv)
    filename = sys.argv[1] if argc > 1 else 'data_test'
    result = process(filename)
    print(len(result))

if __name__ == '__main__':
    main()
