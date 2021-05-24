
# the original code, cleaned up

import sys

def process(filename):
    d = {}
    with open(filename, 'r') as f:
        for l in f:
            v = l.split(" ")
            b = int(v[0])
            i = int(v[2])
            o = int(v[3])
            for x in range(4+i, 4+i+o):
                d[v[x+o]] = (b, float(v[x]))
            for y in range(4, 4+i):
                del d[v[y]]
    return d

def main():
    argc = len(sys.argv)
    filename = sys.argv[1] if argc > 1 else 'data_test'
    result = process(filename)
    print(len(result))

if __name__ == '__main__':
    main()
