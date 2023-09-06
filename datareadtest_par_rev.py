
# This combines *_par and *_rev, the whole file is read and chunked upfront.
# Chunks are processed in reverse.
#
# There seems to be no time advantage over *_par, but it uses slightly less
# CPU time overall.
# It seems to work best with jobs_per_worker == 1.

import os, sys
from multiprocessing import Pool

def process(filename, workers, chunks):
    d = {}
    seen = set()
    with open(filename, 'r') as fd:
        chunksize = os.stat(fd.fileno()).st_size // chunks + 1
        chunks = [fd.readlines(chunksize) for _ in range(chunks)]
    with Pool(processes=workers) as pool:
        for result in pool.imap(process_chunk, reversed(chunks)):
            print('### chunk result', len(result[0]), len(result[1]))
            merge_result(d, seen, *result)
            print('### total', len(d))
    return d

def process_chunk(lines):
    d = {}
    seen = set()
    for l in reversed(lines):
        v = l.split(' ')
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
    return d, seen

def merge_result(d, seen, chunk_d, chunk_seen):
    for k, v in chunk_d.items():
        if k not in seen:
            d[k] = v
    seen |= chunk_seen

def main():
    argc = len(sys.argv)
    filename = sys.argv[1] if argc > 1 else 'data_test'
    workers = int(sys.argv[2]) if argc > 2 else os.cpu_count() - 1
    jobs_per_worker = int(sys.argv[3]) if argc > 3 else 1
    result = process(filename, workers, workers * jobs_per_worker)
    print(len(result))


if __name__ == '__main__':
    main()
