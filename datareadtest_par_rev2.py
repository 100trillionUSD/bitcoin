
# An alternate take on the reversal theme, this processes a stream of chunks
# in the order read, but processing each chunk in reverse internally.
#
# Results are inconclusive. The fast start advantage is offset by more complex
# merging.
# It seems to work best with jobs_per_worker == 1 (memory permitting).

import os, sys
from multiprocessing import Pool

def process(filename, workers, chunks):
    d = {}
    deleted = set()
    with open(filename, 'r') as fd:
        chunksize = os.stat(fd.fileno()).st_size // chunks + 1
        chunks = (fd.readlines(chunksize) for _ in range(chunks))
        with Pool(processes=workers) as pool:
            for result in pool.imap(process_chunk, chunks):
                print('### chunk result', len(result[0]), len(result[1]))
                merge_result(d, deleted, *result)
                print('### total', len(d))
    return d

def process_chunk(lines):
    d = {}
    deleted = set()
    for l in reversed(lines):
        v = l.split(' ')
        b = int(v[0])
        i = int(v[2])
        o = int(v[3])
        for x in range(4, 4+i):
            k = v[x]
            deleted.add(k)
        for x in range(4+i, 4+i+o):
            k = v[x+o]
            if k not in deleted:
                d[k] = (b, float(v[x]))
    return d, deleted

def merge_result(d, deleted, chunk_d, chunk_deleted):
    for k in chunk_deleted:
        d.pop(k, None)
    d.update(chunk_d)
    deleted.update(chunk_deleted)
    deleted.intersection_update(chunk_d.keys())

def main():
    argc = len(sys.argv)
    filename = sys.argv[1] if argc > 1 else 'data_test'
    workers = int(sys.argv[2]) if argc > 2 else os.cpu_count() - 1
    jobs_per_worker = int(sys.argv[3]) if argc > 3 else 1
    result = process(filename, workers, workers * jobs_per_worker)
    print(len(result))


if __name__ == '__main__':
    main()
