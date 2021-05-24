
# This splits the input into a stream of chunks and processes them in a pool
# of worker processes. The intermediary results are extended by a set
# of deleted keys in d to facilitate merging (map-reduce).
#
# This appears to be faster in some cases. The communication effort is
# high negating much of the expected benefit, but this is very sensitive
# to the size and locality of the data.
#
# Note that processing starts as soon as the first chunk is read,
# and also merging of partial results is interleaved to some extent.
# This can be tuned by parameters in main().

import os, sys
from multiprocessing import Pool

def process(filename, workers, chunks):
    d = {}
    with open(filename, 'r') as fd:
        chunksize = os.stat(fd.fileno()).st_size // chunks + 1
        with Pool(processes=workers) as pool:
            chunks = (fd.readlines(chunksize) for _ in range(chunks))
            for result in pool.imap(process_chunk, chunks):
                print('### chunk result', len(result[0]), len(result[1]))
                merge_result(d, *result)
                print('### total', len(d))
    return d

def process_chunk(lines):
    d = {}
    deleted = set()
    for l in lines:
        v = l.split(' ')
        b = int(v[0])
        i = int(v[2])
        o = int(v[3])
        for x in range(4+i, 4+i+o):
            k = v[x+o]
            d[k] = (b, float(v[x]))
            deleted.discard(k)
        for x in range(4, 4+i):
            k = v[x]
            # if setting d[] 4 lines above never overwrites an existing value
            # (as in the example file) this is correct and efficient:
            if not d.pop(k, False):
                deleted.add(k)
            # else you NEED this for correctnes, leading to bigger s & more overhead:
            # d.pop(k, None)
            # deleted.add(k)
    return d, deleted

def merge_result(d, chunk_d, chunk_deleted):
    for k in chunk_deleted:
        # same choice as in process_chunk:
        del d[k]
        # d.pop(k, None)
    d.update(chunk_d)

def main():
    argc = len(sys.argv)
    filename = sys.argv[1] if argc > 1 else 'data_test'
    workers = int(sys.argv[2]) if argc > 2 else os.cpu_count() - 1
    jobs_per_worker = int(sys.argv[3]) if argc > 3 else 5
    result = process(filename, workers, workers * jobs_per_worker)
    print(len(result))


if __name__ == '__main__':
    main()
