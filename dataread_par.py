
import os, sys
from multiprocessing import Pool

def process(fd, workers, chunks):
    d = {}
    chunksize = os.stat(fd.fileno()).st_size // chunks + 1
    with Pool(processes=workers) as pool:
        results = (fd.readlines(chunksize) for _ in range(chunks))
        for result in pool.imap(process_chunk, results):
            print('### chunk result', len(result[0]), len(result[1]))
            merge_result(d, *result)
            print('### total', len(d))
    return d

def process_chunk(lines):
    d = {}
    s = set()
    for l in lines:
        v = l.split(' ')
        b = int(v[0])
        i = int(v[2])
        o = int(v[3])
        for x in range(4+i, 4+i+o):
            loc = v[x+o]
            d[loc] = (b, float(v[x]))
            s.discard(loc)
        for x in range(4, 4+i):
            loc = v[x]
            # if setting d[] 4 lines above never overwrites an existing value
            # (as in the example file) this is correct and efficient:
            if not d.pop(loc, False):
                s.add(loc)
            # else you NEED this for correctnes, leading to bigger s & more overhead:
            # d.pop(loc, None)
            # s.add(loc)
    return d, s

def merge_result(d, chunk_d, chunk_s):
    for loc in chunk_s:
        # same choice as in process_chunk:
        del d[loc]
        # d.pop(loc, None)
    d.update(chunk_d)

def main():
    workers = os.cpu_count() * 3 // 4
    jobs_per_worker = 3
    filename = sys.argv[1] if len(sys.argv) > 1 else 'data_test'
    with open(filename, 'r') as fd:
        result = process(fd, workers, workers * jobs_per_worker)
    print(len(result))


if __name__ == '__main__':
    main()
