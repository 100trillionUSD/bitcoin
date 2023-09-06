
# quick correctnes check

import multiprocessing as mp
import os, sys

import datareadtest_orig
import datareadtest_rev
import datareadtest_par
import datareadtest_par_rev
import datareadtest_par_rev2

def main():
    argc = len(sys.argv)
    filename = sys.argv[1] if argc > 1 else 'data_test'
    workers = int(sys.argv[2]) if argc > 2 else os.cpu_count() - 1
    jobs_per_worker = int(sys.argv[3]) if argc > 3 else 5

    r0 = datareadtest_orig.process(filename)

    r1 = datareadtest_rev.process(filename)
    assert r1 == r0

    r2 = datareadtest_par.process(filename, workers, workers * jobs_per_worker)
    assert r2 == r0

    r3 = datareadtest_par_rev.process(filename, workers, workers * jobs_per_worker)
    assert r3 == r0

    r4 = datareadtest_par_rev2.process(filename, workers, workers * jobs_per_worker)
    assert r4 == r0

if __name__ == '__main__':
    main()
