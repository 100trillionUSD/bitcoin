#!/bin/sh

if [ ! -f data_test_big ] ; then
  cp data_test data_test_big
  cat data_test_big data_test_big data_test_big data_test_big > t && mv t data_test_big
  cat data_test_big data_test_big data_test_big data_test_big > t && mv t data_test_big
  cat data_test_big data_test_big data_test_big data_test_big > t && mv t data_test_big
fi

for v in orig rev par par_rev par_rev2 ; do
  echo ''
  echo datareadtest_$v.py
  time python3 datareadtest_$v.py data_test_big >/dev/null
  sleep 1
done
