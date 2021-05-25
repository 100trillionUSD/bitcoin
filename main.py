import asyncio
from timeit import Timer
import time
import csv

from dataread9test import dataread9test
from dataread9test_improved import dataread9test_improved
from dataread9test_improved2 import dataread9test_improved2

print(time.time())
# Count max columns, was gonna add this as method argument, but doesn't work with timeit
columns = 0
with open('data_test_small_processed', 'r+') as f:
    csv_f = csv.reader(f, delimiter=' ')
    max = 0
    for row in csv_f:
        max = len(row) if len(row) > max else max
    columns = max
    # f.seek(0, 0)
    # s = f.read()
    # f.seek(0, 0)
    # f.write(" ".join(map(str, list(range(columns)))) + "\n" + s)
print(time.time())
print(columns)


async def helper():
    # Good timer reference https://stackoverflow.com/a/24105845/4600952
    # print(len(dataread9test()), min(Timer(dataread9test).repeat(repeat=20, number=5)))
    # print(len(dataread9test_improved()), min(Timer(dataread9test_improved).repeat(repeat=20, number=5)))
    print(len(await dataread9test_improved2()), min(Timer(await dataread9test_improved2).repeat(repeat=20, number=5)))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(helper())
    loop.close()

