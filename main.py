from timeit import Timer
import csv

from dataread9test import dataread9test
from pandas1 import pandas1
from pandas2 import pandas2
from pandas3 import pandas3
from pandas4 import pandas4
from pandas5 import pandas5


# Count max columns, was gonna add this as method argument, but doesn't work with timeit
columns = 0
with open('data_test', 'r') as f:
    csv_f = csv.reader(f, delimiter=' ')
    max = 0
    for row in csv_f:
        max = len(row) if len(row) > max else max
    columns = max
print(columns)

if __name__ == '__main__':
    # Good timer reference https://stackoverflow.com/a/24105845/4600952
    print(len(dataread9test()), min(Timer(dataread9test).repeat(repeat=3, number=1)))
    # print(len(pandas1()      ))#, min(Timer(pandas1      ).repeat(repeat=3, number=3)))
    # print(len(pandas2()      ))#, min(Timer(pandas2      ).repeat(repeat=3, number=3)))
    # print(len(pandas3()      ))#, min(Timer(pandas3      ).repeat(repeat=3, number=3)))
    # print(len(pandas4()      ))#, min(Timer(pandas4      ).repeat(repeat=3, number=3)))
    print(len(pandas5()      ))#, min(Timer(pandas5      ).repeat(repeat=1, number=1)))
