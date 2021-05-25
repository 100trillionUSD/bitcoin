from timeit import Timer

from dataread9test import dataread9test
from dataread9test_improved import dataread9test_improved

def helper():
    # Good timer reference https://stackoverflow.com/a/24105845/4600952
    print(len(dataread9test()), min(Timer(dataread9test).repeat(repeat=3, number=3)))
    print(len(dataread9test_improved()), min(Timer(dataread9test_improved).repeat(repeat=3, number=3)))


if __name__ == '__main__':
    helper()
