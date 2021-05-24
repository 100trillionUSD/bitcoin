from timeit import Timer
from dataread9test import dataread9test

# Good timer reference https://stackoverflow.com/a/24105845/4600952
print(len(dataread9test()), min(Timer(dataread9test).repeat(repeat=3, number=3)))
