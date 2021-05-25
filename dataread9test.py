def dataread9test():
    d = {}  # d will be ~60GB in ram
    with open('data_test_small', 'r') as f:  # 200GB csv file
        for l in f:  # ~700,000 lines
            v = l.split(" ")  # v contains thousands of items
            b = int(v[0])
            i = int(v[2])
            o = int(v[3])
            #  [d.update({v[x+o]:(b,float(v[x]))}) for x in range(4+i, 4+i+o)]
            for x in range(4 + i, 4 + i + o):
                d[v[x + o]] = (b, float(v[x]))
            for y in range(4, 4 + i):
                del d[v[y]]

    return d
