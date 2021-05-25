def dataread9test_improved():
    d = {}  # d will be ~60GB in ram
    with open('data_test', 'r') as f:  # 200GB csv file
        b = 0
        for l in f:  # ~700,000 lines
            v = l.split(" ")  # v contains thousands of items
            i = int(v[2]) + 3
            o = int(v[3])
            # d.update({v[x + o]: (b, float(v[x])) for x in range(4 + i, 4 + i + o)})
            for x in range(i + o, 3, -1):
                if x > i:
                    d[v[x + o]] = (b, float(v[x]))
                else:
                    del d[v[x]]
            b += 1

    return d
