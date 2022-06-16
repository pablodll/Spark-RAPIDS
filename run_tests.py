from time_test import *

def main(mode):
    dfs = ['DF_3', 'DF_7', 'DF_8', 'DF_9']
    t = time_test(mode + "_log.txt")
    
    for d in dfs:
        print(d)
        df = t.READ(d)
        df = t.READ(d)

        t.COUNT()
        t.COUNT()

        t.SELECT_COUNT("_c3")
        t.SELECT_COUNT("_c3")

        t.DISTINCT_COUNT()
        t.DISTINCT_COUNT()

        t.FILTER_COUNT("_c1", 110)
        t.FILTER_COUNT("_c1", 110)

    print("DONE")

