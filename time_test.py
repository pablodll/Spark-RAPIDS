import time
import inspect
import subprocess
from os import listdir
import re

class time_test:
    
    def __init__(self, log_path=None, dataframe=None):
        self.log_path = log_path
        self.df = dataframe
    
    def write_file(self, x):
        if self.log_path is not None:
            self.log_file = open(self.log_path, 'a')
            self.log_file.write("[{path} ({size})] -> {fun}: {time} \n".format(path=self.df_path, size=self.df_size, fun=inspect.stack()[1].function, time=x))
            self.log_file.close()

    def READ(self, path, format_='csv'):
        start = time.time()
        self.df = spark.read.load(path, format=format_)
        end = time.time()
        total = str(end - start)
        print(total)
        
        self.df_size = subprocess.check_output(['du','-sh', path]).split()[0].decode('utf-8')
        self.df_path = path
        
        self.write_file(total)
        return self.df

    def SHOW(self):
        start = time.time()
        self.df.show()
        end = time.time()
        total = str(end - start)
        print(total)
        self.write_file(total)

    def COUNT(self):
        start = time.time()
        self.df.count()
        end = time.time()
        total = str(end - start)
        print(total)
        self.write_file(total)

    def SELECT_SHOW(self, col):
        start = time.time()
        self.df.select(col).show()
        end = time.time()
        total = str(end - start)
        print(total)
        self.write_file(total)

    def DISTINCT_SHOW(self):
        start = time.time()
        self.df.distinct().show()
        end = time.time()
        total = str(end - start)
        print(total)
        self.write_file(total)

    def SELECT_COUNT(self, col):
        start = time.time()
        self.df.select(col).count()
        end = time.time()
        total = str(end - start)
        print(total)
        self.write_file(total)

    def DISTINCT_COUNT(self):
        start = time.time()
        self.df.distinct().count()
        end = time.time()
        total = str(end - start)
        print(total)
        self.write_file(total)

    def FILTER_SHOW(self, col, val):
        start = time.time()
        self.df.filter(df[col] > val).show()
        end = time.time()
        total = str(end - start)
        print(total)
        self.write_file(total)

    def FILTER_COUNT(self, col, val):
        start = time.time()
        self.df.filter(df[col] > val).count()
        end = time.time()
        total = str(end - start)
        print(total)
        self.write_file(total)
        
    def run_tests(self, dfs=None):
        if dfs is None:
            dfs = [d for d in listdit('./') if re.search('DF_*')]

        for d in dfs:
            print(d)
            df = self.READ(d)
            df = self.READ(d)

            self.COUNT()
            self.COUNT()

            self.SELECT_COUNT("_c3")
            self.SELECT_COUNT("_c3")

            self.DISTINCT_COUNT()
            self.DISTINCT_COUNT()

            self.FILTER_COUNT("_c1", 110)
            self.FILTER_COUNT("_c1", 110)

        print("DONE")


