import time
class time_test:
    
    def __init__(self, log_path, dataframe=None):
        self.log_path = log_path
        self.df = dataframe
    
    def write_file(self, x):
        self.log_file = open(self.log_path, 'a')
        self.log_file.write(inspect.stack()[1].function + x + '\n')
        self.log_file.close()

    def read_time(self, path, format_='csv'):
        start = time.time()
        self.df = spark.read.load(path, format=format_)
        end = time.time()
        total = str(end - start)
        print(total)
        self.write_file(total)
        return self.df

    def show_time(self):
        start = time.time()
        self.df.show()
        end = time.time()
        total = str(end - start)
        print(total)
        self.write_file(total)

    def count_time(self):
        start = time.time()
        self.df.count()
        end = time.time()
        total = str(end - start)
        print(total)
        self.write_file(total)

    def select_show_time(self, col):
        start = time.time()
        self.df.select(col).show()
        end = time.time()
        total = str(end - start)
        print(total)
        self.write_file(total)

    def distinct_show_time(self):
        start = time.time()
        self.df.distinct().show()
        end = time.time()
        total = str(end - start)
        print(total)
        self.write_file(total)

    def select_count_time(self, col):
        start = time.time()
        self.df.select(col).count()
        end = time.time()
        total = str(end - start)
        print(total)
        self.write_file(total)

    def distinct_count_time(self):
        start = time.time()
        self.df.distinct().count()
        end = time.time()
        total = str(end - start)
        print(total)
        self.write_file(total)

    def filter_show_time(self, col, val):
        start = time.time()
        self.df.filter(df[col] > val).show()
        end = time.time()
        total = str(end - start)
        print(total)
        self.write_file(total)

    def filter_count_time(self, col, val):
        start = time.time()
        self.df.filter(df[col] > val).count()
        end = time.time()
        total = str(end - start)
        print(total)
        self.write_file(total)


