import time

class time_test:
    
    def __init__(self, log_path, dataframe=None):
        self.log_paht = log_path
        self.df = dataframe
    
    def write_file(self, x):
        self.log_file = open(self.log_path, 'a')
        self.log_file.write(x)
        self.log_file.close()

    def read_time(path, format_='csv'):
        start = time.time()
        df = spark.read.load(path, format=format_)
        end = time.time()
        print(end - start)
        self.write_file("READ: " + str(end-start) + "\n")
        return df

    def show_time(df):
        start = time.time()
        df.show()
        end = time.time()
        self.write_file("SHOW: " + str(end-start) + "\n")
        print(end - start)

    def count_time(df):
        start = time.time()
        df.count()
        end = time.time()
        self.write_file("COUNT: " + str(end-start) + "\n")
        print(end - start)

    def select_show_time(df, col):
        start = time.time()
        df.select(col).show()
        end = time.time()
        self.write_file("SELECT SHOW: " + str(end-start) + "\n")
        print(end - start)

    def distinct_show_time(df, col):
        start = time.time()
        df.select(col).distinct().show()
        end = time.time()
        self.write_file("DISTINCT SHOW: " + str(end-start) + "\n")
        print(end -start)

    def select_count_time(df, col):
        start = time.time()
        df.select(col).count()
        end = time.time()
        self.write_file("SELECT COUNT: " + str(end-start) + "\n")
        print(end - start)

    def distinct_count_time(df, col):
        start = time.time()
        df.select(col).distinct().count()
        end = time.time()
        self.write_file("DISTINCT COUNT: " + str(end-start) + "\n")
        print(end -start)

    def filter_show_time(df, col, val):
        start = time.time()
        df.filter(df[col] > val).show()
        end = time.time()
        self.write_file("FILTER SHOW: " + str(end-start) + "\n")
        print(end - start)

    def filter_count_time(df, col, val):
        start = time.time()
        df.filter(df[col] > val).count()
        end = time.time()
        self.write_file("FILTER COUNT: " + str(end-start) + "\n")
        print(end - start)


