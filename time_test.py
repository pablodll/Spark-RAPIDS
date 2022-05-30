import time

def read_time(path, format_='csv'):
    start = time.time()
    df = spark.read.load(path, format=format_)
    end = time.time()
    print(end - start)
    return df

def show_time(df):
    start = time.time()
    df.show()
    end = time.time()
    print(end - start)

def count_time(df):
    start = time.time()
    df.count()
    end = time.time()
    print(end - start)

def select_show_time(df, col):
    start = time.time()
    df.select(col).show()
    end = time.time()
    print(end - start)

def distinct_show_time(df, col):
    start = time.time()
    df.select(col).distinct().show()
    end = time.time()
    print(end -start)
    
def select_count_time(df, col):
    start = time.time()
    df.select(col).count()
    end = time.time()
    print(end - start)

def distinct_count_time(df, col):
    start = time.time()
    df.select(col).distinct().count()
    end = time.time()
    print(end -start)

def filter_show_time(df, col, val):
    start = time.time()
    df.filter(df[col] > val).show()
    end = time.time()
    print(end - start)

def filter_count_time(df, col, val):
    start = time.time()
    df.filter(df[col] > val).count()
    end = time.time()
    print(end - start)


