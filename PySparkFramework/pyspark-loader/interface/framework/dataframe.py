class LocalDataFrame:
    DISK_ONLY = None

    def __init__(self, df):
        self._df = df

    @property
    def df(self):
        return self._df

    @property
    def columns(self):
        return self.df.columns

    @property
    def dtypes(self):
        return self.df.dtypes

    @property
    def schema(self):
        return self.df.schema

    def alias(self, alias):
        return LocalDataFrame(self.df.alias(alias))

    def filter(self, condition):
        return LocalDataFrame(self.df.filter(condition))

    def column_alias(self, name, alias):
        return LocalDataFrame(self.df.withColumnRenamed(name, alias))

    def select(self, cols):
        return LocalDataFrame(self.df.select(cols))

    def drop(self, columns):
        return LocalDataFrame(self.df.drop(columns))

    def fill_na(self, value, column):
        return LocalDataFrame(self.df.na.fill(value, column))

    def cast(self, col, dtype):
        df = self.df.withColumn(col, self.df[col].cast(dtype))
        return LocalDataFrame(df)

    def add_column(self, col, value):
        df = self.df.withColumn(col, LocalDataFrame.lit(value))
        return LocalDataFrame(df)

    def with_column(self, col, value):
        df = self.df.withColumn(col, value())
        return LocalDataFrame(df)

    def join(self, df, condition, how):
        df = self.df.join(df.df, LocalDataFrame.expr(condition), how=how)
        return LocalDataFrame(df)

    def repartition_and_persist(self, size, level):
        if level == "DISK_ONLY":
            self._df = self.df.repartition(size).persist(
                LocalDataFrame.DISK_ONLY)
        self.df.count()
        return LocalDataFrame(self.df)

    def count(self):
        return self.df.count()

    def groupBy(self, columns):
        return LocalDataFrame(self.df.groupBy(columns).count())

    def orderBy(self, columns, ascending):
        return LocalDataFrame(self.df.orderBy(columns, ascending=ascending))

    def collect(self):
        return self.df.rdd.collect()

    def unpersist(self):
        self.df.unpersist()

    def write_data(self, path, files):
        self.df.coalesce(files).write.parquet(path=path, mode='overwrite')

    def createOrReplaceTempView(self, name):
        self.df.createOrReplaceTempView(name)

    def first(self):
        self.df.first()

    def union(self, df):
        return LocalDataFrame(self.df.union(df.df))

    @staticmethod
    def asDict(row):
        return row.asDict()

    @staticmethod
    def expr(condition):
        pass

    @staticmethod
    def lit(value):
        pass

    def repartition_write_parquet(self, size, path, mode):
        df = self.df.repartition(size).write.parquet(path=path, mode=mode)
        return LocalDataFrame(df)

    def coalesce_write_format(self, size, form, option, mode, opt,
                              hdfs_fileloc):
        df = self.df.coalesce(size).write.format(form).option(
            option, True).mode(mode).option(opt, ',').save(hdfs_fileloc)
        return LocalDataFrame(df)

    def head(self, num):
        return self.df.head(num)

    def toDF(self, cols):
        return LocalDataFrame(self.df.toDF(*cols))
