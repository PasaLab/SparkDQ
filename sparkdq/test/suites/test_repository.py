# from sparkdq.analytics.analyzers.ApproxCountDistinct import ApproxCountDistinct
# from sparkdq.analytics.analyzers.ApproxQuantile import ApproxQuantile
# from sparkdq.analytics.analyzers.ApproxQuantiles import ApproxQuantiles
# from sparkdq.analytics.analyzers.Completeness import Completeness
# from sparkdq.analytics.analyzers.Compliance import Compliance
# from sparkdq.analytics.analyzers.Correlation import Correlation
# from sparkdq.analytics.analyzers.DataType import DataType
# from sparkdq.analytics.analyzers.Distinctness import Distinctness
# from sparkdq.analytics.analyzers.Entropy import Entropy
# from sparkdq.analytics.analyzers.Histogram import Histogram
# from sparkdq.analytics.analyzers.Maximum import Maximum
# from sparkdq.analytics.analyzers.Mean import Mean
# from sparkdq.analytics.analyzers.Minimum import Minimum
# from sparkdq.analytics.analyzers.MutualInformation import MutualInformation
# from sparkdq.analytics.analyzers.PatternMatch import PatternMatch
# from sparkdq.analytics.analyzers.Size import Size
# from sparkdq.analytics.analyzers.StandardDeviation import StandardDeviation
# from sparkdq.analytics.analyzers.Sum import Sum
# from sparkdq.analytics.analyzers.Uniqueness import Uniqueness
# from sparkdq.analytics.runners.AnalysisRunner import AnalysisRunner
from sparkdq.conf.Context import Context
# from sparkdq.repository.FileSystemMetricsRepository import FileSystemMetricsRepository
# from sparkdq.repository.MemoryMetricsRepository import MemoryMetricsRepository
# from sparkdq.repository.ResultKey import ResultKey
# from sparkdq.structures.FileSystem import FileSystem
# from sparkdq.utils.RegularExpressions import EMAIL


if __name__ == "__main__":
    spark = Context().spark
    rdd = spark.sparkContext.parallelize([
        (1, "M", "22", 19, 168, 72, "https://www.baidu.com", "12312d12@163.com"),
        (2, "W", "BE2", 22, 172, -100, "http://192.168.20.2", "222jj@nju.edu.cn"),
        (3, "M", "CD", 23, 166, 55, "ftp://192.168.2", "SWS@gmail.com"),
        (8, "M", "CD2", 24, 166, 55, "ftp://192.168.2", "SWS2@gmail.com"),
        (4, "W", '33', 26, 18, 70, "xxx:http://www.baidu.com", "12324@qq.com"),
        (5, 'W', 'E', 18, 180, 68, None, None),
        (5, 'M', 'EF2', 12, 185, 88, None, "。。。")
    ])
    from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType
    from pyspark.sql.functions import col, when, expr, sum as _sum, length, max as _max, min as _min, regexp_extract, \
        lit
    import time

    schema = StructType([
        StructField("id", LongType(), False),
        StructField("gender", StringType(), True),
        StructField("name", StringType(), True),
        StructField("age", LongType(), True),
        StructField("height", IntegerType(), True),
        StructField("weight", IntegerType(), True),
        StructField("homepage", StringType(), True),
        StructField("email", StringType(), True)
    ])
    df = spark.createDataFrame(rdd, schema)

    from sparkdq.models.IForest import IForest

    ff = IForest()
