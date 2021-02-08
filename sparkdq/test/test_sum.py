from pyspark.sql.session import SparkSession
from pyspark.sql.functions import sum as _sum, col, expr, regexp_extract, when, lit


if __name__ == "__main__":
    spark = SparkSession.builder.master('local').getOrCreate()
    # df = spark.read.csv("file:///Users/qiyang/project/SparkDQ/qualitydetector/data/breastw.csv")

    rdd = spark.sparkContext.parallelize([
        (1, "22", 19, 168, 72, "https://www.baidu.com", "12312d12@163.com"),
        (2, "BE2", 22, 172, -100, "http://192.168.20.2", "222jj@nju.edu.cn"),
        (3, "CD", 23, 166, 55, "ftp://192.168.2", "SWS@gmail.com"),
        (4, 'ED', 26, 18, 70, "www.", "12324@qq.com"),
        (5, 'E', 18, 180, 68, None, None),
        (5, 'EF2', 12, 185, 88, None, None)
    ])
    from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType

    schema = StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("age", LongType(), True),
        StructField("height", IntegerType(), True),
        StructField("weight", IntegerType(), True),
        StructField("homepage", StringType(), True),
        StructField("email", StringType(), True)
    ])
    df = spark.createDataFrame(rdd, schema)
    print(type(df.schema.names))
    # test not null
    # condition = _sum(col("_c6").isNotNull().cast("integer")).alias("_c6_sum")
    # df.agg(condition).show()

    # test expr, contained in
    # allowed_values_str = ",".join(["'{}'".format(value) for value in allowed_values])
    # condition = _sum(expr("age not in ({})".format(allowed_values_str)).cast("integer")).alias("_c6")

    # test compare
    # condition = _sum(expr("COALESCE({}, 0.0) < 0".format("age")).cast("integer"))

    # test regex
    # regex = regexp_extract(col('name'), '(E)(.*)', 0)
    # df.select(regex).show()

    # test url
    # url = "(http(s?)|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]"

    # test email
    # email = """^([A-Za-z0-9_\-\.])+\@([A-Za-z0-9_\-\.])+\.([A-Za-z]{2,4})$"""
    # expression = when(regexp_extract(col('email'), email, 0) != lit(""), 1).otherwise(0)

    # test distinctness
    # expression = col("name")
    # condition = _sum(expression.cast("integer"))
    # df.agg(condition).show()






