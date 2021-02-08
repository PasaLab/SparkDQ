from sparkdq.conf.Context import Context
from sparkdq.suggestions.SuggestionSuite import SuggestionSuite, DEFAULT_RULES


if __name__ == "__main__":
    spark = Context().spark
    rdd = spark.sparkContext.parallelize([
        (1, "1101", 19, 168, 72, "https://www.baidu.com", "12312d12@163.com"),
        (2, "1102", 22, 172, -100, "http://192.168.20.2", "222jj@nju.edu.cn"),
        (3, "1103", 23, 166, 55, "ftp://192.168.2", "SWS@gmail.com"),
        (8, "1108", 24, 169, 55, "ftp://192.168.2", "SWS2@gmail.com"),
        (4, '4',    26, 18,  70, "www.", "12324@qq.com"),
        (5, '1105X', 18, 180, 68, None, None),
        (5, '1105X', 12, 185, 88, None, None)
    ])
    from pyspark.sql.types import *

    schema = StructType([
        StructField("id", LongType(), False),
        StructField("name", StringType(), True),
        StructField("age", LongType(), True),
        StructField("height", IntegerType(), True),
        StructField("weight", IntegerType(), True),
        StructField("homepage", StringType(), True),
        StructField("email", StringType(), True)
    ])
    df = spark.createDataFrame(rdd, schema)
    result = SuggestionSuite()\
        .on_data(df)\
        .add_constraint_rules(DEFAULT_RULES) \
        .set_test_set_ratio(0.5) \
        .set_random_seed(20) \
        .run()

    print(result.profiles)
    for suggestion in result.constraint_suggestions:
        print(suggestion, "\n")

    verification_result = result.verification_result
    print(verification_result)
