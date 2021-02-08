from sparkdq.checks.Check import Check
from sparkdq.conf.Context import Context
from sparkdq.detections.DetectionSuite import DetectionSuite
from sparkdq.structures.ConstrainableDataTypes import ConstrainableDataTypes
from sparkdq.utils.RegularExpressions import EMAIL
from sparkdq.utils.Assertions import greater_than_or_equal_to, greater_than, less_than, is_positive, \
    is_negative, equal_to, in_range


if __name__ == '__main__':

    Context().init_spark()
    spark = Context().spark

    rdd = spark.sparkContext.parallelize([
        (1, "M", "22", 19, 168, 72, "https://www.baidu.com", "12312d12@163.com"),
        (2, "W", "BE2", 22, 172, -100, "http://192.168.20.2", "222jj@nju.edu.cn"),
        (3, "M", "CD", 23, 166, 55, "ftp://192.168.2", "SWS@gmail.com"),
        (8, "M", "CD2", 24, 166, 55, "ftp://192.168.2", "SWS2@gmail.com"),
        (4, "W", '33', 26, 18, 70, "xxx:http://www.baidu.com", "12324@qq.com"),
        (5, 'W', 'E', 18, 180, 68, None, None),
        (5, 'M', 'EF2', 12, 185, 88, None, None)
    ])
    from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType

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
    #
    completeness_check = Check("Completeness") \
        .is_complete("name") \
        .is_complete("homepage")\
        .has_completeness("name", greater_than(0.8))\
        .has_completeness("homepage", greater_than_or_equal_to(0.8))\
        .satisfies_fd("name@homepage#_&_")

    detection_result = DetectionSuite() \
        .on_data(df) \
        .add_check(completeness_check) \
        .run()

    detection_result.success_metrics_ad_df().show()
    #
    # uniqueness_check = Check("Uniqueness")\
    #     .is_unique("name")\
    #     .is_unique(["weight", "height"])\
    #     .has_uniqueness("name", less_than(0.8))\
    #     .has_uniqueness(["weight", "height"], less_than(0.8))\
    #     .has_distinctness("name", greater_than(0.8))\
    #     .has_distinctness(["weight", "height"], less_than(0.8))\
    #     .has_count_distinct("weight", greater_than_or_equal_to(6))\
    #     .has_approx_count_distinct("weight", greater_than_or_equal_to(6))\
    #
    # consistency_check = Check("Consistency")\
    #     .satisfies_expression("height > 0", "height is positive")\
    #     .satisfies_expression("(weight > 0) and (height > 0)", "most weights are positive",
    #                           greater_than_or_equal_to(0.8))\
    #     .is_positive("weight")\
    #     .is_non_negative("height")\
    #     .is_greater_than("height", 18)\
    #     .is_greater_than_or_equal_to("height", 18)\
    #     .is_less_than("weight", 88)\
    #     .is_less_than_or_equal_to("weight", 88)\
    #     .is_in_range("weight", -100, 88, True, True)\
    #     .is_in_range("weight", -100, 88, False, False)\
    #     .is_contained_in("gender", ['M', "W"])\
    #     .is_contained_in("gender", ['M', 'K'])\
    #     .has_data_type("height", ConstrainableDataTypes.Integral)\
    #     .has_data_type("name", ConstrainableDataTypes.Integral, is_positive())\
    #     .matches_pattern("email", EMAIL)\
    #     .matches_pattern("email", EMAIL, where="email is not null")\
    #     .begins_with("homepage", "ftp", is_positive())\
    #     .ends_with("email", ".com", greater_than(0.5))\
    #     .contains_str("email", "@")\
    #     .contains_url("homepage")\
    #     .contains_email("email")\
    #
    # validity_check = Check("Validity")\
    #     .has_size(equal_to(7))\
    #     .has_max("weight", less_than(100))\
    #     .has_min("height", greater_than(0))\
    #     .has_mean("height", greater_than(150))\
    #     .has_sum("weight", greater_than(350))\
    #     .has_standard_deviation("weight", greater_than(10))\
    #     .has_entropy("weight", greater_than(-400))\
    #     .has_approx_quantile("weight", 0.5, greater_than(50))\
    #     .has_mutual_information("weight", "height", is_positive())\
    #     .has_correlation("weight", "height", is_negative())

    # check = Check("test") \
    #     .is_complete("workclass") \
    #     .is_contained_in("sex", ["Male", "Female"]) \
    #     .is_greater_than("hours-per-week", 20, greater_than(0.8)) \
    #     .is_non_negative("capital-gain") \
    #     .has_data_type("age", "integral") \
    #     .has_approx_quantile("age", 0.5, in_range(40, 50)) \
    #     .has_mean("capital-loss", less_than(100)) \
    #     .satisfies_fd("")


    # detection_result = DetectionSuite() \
    #     .on_data(df) \
    #     .add_check(completeness_check) \
    #     .add_check(uniqueness_check) \
    #     .add_check(consistency_check) \
    #     .add_check(validity_check) \
    #     .run()

    # print()
    # print("DetectionStatus: {}".format(detection_result.status))
    # print()
    # for check, check_result in detection_result.check_results.items():
    #     print("[Check]", check.description, check_result.status)
    #     for constraint_result in check_result.constraint_results:
    #         print(constraint_result.constraint)
    #         print(constraint_result.status)
    #         print(constraint_result.message)
    #         print(constraint_result.metric)
    #         print()
    #     print()
    #
    # # 检查统计信息是否正确
    # from pyspark.sql.functions import corr, mean, sum, stddev_pop
    # print(df.approxQuantile("weight", [0.5], 0.001))
    # df.agg(mean("height"), sum("weight"), stddev_pop("weight"), corr("weight", "height")).show()
