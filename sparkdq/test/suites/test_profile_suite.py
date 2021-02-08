from sparkdq.conf.Context import Context
from sparkdq.analytics.catalyst.DQFunctions import stateful_stddev

from pyspark.sql.functions import max as _max, when, col, monotonically_increasing_id

import time


class A:

    def __init__(self, a, b):
        self.a = a
        self.b = b


if __name__ == "__main__":
    Context().init_spark()
    spark = Context().spark
    # rdd = spark.sparkContext.parallelize([
    #     (1, "Jack", "true", 19, 168, 72, "https://www.baidu.com", "12312d12@163.com"),
    #     (2, "Tom", "false", 22, 172, -100, "http://192.168.20.2", "222jj@nju.edu.cn"),
    #     (3, "1103", "true", 23, 166, 55, "ftp://192.168.2", "SWS@gmail.com"),
    #     (4, "1108", "false", 24, 169, 55, "ftp://192.168.2", "SWS2@gmail.com"),
    #     (5, '4.5', None, 26, 180, 68, "www.", "12324@qq.com"),
    #     (6, 'null', "false", 18, 180, 68, None, None),
    #     (7, None, "true", 12, 20, 88, None, None)
    # ])
    # from pyspark.sql.types import *
    #
    # schema = StructType([
    #     StructField("id", LongType(), False),
    #     StructField("name", StringType(), True),
    #     StructField("is_new", StringType(), True),
    #     StructField("age", LongType(), True),
    #     StructField("height", IntegerType(), True),
    #     StructField("weight", IntegerType(), True),
    #     StructField("homepage", StringType(), True),
    #     StructField("email", StringType(), True)
    # ])

    # df = spark.createDataFrame(rdd, schema)

    from sparkdq.io.readers import from_csv
    test_file = "/Users/qiyang/Downloads/adult.csv"
    import pandas as pd
    import numpy as np
    data = pd.read_csv(test_file, header=0)
    data = data.replace(np.NaN, None)

    xx = data.values.tolist()



    df = spark.createDataFrame(data.values.tolist(), list(data.columns)).withColumn("id", monotonically_increasing_id())


    from sparkdq.analytics.analyzers.ApproxQuantile import ApproxQuantile

    apx1 = ApproxQuantile(column="Age", quantile_or_quantiles=0.25)
    m1 = apx1.compute_metric_from_data(df)
    print(m1.value)

    apx2 = ApproxQuantile(column="Age", quantile_or_quantiles=0.5)
    m2 = apx2.compute_metric_from_data(df)
    print(m2.value)

    apx3 = ApproxQuantile(column="Age", quantile_or_quantiles=0.75)
    m3 = apx3.compute_metric_from_data(df)
    print(m3.value)

    # from sparkdq.repairs.transformers.filters.RegexFilter import RegexFilter
    # rf = RegexFilter("GrossIncome", "(<=)")
    # df = rf.transform(df)
    # df.show()


    from sparkdq.analytics.analyzers.DataType import DataType
    from sparkdq.analytics.analyzers.Distinctness import Distinctness
    from sparkdq.analytics.analyzers.MutualInformation import MutualInformation
    from sparkdq.analytics.analyzers.Outliers import Outliers
    from sparkdq.outliers.OutlierSolver import OutlierSolver
    from sparkdq.outliers.params.IForestParams import IForestParams


    # dt = Outliers(column_or_columns="HoursPerWeek", model=OutlierSolver.IForest, model_params=IForestParams())
    # metric = dt.compute_metric_from_data(df)
    # print(metric)


    # from sparkdq.repairs.transformers.complex_repairers.FDRepairer import FDRepairer
    #
    # fd = FDRepairer(["Education@EducationNum#Bachelors&13#Doctorate&16",
    #                            "MaritalStatus,Sex@Relationship#Divorced,_&Not-in-family#Married-civ-spouse,Female&Wife#Married-civ-spouse,Male&Husband"])
    # df2 = fd.transform(df)
    # df2.show()

    # from sparkdq.repairs.transformers.complex_repairers.BayesianNullFiller import BayesianNullFiller
    #
    # bf = BayesianNullFiller(column="WorkClass", dependent_column_or_columns=["Occupation"])
    # df3 = bf.transform(df)
    # df3.show()

    # from sparkdq.repairs.transformers.complex_repairers.EntityExtractor import EntityExtractor
    #
    # ee = EntityExtractor(column_or_columns=["WorkClass", "Occupation"])
    # df4 = ee.transform(df)
    # df4.show()

    # from sparkdq.analytics.analyzers.Completeness import Completeness
    # from sparkdq.analytics.analyzers.Compliance import Compliance
    # from sparkdq.analytics.analyzers.ApproxCountDistinct import ApproxCountDistinct
    #
    # file_path = ""
    #
    # df = from_csv(file_path, header=True, infer_schema=True)
    # cn = Completeness("WorkClass")
    # m1 = cn.compute_metric_from_data(df)
    # cp = Compliance("HoursPerWeek>20")
    # m2 = cp.compute_metric_from_data(df)
    # acd = ApproxCountDistinct("Age")
    # m3 = acd.compute_metric_from_data(df)


    # from sparkdq.detections.DetectionSuite import DetectionSuite
    # from sparkdq.checks.Check import Check
    # from sparkdq.utils.Assertions import less_than
    #
    # columns = ["CapitalGain", "CapitalLoss"]
    # c = Check("test")\
    #     .have_distinctness(columns)\
    #     .is_primary_key(columns)\
    #     .has_mutual_information(columns[0], columns[1], less_than(0.1))
    # dr = DetectionSuite().on_data(df).add_check(c).run()

    #
    # c = Check("test")\
    #     .is_complete("WorkClass")\
    #     .is_greater_than("HoursPerWeek", 20)\
    #     .has_approx_count_distinct("Age", greater_than(60))
    # dr = DetectionSuite().on_data().add_check(c).run()


    # from sparkdq.repairs.transformers.filters.NullFilter import NullFilter
    # from sparkdq.repairs.transformers.filters.RegexFilter import RegexFilter
    # from sparkdq.repairs.transformers.filters.ExpFilter import ExpFilter
    #
    # nf = NullFilter("WorkClass")
    # df = nf.transform(df)
    # rf = RegexFilter("GrossIncome", "^(>=)$")
    # df = rf.transform(df)
    # ef = ExpFilter("Age > 20")
    # df = ef.transform(df)

    # from sparkdq.analytics.analyzers.Distinctness import Distinctness
    # from sparkdq.analytics.analyzers.Uniqueness import Uniqueness
    # from sparkdq.analytics.analyzers.MutualInformation import MutualInformation
    # file_path = ""
    #
    # df = from_csv(file_path, header=True, infer_schema=True)
    # columns = ["CapitalGain", "CapitalLoss"]
    # cn = Distinctness(columns)
    # m1 = cn.compute_metric_from_data(df)
    # cp = Uniqueness(columns)
    # m2 = cp.compute_metric_from_data(df)
    # acd = MutualInformation(columns[1], columns[2])
    # m3 = acd.compute_metric_from_data(df)

    from sparkdq.repairs.RepairSuite import RepairSuite
    from sparkdq.repairs.Clean import Clean

    clean = Clean("test")\
        .drop_null("WorkClass")\
        .filter_by_pattern("GrossIncome", "^(>=)$")\
        .filter_by_exp("Age > 20")
    rr = RepairSuite().on_data(df).add_clean(clean).run()



    # from sparkdq.profiling.ProfilerSuite import ProfilerSuite
    # ps = ProfilerSuite()\
    #     .on_data(data=df)\
    #     .run()
    #
    # print(ps)

    # print("NumRecords: {}".format(ps.num_records))
    # for k, v in ps.profiles.items():
    #     print("Column '{}'".format(k))
    #     print("\tCompleteness: {}".format(v.completeness))
    #     print("\tApproxCountDistinct: {}".format(v.approx_count_distinct))
    #     print("\tDataType: {}\n".format(v.data_type.name))

    # from sparkdq.suggestions.SuggestionSuite import SuggestionSuite, DEFAULT_RULES
    # ss = SuggestionSuite().on_data(data=df).add_constraint_rules(DEFAULT_RULES).run()
    # ss.suggestion_df().show()

    # from sparkdq.checks.Check import Check
    # from sparkdq.utils.Assertions import greater_than, in_range, less_than
    # from sparkdq.detections.DetectionSuite import DetectionSuite
    #
    # check = Check("test") \
    #     .is_contained_in("Sex", ["Male", "Female"]) \
    #     .is_greater_than("HoursPerWeek", 20, greater_than(0.8)) \
    #     .is_non_negative("CapitalGain") \
    #     .has_data_type("Age", "integral") \
    #     .has_approx_quantile("Age", 0.5, in_range(40, 50)) \
    #     .has_mean("CapitalLoss", less_than(100)) \
    #     .satisfies_fd("Education@EducationNum#_&_")
    #
    # detection_result = DetectionSuite() \
    #     .on_data(df) \
    #     .add_check(check) \
    #     .run()
    #
    # detection_result.success_metrics_ad_df().show(truncate=False)
    # detection_result.assertion_results_as_df().show(truncate=False)
    #
    # x = 3 if 2 > 1 else 2



    # xx = [col("homepage"), col("email")]
    # df.select(when(col("homepage").isNull(), "empty").when(col("homepage").startswith("ftp"), "error ftp").otherwise(col("homepage"))).show()
    # df.select(when(col("homepage").isNull(), "error web").otherwise()).show()
    # df.select(when(col("homepage").rlike("^ftp://"), "error ftp").otherwise(col("homepage")).alias("homepage")).show()
    # df.fillna("this is null", ["homepage", "email"])
    # df.withColumn("homepage", when(col("homepage").rlike("^ftp://"), "error ftp").when(col("homepage").rlike("^http"), "error http").otherwise(col("homepage"))).show()

    # from pyspark.sql.window import Window
    # from pyspark.sql.functions import rank, col, row_number
    # # # 去重
    # window = Window.partitionBy(['height', 'weight']).orderBy(col('id').desc())
    # df.select('*', row_number().over(window).alias('posicion')).where('posicion ==1').show()

    # values = ["Tom", "Jack"]
    # exp = "name in " + "('" + "','".join(values) + "')"

    # from pyspark.sql import Window
    # window = Window.orderBy().rowsBetween(-99, 0)
    # df.select("*", when(col("homepage").isNull(), last("homepage", True).over(window)).otherwise(col("homepage"))).show()
    # #  所有的select条件用when和else连接起来，并alias原来的列名，所有的列获取对应的select条件，生成替换后的新DF
    # df.filter(~expr("height>100")).show()

    # when之间如何写代码连接，when和regexp_replace之间如何连接
    # df.select(regexp_replace(regexp_replace("homepage", "(ftp)", "http"), "(http)", "https")).show()

    # df.filter(col("homepage").isNull() & (col("weight") > 50)).show()
    # df.select(when(col("homepage").isNull(), "error address").otherwise(regexp_replace("homepage", "(ftp)", "http")).alias("homepage")).show()

    # df.withColumn("newCol", abs(df["weight"] - 2)).show()
    # result = ProfilerSuite().on_data(df).run()
    # print(result)
