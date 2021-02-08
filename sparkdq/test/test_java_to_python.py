from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.feature import VectorAssembler
from sparkdq.models.IForest import *


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    # spark = SparkSession.builder \
    #     .master('local') \
    #     .config("spark.jars", "/Users/qiyang/project/dqlib/target/dqlib-1.0-jar-with-dependencies.jar") \
    #     .getOrCreate()

    spark = SparkSession.builder\
        .master('local')\
        .getOrCreate()
    sc = spark.sparkContext

    rdd = spark.sparkContext.parallelize([
        (1, "A", 19, 168, 72),
        (2, "B", 22, 172, -100),
        (3, "C", 23, 166, 55),
        (4, 'D', 26, 18, 70),
        (5, 'E', 18, 180, 68)
    ])
    from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType

    schema = StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("age", LongType(), True),
        StructField("height", IntegerType(), True),
        StructField("weight", IntegerType(), True)
    ])
    df = spark.createDataFrame(rdd, schema)

    row_key = "id"
    data_columns = ["name", "age", "height", "weight"]

    x = df.rdd.flatMap(
        lambda row: [(row[row_key], [row[row_key], c, row[c]]) for c in data_columns]
    ).collect()
    print(x)

    # iForest = IForest(columns=["height", "weight"])
    # ifm = iForest.fit(df)
    # print(ifm.summary.success)

    # from sparkdq.models.LOF import *
    # lof = LOF(columns=["height", "weight"])
    # lm = lof.fit(df)
    # print(lm.summary.dataWithScores.success)

    # from sparkdq.models.CFDSolver import CFDSolver
    # cfds = CFDSolver(cfds=["name@age#_&_"], taskType="repair")
    # cfm = cfds.fit(df)
    # print(cfm.summary.success)

    # from sparkdq.models.entity.EntityResolution import EntityResolution
    # es = EntityResolution(columns=["name", "age"], taskType="repair", indexCol="id")
    # esm = es.fit(df)
    # print(esm.summary.numOfEntities)
    # print(esm.summary.message)
    # esm.summary.targetData.show()

    # from sparkdq.models.BayesianImputor import *
    # bi = BayesianImputor(targetCol="name", dependentColumns=["age", "height"]).setTargetCol("name")
    # dm = bi.fit(df)
    # print(dm.summary.success)
