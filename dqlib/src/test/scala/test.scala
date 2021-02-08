import org.apache.spark.sql.functions.{col, max}
import org.apache.spark.sql.{Row, SparkSession, StatefulMode}
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.PCA


object test {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("DataClean")
      .getOrCreate()
    val sc = spark.sparkContext

    val schema = StructType(List(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("height", IntegerType, nullable = true),
      StructField("weight", IntegerType, nullable = true),
      StructField("homepage", StringType, nullable = true),
      StructField("email", StringType, nullable = true)
    ))

    val rdd = sc.parallelize(Seq(
      Row(1L, "A", 19, 168, 72, "https://www.baidu.com", "12312d12@163.com"),
      Row(2L, "A", 22, 168, -100, "http://192.168.20.2", "222jj@nju.edu.cn"),
      Row(3L, "A", 22, 166, 55, "ftp://192.168.2", "SWS@gmail.com"),
      Row(8L, "B", 24, 169, 55, "ftp://192.168.2", "SWS2@gmail.com"),
      Row(4L, "B", 24, 180, 70, "www.", "12324@qq.com"),
      Row(5L, "B", 18, 180, 68, null, null),
      Row(5L, "B", 12, 185, 88, null, null)))
    val df = spark.createDataFrame(rdd, schema=schema)

  }

}

