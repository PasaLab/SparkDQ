import org.apache.spark.sql.{Row, SparkSession}

import pasa.bigdata.dqlib.models.entities.EntityResolution

object testCFD {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("DataClean")
      .getOrCreate()

    import org.apache.spark.sql.types._
    val schema = StructType(List(
      StructField("id", LongType, nullable = false),
      StructField("CC", StringType, nullable = true),
      StructField("AC", IntegerType, nullable = true),
      StructField("PN", IntegerType, nullable = true),
      StructField("NM", StringType, nullable = true),
      StructField("STR", StringType, nullable = true),
      StructField("CT", StringType, nullable = true),
      StructField("ZIP", StringType, nullable = true)
    ))
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Seq(
      Row(109L, "01", 908, 1111111, "David", "Tree Ave.", "MH", "03333"),
      Row(150L, "01", 908, 6666666, "JoJo", "Tree Ave.", "MH", "07974"),
      Row(190L, "01", 212, 2222222, "Joe", "Elm St.", "GLA", "01201"),
      Row(228L, "01", 212, 2222222, "Jim", "Elm Str.", "NYC", "01299"),
      Row(24L, "01", 908, 1111111, "Mike", "Tree Ave.", "MH", "07974"),
      Row(267L, "01", 212, 2222222, "Eline", "Elm Str.", "NYC", "01201"),
      Row(308L, "01", 215, 3333333, "Ben", "Oak Ave.", "P", "02394"),
      Row(345L, "01", 215, 4444444, "Jane", "Mel St.", "PHI", "06873"),
      Row(384L, "44", 131, 4444444, "Ian", "High Str.", "EDI", "EH4IDT"),
      Row(425L, "44", 131, 5555555, "Anna", "High St.", "EDI", "EH4IDT"),
      Row(466L, "44", 141, 5555555, "Caral", "High Str.", "EDI", "EH4IDT"),
      Row(64L, "01", 908, 1111111, null, "Tree Ave Str.", "MH", "07974")
    ))

    /**
      * 测试1：
      * 检测null转字符串的取值，发现null对应空值，无法调用函数toString
      * schema和定义一定得完全一样，否则collect的时候会报错，特别是Long类型末尾要加L，String和Int不能弄混
      */
    val df = spark.createDataFrame(rdd, schema=schema)
    val es = new EntityResolution().setIndexCol("id").setColumns(Array("ZIP")).setTaskType("repair")
    val esm = es.fit(df)
    println(esm.summary.message)
    println(esm.summary.numOfEntities)
    esm.summary.targetData.show()

    /*
      CC,ZIP@STR#44,_&_#0,6,4#
		  CC,AC,PN@STR,CT,ZIP#01,908,_&_,MH,_#01,212,_&_,NYC,_#0,1,2,4,5,6#
		  CC,AC@CT#01,215&PHI#44,141&GLA#0,1,5#
     */


    /**
      * 测试2：
      * 检测多个CFD的冲突
      * 1）检测变量冲突冲突对的计数方式是否正确
      * 2）检测多个CFD情况下是否全部统计正确
      * 3）检测包含纯FD的情况下是否全部统计正确
      */
//    val testArray = Array(4, 7, 3)
//    val sum = testArray.sum
//    val rightAcc = testArray.scanRight(0)(_ + _).tail
//    val count = testArray.zip(rightAcc).map {
//      case (v, acc) =>
//        v * acc
//    }.sum
//    println(count)

//    val df = spark.createDataFrame(rdd, schema = schema)
//    val cfds = Array("CC,ZIP@STR#44,_&_", "CC,AC,PN@STR,CT,ZIP#01,908,_&_,MH,_#01,212,_&_,NYC,_",
//    "CC,AC@CT#01,215&PHI#44,141&GLA") // pure CFDs
//    val cfds = Array("CC,ZIP@STR#44,_&_", "CT@ZIP#_&_") // CFDs with FDs
//    val cfds = Array("CT@ZIP#_&_", "CC,AC@CT,ZIP#_,_&_,_") // pure FDs
//    val colWeights = new Array[Double](8)
//    val detector = new CFDDetector()
//      .setCFDs(cfds)
//      .setIdxCol("ID")
//      .setColWeights(colWeights)
//      .setMaxRound(3)
//      .setTaskType("detect")
//    val model = detector.fit(df)
//    println("Success: " + model.success)
//    println("Message: " + model.message)
//    val violations = model.summary.violations
//    println("Size: " + violations.size)
//    violations.foreach { violationsPerCFD =>
//      println("CFD idx: " + violationsPerCFD._1)
//      violationsPerCFD._2.foreach { violationsPerPat =>
//        println(violationsPerPat.map(x => "(" + x._1 + ", " + x._2 + ")").mkString("\t"))
//      }
//    }

    /**
      * 测试3：
      * 修复多个CFD的冲突
      * 1）当前无优先级、无属性权重的修复是否有效
      * 2）添加CFD的优先级，辅助RHS的修复和LHS的修复
      * 3) 如果没有CFD的优先级和属性的权重怎么办？
      *   1。CFD的优先级可以根据检测结果，如何结合常量、变量冲突这两个值
      *   2。如何选择要置空的LHS属性，尽量不影响其他的CFD
      */
//    val df = spark.createDataFrame(rdd, schema = schema)
//    val cfds = Array("CC,ZIP@STR#44,_&_#1", "CC,AC,PN@STR,CT,ZIP#01,908,_&_,MH,_#01,212,_&_,NYC,_#1,1",
//    "CC,AC@CT#01,215&PHI#44,141&GLA#1,1") // pure CFDs
//    val cfds = Array("CC,ZIP@STR#44,_&_", "CT@ZIP#_&_") // CFDs with FDs
//    val cfds = Array("CT@ZIP#_&_", "CC,AC@CT,ZIP#_,_&_,_") // pure FDs
//    val colWeights = new Array[Double](8)
//    val detector = new CFDDetector()
//      .setCFDs(cfds)
//      .setIdxCol("ID")
//      .setColWeights(colWeights)
//      .setMaxRound(3)
//      .setTaskType("repair")
//    val model = detector.fit(df)
//    println("Success: " + model.success)
//    println("Message: " + model.message)
//    val result = model.summary.repairedData.sort("ID")
//    result.show()
  }

}
