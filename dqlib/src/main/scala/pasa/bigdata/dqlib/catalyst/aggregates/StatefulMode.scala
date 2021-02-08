package org.apache.spark.sql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import scalaz.Scalaz._

object StatefulMode extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(StructField("value", StringType) :: Nil)

  override def bufferSchema: StructType = StructType(
    StructField("frequencyMap", DataTypes.createMapType(StringType, LongType)) :: Nil
  )

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String, Long]()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Map[String, Long]](0) |+| Map(input.getAs[String](0) -> 1L)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Map[String, Long]](0) |+| buffer2.getAs[Map[String, Long]](0)
  }

  override def evaluate(buffer: Row): String = {
    buffer.getAs[Map[String, Long]](0).maxBy(_._2)._1
  }

}
