package org.apache.spark.sql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import pasa.bigdata.dqlib.catalyst.states.DataTypeHistogram

import scala.util.matching.Regex

object StatefulDataType extends UserDefinedAggregateFunction {

  val NULL_POS = 0
  val FRACTIONAL_POS = 1
  val INTEGRAL_POS = 2
  val BOOLEAN_POS = 3
  val STRING_POS = 4

  val FRACTIONAL: Regex = """^(-|\+)? ?\d*\.\d*$""".r
  val INTEGRAL: Regex = """^(-|\+)? ?\d*$""".r
  val BOOLEAN: Regex = """^(true|false)$""".r

  override def inputSchema: StructType = StructType(StructField("value", StringType) :: Nil)

  override def bufferSchema: StructType = StructType(StructField("null", LongType) ::
    StructField("fractional", LongType) :: StructField("integral", LongType) ::
    StructField("boolean", LongType) :: StructField("string", LongType) :: Nil)

  override def dataType: DataType = BinaryType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(NULL_POS) = 0L
    buffer(FRACTIONAL_POS) = 0L
    buffer(INTEGRAL_POS) = 0L
    buffer(BOOLEAN_POS) = 0L
    buffer(STRING_POS) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (input.isNullAt(0)) {
      buffer(NULL_POS) = buffer.getLong(NULL_POS) + 1L
    } else {
      input.getString(0) match {
        case FRACTIONAL(_) => buffer(FRACTIONAL_POS) = buffer.getLong(FRACTIONAL_POS) + 1L
        case INTEGRAL(_) => buffer(INTEGRAL_POS) = buffer.getLong(INTEGRAL_POS) + 1L
        case BOOLEAN(_) => buffer(BOOLEAN_POS) = buffer.getLong(BOOLEAN_POS) + 1L
        case _ => buffer(STRING_POS) = buffer.getLong(STRING_POS) + 1L
      }
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(NULL_POS) = buffer1.getLong(NULL_POS) + buffer2.getLong(NULL_POS)
    buffer1(FRACTIONAL_POS) = buffer1.getLong(FRACTIONAL_POS) + buffer2.getLong(FRACTIONAL_POS)
    buffer1(INTEGRAL_POS) = buffer1.getLong(INTEGRAL_POS) + buffer2.getLong(INTEGRAL_POS)
    buffer1(BOOLEAN_POS) = buffer1.getLong(BOOLEAN_POS) + buffer2.getLong(BOOLEAN_POS)
    buffer1(STRING_POS) = buffer1.getLong(STRING_POS) + buffer2.getLong(STRING_POS)
  }

  override def evaluate(buffer: Row): Any = {
    DataTypeHistogram.toBytes(buffer.getLong(NULL_POS), buffer.getLong(FRACTIONAL_POS),
      buffer.getLong(INTEGRAL_POS), buffer.getLong(BOOLEAN_POS), buffer.getLong(STRING_POS))
  }
}
