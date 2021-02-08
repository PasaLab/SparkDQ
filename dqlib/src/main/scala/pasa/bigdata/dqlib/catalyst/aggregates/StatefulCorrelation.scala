package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.aggregate.Corr
import org.apache.spark.sql.catalyst.expressions.{CreateStruct, Expression}
import org.apache.spark.sql.types._


private[sql] class StatefulCorrelation(x: Expression, y: Expression) extends Corr(x, y) {

  override def dataType: org.apache.spark.sql.types.DataType =
    StructType(StructField("n", DoubleType) :: StructField("xAvg", DoubleType) ::
    StructField("yAvg", DoubleType) :: StructField("ck", DoubleType) ::
    StructField("xMk", DoubleType) :: StructField("yMk", DoubleType) :: Nil)

  override val evaluateExpression: Expression = {
    CreateStruct(n :: xAvg :: yAvg :: ck :: xMk :: yMk :: Nil)
  }

  override def prettyName: String = "stateful_corr"

  override def canEqual(other: Any): Boolean = other.isInstanceOf[StatefulCorrelation]

  override def equals(other: Any): Boolean = other match {
    case that: StatefulCorrelation =>
      (that canEqual this) && evaluateExpression == that.evaluateExpression
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), evaluateExpression)
    state.map { _.hashCode() }.foldLeft(0) {(a, b) => 31 * a + b }
  }
}
