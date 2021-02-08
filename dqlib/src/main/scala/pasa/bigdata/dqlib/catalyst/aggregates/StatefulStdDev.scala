package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.aggregate.CentralMomentAgg
import org.apache.spark.sql.catalyst.expressions.{CreateStruct, Expression}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}

case class StatefulStdDev(child: Expression) extends CentralMomentAgg(child) {

  override protected def momentOrder = 2

  override def dataType: DataType = StructType(StructField("n", DoubleType) ::
    StructField("avg", DoubleType) :: StructField("m2", DoubleType) :: Nil)

  override val evaluateExpression: Expression = CreateStruct(n :: avg :: m2 :: Nil)

  override def prettyName: String = "stateful_stddev"

}
