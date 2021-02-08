package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, StatefulApproxCountDistinct,
  StatefulApproxQuantile}


object DQFunctions {

  /**
    * Transform AggregateFunction to Column
    */
  private[this] def withAggregateFunction(func: AggregateFunction, isDistinct: Boolean = false): Column = {
    Column(func.toAggregateExpression(isDistinct))
  }

  /** Approximate number of distinct values with state via HLL */
  def stateful_approx_count_distinct(column: Column): Column = withAggregateFunction {
    StatefulApproxCountDistinct(column.expr)
  }

  /** Standard deviation */
  def stateful_stddev(column: Column): Column = withAggregateFunction {
    StatefulStdDev(column.expr)
  }

  /** Approx quantiles */
  def stateful_approx_quantile(column: Column, relative_error: Double): Column = withAggregateFunction {
    StatefulApproxQuantile(
      column.expr,
      Literal(1.0 / relative_error),
      mutableAggBufferOffset = 0,
      inputAggBufferOffset = 0
    )
  }

  /** Correlation */
  def stateful_correlation(column1: Column, column2: Column): Column = withAggregateFunction {
    new StatefulCorrelation(column1.expr, column2.expr)
  }

  /** Actual data type of string attribute */
  def stateful_data_type(column: Column): Column = {
    StatefulDataType.apply(column)
  }

  /** Mode */
  def stateful_mode(column: Column): Column = {
    StatefulMode.apply(column)
  }
}
