from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.functions import col

from sparkdq.conf.Context import Context


def stateful_data_type(column):
    """
    Stateful data type SQL function
    There is another calling way, if the target class inherited from UserDefinedAggregation, we can use xx.apply
    directly.But this is suitable for other classes inherited from ImperativeAggregate.

    For example:
    _stateful_data_type = spark._jvm.pasa.bigdata.dqlib.catalyst.StatefulDataType.apply
    return Column(_stateful_data_type(_to_seq(spark, [column], _to_java_column)))

    Note that:
    If called correctly, the stateful object should be a JavaMember corresponding to a java method.If you meet a
    JavaPackage, check your method path.
    """
    spark = Context().spark
    if isinstance(column, str):
        column = col(column)
    _stateful_data_type = spark._jvm.org.apache.spark.sql.DQFunctions.stateful_data_type
    return Column(_stateful_data_type(_to_java_column(column)))


def stateful_approx_count_distinct(column):
    spark = Context().spark
    if isinstance(column, str):
        column = col(column)
    _stateful_approx_count_distinct = spark._jvm.org.apache.spark.sql.DQFunctions.stateful_approx_count_distinct
    return Column(_stateful_approx_count_distinct(_to_java_column(column)))


def stateful_stddev(column):
    spark = Context().spark
    if isinstance(column, str):
        column = col(column)
    _stateful_stddev = spark._jvm.org.apache.spark.sql.DQFunctions.stateful_stddev
    return Column(_stateful_stddev(_to_java_column(column)))


def stateful_approx_quantile(column, relative_error):
    spark = Context().spark
    if isinstance(column, str):
        column = col(column)
    _stateful_approx_quantile = spark._jvm.org.apache.spark.sql.DQFunctions.stateful_approx_quantile
    return Column(_stateful_approx_quantile(_to_java_column(column), relative_error))


def stateful_correlation(column1, column2):
    spark = Context().spark
    if isinstance(column1, str):
        column1 = col(column1)
    if isinstance(column2, str):
        column2 = col(column2)
    _stateful_correlation = spark._jvm.org.apache.spark.sql.DQFunctions.stateful_correlation
    return Column(_stateful_correlation(_to_java_column(column1), _to_java_column(column2)))


def stateful_mode(column):
    spark = Context().spark
    if isinstance(column, str):
        column = col(column)
    _stateful_mode = spark._jvm.org.apache.spark.sql.DQFunctions.stateful_mode
    return Column(_stateful_mode(_to_java_column(column)))
