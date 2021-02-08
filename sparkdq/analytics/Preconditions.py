from pyspark.sql.types import ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType, StringType

from sparkdq.exceptions.CommonExceptions import NoSuchColumnException,\
    NoColumnsSpecifiedException, MismatchColumnTypeException, NumberOfSpecifiedColumnsException


def exactly_n(columns, n):
    def _exactly_n(_):
        if len(columns) != n:
            raise NumberOfSpecifiedColumnsException("Exactly {} columns should be specified, but found {},"
                                                    " they are {}".format(n, len(columns), ",".join(columns)))
    return _exactly_n


def find_first_failing(schema, conditions):
    """
    Find the first failing precondition if exists
    :param schema: data schema for testing preconditions
    :param conditions: analyzer's preconditions
    :return: None if all successful, else exception of the first failing precondition
    """
    for condition in conditions:
        try:
            condition(schema)
        except Exception as e:
            return e
    return None


def has_column(column):
    def _has_column(schema):
        if column not in schema.names:
            raise NoSuchColumnException("Input data does not include column {}!".format(column))
    return _has_column


def has_columns(columns):
    def _has_columns(schema):
        if len(columns) < 1:
            raise NoColumnsSpecifiedException("At least one column needs to be specified!")
        for c in columns:
            if c not in schema.names:
                raise NoSuchColumnException("Input data does not include column {}!".format(c))
    return _has_columns


def is_numeric(column):
    def _is_numeric(schema):
        data_type = schema[column].dataType
        type_match = False
        numeric_types = [ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType]
        for numeric_type in numeric_types:
            if isinstance(data_type, numeric_type):
                type_match = True
                break
        if not type_match:
            raise MismatchColumnTypeException(
                "Expected data type of column {} is to be one of {}, but found {}!".format(
                    column, ",".join(map(lambda t: t.__name__, numeric_types)), data_type))
    return _is_numeric


def is_string(column):
    def _is_string(schema):
        data_type = schema[column].dataType
        if not isinstance(data_type, StringType):
            raise MismatchColumnTypeException(
                "Expected data type of column {} is to be StringType, but found {}!".format(column, data_type)
            )
    return _is_string
