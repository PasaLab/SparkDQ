from pyspark.sql.functions import col, count, expr, sum as _sum, when

from sparkdq.exceptions.AnalysisExceptions import EmptyStateException, AnalysisException
from sparkdq.structures.Entity import Entity


# common columns for managing DataFrames
INDEX_COL = "id"

#  additional temporary columns, remove after used
COUNT_COL = "sparkdq_count"
MAD_COL = "sparkdq_mad"
SMA_COL = "sparkdq_sma"


def merge(state1, state2):
    if state1 is None:
        return state2
    elif state2 is None:
        return state1
    else:
        return state1.sum_untyped(state2)


def metric_from_value(analyzer, cls, value):
    return cls(analyzer.name, analyzer.entity, analyzer.instance, value, True)


def metric_from_failure(analyzer, cls, exception):
    return cls(analyzer.name, analyzer.entity, analyzer.instance,
               AnalysisException.wrap_if_necessary(exception), False)


def metric_from_empty(analyzer, cls):
    return metric_from_failure(analyzer, cls, empty_state_exception(analyzer))


def empty_state_exception(analyzer):
    return EmptyStateException("Empty state for analyzer {}, all input values were NULL."
                               .format(analyzer))


def if_no_nulls_in(result, offset, how_many=1):
    """
    Check if null not exist from offset until offset+how_many
    :param result: aggregation result
    :param offset: current offset
    :param how_many: check number
    :return: true if not exist, else false
    """
    for index in range(offset, offset + how_many):
        if result[index] is None:
            return False
    return True


def entity_from(columns):
    if len(columns) == 1:
        return Entity.Column
    return Entity.Multicolumn


def conditional_count(where):
    """
    :param where: str, default where
    :return:
    """
    if where is None:
        return count("*")
    return _sum(expr(where).cast("long"))


def conditional_selection(selection, where):
    """
    :param selection: str or Column
    :param where: str, default None
    :return: Column
    """
    if isinstance(selection, str):
        selection = col(selection)
    if where is None:
        return selection
    return when(expr(where), selection)


def value_picker(member):
    """
    value picker for fetching member from value
    :param member: member for fetching
    :return:
    """
    def _value_picker(value):
        return value.__getattribute__(str(member))
    return _value_picker
