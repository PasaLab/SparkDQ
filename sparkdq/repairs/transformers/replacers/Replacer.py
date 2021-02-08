from abc import abstractmethod

from pyspark.sql import Window
from pyspark.sql.functions import col, when, last, first

from sparkdq.analytics.analyzers.ApproxQuantile import ApproxQuantile
from sparkdq.analytics.analyzers.Mean import Mean
from sparkdq.analytics.analyzers.Minimum import Minimum
from sparkdq.analytics.analyzers.Maximum import Maximum
from sparkdq.analytics.analyzers.Mode import Mode
from sparkdq.analytics.states.ApproxQuantileState import ApproxQuantileState
from sparkdq.analytics.states.MaxState import MaxState
from sparkdq.analytics.states.MinState import MinState
from sparkdq.analytics.states.MeanState import MeanState
from sparkdq.analytics.Preconditions import has_column
from sparkdq.exceptions.CommonExceptions import UnknownStateException
from sparkdq.exceptions.TransformExceptions import IllegalTransformerParameterException
from sparkdq.models.CommonUtils import DEFAULT_INDEX_COL
from sparkdq.repairs.CommonUtils import ReplaceWays, WAYS_NEED_PREPARE, STRING_SUPPORTED_WAYS, NUMERIC_SUPPORTED_WAYS
from sparkdq.repairs.transformers.Transformer import Transformer


class Replacer(Transformer):

    def __init__(self, column, way, value=None, additional_condition=None, order_by=DEFAULT_INDEX_COL):
        self.column = column
        self.way = way
        if self.way == ReplaceWays.FORWARD.value:
            window = Window.orderBy(order_by).rowsBetween(Window.unboundedPreceding, -1)
            self.value = last(self.column, True).over(window)
        elif self.way == ReplaceWays.BACKWARD.value:
            window = Window.orderBy(order_by).rowsBetween(1, Window.unboundedFollowing)
            self.value = first(self.column, True).over(window)
        else:
            self.value = value
        self.additional_condition = additional_condition

    @abstractmethod
    def select_condition(self):
        pass

    def need_preparation(self):
        return self.way in WAYS_NEED_PREPARE

    def related_analyzer(self):
        if self.way == ReplaceWays.MAX.value:
            return Maximum(self.column, self.additional_condition)
        elif self.way == ReplaceWays.MIN.value:
            return Minimum(self.column, self.additional_condition)
        elif self.way == ReplaceWays.MEAN.value:
            return Mean(self.column,  self.additional_condition)
        elif self.way == ReplaceWays.MEDIAN.value:
            return ApproxQuantile(self.column, 0.5, where=self.additional_condition)
        elif self.way == ReplaceWays.MODE.value:
            return Mode(self.column, self.additional_condition)
        else:
            return None

    def prepare_replacement(self, value):
        self.value = value

    def common_preconditions(self):
        return [has_column(self.column)]

    @staticmethod
    def _num_param_check(way, value):
        def _param_check(_):
            supported_ways = NUMERIC_SUPPORTED_WAYS
            if way not in supported_ways:
                raise IllegalTransformerParameterException("Unsupported numeric fill way {}, only {} are supported now!"
                                                           .format(way, ", ".join(supported_ways)))
            if (way == ReplaceWays.FIXED.value) and (value is None):
                raise IllegalTransformerParameterException("Meaningless numeric fill, fill null with null!")
        return _param_check

    @staticmethod
    def _str_param_check(way, value):
        def _param_check(_):
            supported_ways = STRING_SUPPORTED_WAYS
            if way not in supported_ways:
                raise IllegalTransformerParameterException("Unsupported string fill way {}, only {} are supported now!"
                                                           .format(way, ", ".join(supported_ways)))
            if (way == ReplaceWays.FIXED.value) and (value is None):
                raise IllegalTransformerParameterException("Meaningless string fill, fill null with null!")
        raise _param_check

    @staticmethod
    def fetch_replacement_from_state(state):
        if isinstance(state, MaxState) or isinstance(state, MinState) or isinstance(state, MeanState):
            return state.metric_value()
        elif isinstance(state, ApproxQuantileState):
            return state.query(0.5)
        else:
            raise UnknownStateException("Unsupported state type {} of state {}!".format(type(state), state))

    @staticmethod
    def merge_conditions(replacers, column):
        normal_replacers = []
        sub_replacers = []
        for replacer in replacers:
            if isinstance(replacer, SubReplacer):
                sub_replacers.append(replacer)
            else:
                normal_replacers.append(replacer)
        if len(normal_replacers) == 0:
            target_column = Replacer.merge_partial_replacers(sub_replacers)
        else:
            first_replacer = normal_replacers[0]
            target_column = when(first_replacer.select_condition(), first_replacer.value)
            for r in normal_replacers[1:]:
                target_column = target_column.when(r.select_condition(), r.value)
            if len(sub_replacers) == 0:
                target_column = target_column.otherwise(col(column))
            else:
                target_column = target_column.otherwise(Replacer.merge_partial_replacers(sub_replacers))
        return target_column.alias(column)

    @staticmethod
    def merge_partial_replacers(replacers):
        target_column = replacers[0].select_condition()
        for replacer in replacers[1:]:
            target_column = replacer.merge(target_column)
        return target_column

    def transform(self, data, check=True):
        if check:
            for condition in self.preconditions():
                condition(data.schema)
        if self.need_preparation():
            analyzer = self.related_analyzer()
            results = data.agg(*analyzer.aggregation_functions()).collect()[0]
            state = analyzer.fetch_state_from_aggregation_result(results, 0)
            self.prepare_replacement(Replacer.fetch_replacement_from_state(state))
        return data.withColumn(self.column, when(self.select_condition(), self.value).otherwise(col(self.column)))


class SubReplacer(Replacer):

    @abstractmethod
    def merge(self, other):
        pass
