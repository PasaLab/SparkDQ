import pymmh3

from sparkdq.analytics.analyzers.ApproxCountDistinct import ApproxCountDistinct
from sparkdq.analytics.analyzers.ApproxQuantile import ApproxQuantile
from sparkdq.analytics.analyzers.Completeness import Completeness
from sparkdq.analytics.analyzers.Compliance import Compliance
from sparkdq.analytics.analyzers.Correlation import Correlation
from sparkdq.analytics.analyzers.DataType import DataType
from sparkdq.analytics.analyzers.Maximum import Maximum
from sparkdq.analytics.analyzers.Mean import Mean
from sparkdq.analytics.analyzers.Minimum import Minimum
from sparkdq.analytics.analyzers.PatternMatch import PatternMatch
from sparkdq.analytics.analyzers.Size import Size
from sparkdq.analytics.analyzers.StandardDeviation import StandardDeviation
from sparkdq.analytics.analyzers.Sum import Sum
from sparkdq.analytics.GroupingAnalyzers import FrequencyBasedAnalyzer
from sparkdq.analytics.states.ApproxCountDistinctState import ApproxCountDistinctState
from sparkdq.analytics.states.ApproxQuantileState import ApproxQuantileState
from sparkdq.analytics.states.cache.StateProvider import StateProvider
from sparkdq.analytics.states.CorrelationState import CorrelationState
from sparkdq.analytics.states.DataTypeHistogram import DataTypeHistogram
from sparkdq.analytics.states.FrequenciesAndRows import FrequenciesAndRows
from sparkdq.analytics.states.MaxState import MaxState
from sparkdq.analytics.states.MeanState import MeanState
from sparkdq.analytics.states.MinState import MinState
from sparkdq.analytics.states.NumMatches import NumMatches
from sparkdq.analytics.states.NumMatchesAndRows import NumMatchesAndRows
from sparkdq.analytics.states.StandardDeviationState import StandardDeviationState
from sparkdq.analytics.states.SumState import SumState
from sparkdq.conf.Context import Context
from sparkdq.io.hdfs import read_file_from_hdfs, write_file_on_hdfs, check_exist_for_hdfs


class FileSystemStateProvider(StateProvider):

    def __init__(self, base_path, over_write):
        self.base_path = base_path
        self.over_write = over_write

    def has_state(self, analyzer):
        identifier = FileSystemStateProvider.to_identifier(analyzer)
        if isinstance(analyzer, FrequencyBasedAnalyzer):
            df_name = "{}/{}-frequencies.parquet".format(self.base_path, identifier)
            file_name = "{}/{}-num_rows.bin".format(self.base_path, identifier)
            return check_exist_for_hdfs(df_name) & check_exist_for_hdfs(file_name)
        else:
            file_name = "{}/{}.bin".format(self.base_path, identifier)
            return check_exist_for_hdfs(file_name)

    def load(self, analyzer):
        identifier = FileSystemStateProvider.to_identifier(analyzer)
        if isinstance(analyzer, Size):
            return NumMatches(self.load_long(identifier))
        elif isinstance(analyzer, Sum):
            return SumState(self.load_double(identifier))
        elif isinstance(analyzer, Minimum):
            return MinState(self.load_double(identifier))
        elif isinstance(analyzer, Maximum):
            return MaxState(self.load_double(identifier))
        elif isinstance(analyzer, Compliance) or isinstance(analyzer, Completeness) or \
                isinstance(analyzer, PatternMatch):
            matches_and_rows = self.load_long_long(identifier)
            return NumMatchesAndRows(matches_and_rows[0], matches_and_rows[1])
        elif isinstance(analyzer, Mean):
            summation_and_count = self.load_double_long(identifier)
            return MeanState(summation_and_count[0], summation_and_count[1])
        elif isinstance(analyzer, DataType):
            return DataTypeHistogram.from_bytes(self.load_bytes(identifier))
        elif isinstance(analyzer, ApproxCountDistinct):
            return ApproxCountDistinctState.words_from_bytes(self.load_bytes(identifier))
        elif isinstance(analyzer, Correlation):
            return self.load_correlation_state(identifier)
        elif isinstance(analyzer, StandardDeviation):
            return self.load_standard_deviation_state(identifier)
        elif isinstance(analyzer, ApproxQuantile):
            freq_rows = self.load_bytes(identifier)
            return ApproxQuantileState(ApproxQuantileState.quantile_summaries_from_bytes(freq_rows))
        elif isinstance(analyzer, FrequencyBasedAnalyzer):
            df_rows = self.load_df_long(identifier)
            return FrequenciesAndRows(df_rows[0], df_rows[1])
        else:
            raise Exception("Unable to load state for analyzer {}.".format(analyzer))

    def persist(self, analyzer, state):
        identifier = FileSystemStateProvider.to_identifier(analyzer)
        if isinstance(analyzer, Size) or isinstance(analyzer, Sum) or isinstance(analyzer, Minimum)\
                or isinstance(analyzer, Maximum):
            self.persist_single_state(state, identifier)
        elif isinstance(analyzer, Compliance) or isinstance(analyzer, Completeness)\
                or isinstance(analyzer, PatternMatch) or isinstance(analyzer, Mean):
            self.persist_double_state(state, identifier)
        elif isinstance(analyzer, DataType):
            bytes_content = DataTypeHistogram.to_bytes(state)
            self.persist_bytes(bytes_content, identifier)
        elif isinstance(analyzer, ApproxCountDistinct):
            bytes_content = ApproxCountDistinctState.words_to_bytes(state)
            self.persist_bytes(bytes_content, identifier)
        elif isinstance(analyzer, Correlation):
            self.persist_correlation_state(state, identifier)
        elif isinstance(analyzer, StandardDeviation):
            self.persist_standard_deviation_state(state, identifier)
        elif isinstance(analyzer, ApproxQuantile):
            bytes_content = ApproxQuantileState.quantile_summaries_to_bytes(state.quantile_summaries)
            self.persist_bytes(bytes_content, identifier)
        elif isinstance(analyzer, FrequencyBasedAnalyzer):
            self.persist_df_long_state(state, identifier)
        else:
            raise Exception("Unable to persist state for analyzer {}.".format(analyzer))

    def persist_single_state(self, state, identifier):
        file_name = "{}/{}.bin".format(self.base_path, identifier)
        write_file_on_hdfs(file_name, str(state.metric_value()), self.over_write)

    def persist_double_state(self, state, identifier):
        file_name = "{}/{}.bin".format(self.base_path, identifier)
        write_file_on_hdfs(file_name, "{},{}".format(state.num_matches, state.num_rows), self.over_write)

    def persist_bytes(self, bytes_content, identifier):
        file_name = "{}/{}.bin".format(self.base_path, identifier)
        write_file_on_hdfs(file_name, str(bytes_content, "utf-8"), self.over_write)

    def persist_correlation_state(self, state, identifier):
        file_name = "{}/{}.bin".format(self.base_path, identifier)
        content = "{},{},{},{},{},{}".format(state.n, state.x_avg, state.y_avg, state.ck, state.x_mk, state.y_mk)
        write_file_on_hdfs(file_name, content, identifier)

    def persist_standard_deviation_state(self, state, identifier):
        file_name = "{}/{}.bin".format(self.base_path, identifier)
        content = "{},{},{}".format(state.n, state.avg, state.m2)
        write_file_on_hdfs(file_name, content, identifier)

    def persist_df_long_state(self, state, identifier):
        df_name = "{}/{}-frequencies.parquet".format(self.base_path, identifier)
        file_name = "{}/{}-num_rows.bin".format(self.base_path, identifier)
        state.frequencies.write.format('parquet').save(df_name)
        write_file_on_hdfs(file_name, str(state.num_rows), self.over_write)

    def load_long(self, identifier):
        file_name = "{}/{}.bin".format(self.base_path, identifier)
        return int(read_file_from_hdfs(file_name))

    def load_double(self, identifier):
        file_name = "{}/{}.bin".format(self.base_path, identifier)
        return float(read_file_from_hdfs(file_name))

    def load_long_long(self, identifier):
        file_name = "{}/{}.bin".format(self.base_path, identifier)
        contents = read_file_from_hdfs(file_name).split(",")
        return int(contents[0]), int(contents[1])

    def load_double_long(self, identifier):
        file_name = "{}/{}.bin".format(self.base_path, identifier)
        contents = read_file_from_hdfs(file_name).split(",")
        return float(contents[0]), int(contents[1])

    def load_bytes(self, identifier):
        file_name = "{}/{}.bin".format(self.base_path, identifier)
        return bytes(read_file_from_hdfs(file_name), "utf-8")

    def load_correlation_state(self, identifier):
        file_name = "{}/{}.bin".format(self.base_path, identifier)
        contents = read_file_from_hdfs(file_name).split(",")
        return CorrelationState(int(contents[0]), float(contents[1]), float(contents[2]), float(contents[3]),
                                float(contents[4]), float(contents[5]))

    def load_standard_deviation_state(self, identifier):
        file_name = "{}/{}.bin".format(self.base_path, identifier)
        contents = read_file_from_hdfs(file_name).split(",")
        return StandardDeviationState(int(contents[0]), float(contents[1]), float(contents[2]))

    def load_df_long(self, identifier):
        df_name = "{}/{}-frequencies.parquet".format(self.base_path, identifier)
        file_name = "{}/{}-num_rows.bin".format(self.base_path, identifier)
        spark = Context().spark
        return spark.read.parquet(df_name), int(read_file_from_hdfs(file_name))

    @staticmethod
    def to_identifier(analyzer):
        return str(pymmh3.hash(analyzer.__hash__(), 42))
