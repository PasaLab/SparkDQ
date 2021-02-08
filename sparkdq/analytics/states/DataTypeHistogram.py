from sparkdq.analytics.states.State import State
from sparkdq.structures.DataTypeInstances import DataTypeInstances
from sparkdq.structures.Distribution import Distribution
from sparkdq.structures.DistributionValue import DistributionValue

# DataType Histogram constants
SIZE_IN_BYTES = 40
BYTE_ORDER = "big"


class DataTypeHistogram(State):

    def __init__(self, num_null, num_fractional, num_integral, num_boolean, num_string):
        self.num_null = num_null
        self.num_fractional = num_fractional
        self.num_integral = num_integral
        self.num_boolean = num_boolean
        self.num_string = num_string

    # def sum(self, other):
    #     return DataTypeHistogram(self.num_null + other.num_null, self.num_fractional + other.num_fractional,
    #                              self.num_integral + other.num_integral, self.num_boolean + other.num_boolean,
    #                              self.num_string + other.num_string)

    @staticmethod
    def from_bytes(value):
        """
        Parse state from bytes
        :param value:
        :return:
        """
        assert len(value) == SIZE_IN_BYTES
        num_null = int.from_bytes(value[0:8], byteorder=BYTE_ORDER)
        num_fractional = int.from_bytes(value[8:16], byteorder=BYTE_ORDER)
        num_integral = int.from_bytes(value[16:24], byteorder=BYTE_ORDER)
        num_boolean = int.from_bytes(value[24:32], byteorder=BYTE_ORDER)
        num_string = int.from_bytes(value[32:40], byteorder=BYTE_ORDER)
        return DataTypeHistogram(num_null, num_fractional, num_integral, num_boolean, num_string)

    @staticmethod
    def to_bytes(state):
        bytes_content = state.num_null.to_bytes(8, BYTE_ORDER) + state.num_fractional.to_bytes(8, BYTE_ORDER) +\
            state.num_integral.to_bytes(8, BYTE_ORDER) + state.num_boolean.to_bytes(8, BYTE_ORDER) +\
            state.num_string.to_bytes(8, BYTE_ORDER)
        return bytes_content

    @staticmethod
    def to_distribution(state):
        """
        Transfer this state to a summarized type Distribution
        :param state:
        :return:
        """
        total_observations = state.num_null + state.num_fractional + state.num_integral + state.num_boolean + \
                             state.num_string
        values = dict()
        if total_observations == 0:
            values[DataTypeInstances.Unknown.name] = DistributionValue(0, 0.0)
            values[DataTypeInstances.Fractional.name] = DistributionValue(0, 0.0)
            values[DataTypeInstances.Integral.name] = DistributionValue(0, 0.0)
            values[DataTypeInstances.Boolean.name] = DistributionValue(0, 0.0)
            values[DataTypeInstances.String.name] = DistributionValue(0, 0.0)
        else:
            values[DataTypeInstances.Unknown.name] = DistributionValue(state.num_null,
                                                                       state.num_null / total_observations)
            values[DataTypeInstances.Fractional.name] = DistributionValue(state.num_fractional,
                                                                          state.num_fractional / total_observations)
            values[DataTypeInstances.Integral.name] = DistributionValue(state.num_integral,
                                                                        state.num_integral / total_observations)
            values[DataTypeInstances.Boolean.name] = DistributionValue(state.num_boolean,
                                                                       state.num_boolean / total_observations)
            values[DataTypeInstances.String.name] = DistributionValue(state.num_string,
                                                                      state.num_string / total_observations)
        return Distribution(values, 5)

    @staticmethod
    def infer_type(distribution):
        _ratio_of = DataTypeHistogram.ratio_of
        if _ratio_of(DataTypeInstances.Unknown, distribution) == 1:
            return DataTypeInstances.Unknown
        # 有字符串类型 或 同时存在布尔型和数值型
        elif (_ratio_of(DataTypeInstances.String, distribution) > 0) or \
                ((_ratio_of(DataTypeInstances.Boolean, distribution) > 0) and
                 ((_ratio_of(DataTypeInstances.Integral, distribution) > 0) or
                  (_ratio_of(DataTypeInstances.Fractional, distribution) > 0))):
            return DataTypeInstances.String
        # 只有布尔型
        elif _ratio_of(DataTypeInstances.Boolean, distribution) > 0:
            return DataTypeInstances.Boolean
        # 只有数值型
        elif _ratio_of(DataTypeInstances.Fractional, distribution) > 0:
            return DataTypeInstances.Fractional
        return DataTypeInstances.Integral

    @staticmethod
    def ratio_of(data_type, distribution):
        if data_type.name in distribution.values:
            return distribution.values[data_type.name].ratio
        return 0
