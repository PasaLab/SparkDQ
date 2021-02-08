import struct

from sparkdq.analytics.states.State import State
from sparkdq.structures.QuantileStats import QuantileStats
from sparkdq.structures.QuantileSummaries import QuantileSummaries


BYTE_ORDER = "big"


class ApproxQuantileState(State):

    def __init__(self, quantile_summaries):
        self.quantile_summaries = quantile_summaries

    # def sum(self, other):
    #     self.quantile_summaries.merge(other.quantile_summaries)
    #     return self

    def query(self, quantile):
        return self.quantile_summaries.query_by_quantile(quantile)

    @staticmethod
    def quantile_summaries_from_bytes(value):
        compress_threshold = int.from_bytes(value[0:4], byteorder=BYTE_ORDER)
        relative_error = struct.unpack('>d', value[4:12])[0]
        count = int.from_bytes(value[12:20], byteorder=BYTE_ORDER)
        sampled_length = int.from_bytes(value[20:24], byteorder=BYTE_ORDER)
        sampled = []
        base = 24
        offset = 16

        for i in range(sampled_length):
            v = struct.unpack('>d', value[base:base+8])[0]
            g = int.from_bytes(value[base+8:base+12], byteorder=BYTE_ORDER)
            delta = int.from_bytes(value[base+12:base+16], byteorder=BYTE_ORDER)
            base += offset
            sampled.append(QuantileStats(v, g, delta))
        return QuantileSummaries(compress_threshold, relative_error, count, sampled)

    @staticmethod
    def quantile_summaries_to_bytes(qs):
        bytes_content = bytes()
        bytes_content += qs.compress_threshold.to_bytes(4, BYTE_ORDER)
        bytes_content += struct.pack('>d', qs.relative_error)
        bytes_content += qs.count.to_bytes(8, BYTE_ORDER)
        bytes_content += len(qs.sampled).to_bytes(4, BYTE_ORDER)
        for sample in qs.sampled:
            bytes_content += struct.pack('>d', sample.v)
            bytes_content += sample.g.to_bytes(4, BYTE_ORDER)
            bytes_content += sample.delta.to_bytes(4, BYTE_ORDER)
        return bytes_content
