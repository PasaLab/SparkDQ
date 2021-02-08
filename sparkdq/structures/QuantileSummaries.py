import math


class QuantileSummaries:

    def __init__(self, compress_threshold, relative_error, count, sampled):
        self.compress_threshold = compress_threshold
        self.relative_error = relative_error
        self.count = count
        self.sampled = sampled.copy()
        self.compressed = False

    def merge(self, other):
        if other.count == 0:
            return
        elif self.count == 0:
            self.copy(other)
        else:
            res = sorted(self.sampled + other.sampled, key=lambda x: x.value)
            comp = self.compress_immut(res, 2 * self.relative_error * self.count)
            return QuantileSummaries(other.compress_threshold, other.relative_error, comp, other.count + self.count)

    def copy(self, other):
        self.compress_threshold = other.compress_threshold
        self.relative_error = other.relative_error
        self.count = other.count
        self.sampled = other.sampled
        self.compressed = other.compressed

    def compress_immut(self, samples, merge_threshold):
        res = list()
        if len(samples) == 0:
            return res
        head = samples[-1]
        i = len(samples) - 2
        while i >= 1:
            sample1 = samples[i]
            if sample1.g + head.g + head.delta < merge_threshold:
                head.copy(g=head.g + sample1.g)
            else:
                res.append(head)
                head = sample1
            i -= 1
        res.append(head)
        curr_head = samples[0]
        if (curr_head.value <= head.value) and (len(samples) > 1):
            res.append(curr_head)
        res.reverse()
        return res

    def query_by_quantile(self, quantile):
        """
        Run a query for a given quantile
        :param quantile: float
        :return: single value of quantile
        """
        if len(self.sampled) == 0:
            return None
        if quantile <= self.relative_error:
            return self.sampled[0].value
        if quantile >= 1 - self.relative_error:
            return self.sampled[-1].value

        rank = math.ceil(quantile * self.count)
        target_error = self.relative_error * self.count

        min_rank = 0
        i = 0
        while i < len(self.sampled) - 1:
            cur_sample = self.sampled[i]
            min_rank += cur_sample.g
            max_rank = min_rank + cur_sample.delta
            if (max_rank - target_error <= rank) and (rank <= min_rank + target_error):
                return cur_sample.value
            i += 1
        return self.sampled[-1].value

    def query_by_quantiles(self, quantiles):
        """
        :param quantiles: list of float
        :return: quantiles map
        """
        quantiles_map = dict()
        for quantile in quantiles:
            quantiles_map[str(quantile)] = self.query_by_quantile(quantile)
        return quantiles_map
