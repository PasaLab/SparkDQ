class GenericStatistics:

    def __init__(self, num_records, completenesses, approx_count_distincts, inferred_types_and_counts, known_types):
        self.num_records = num_records
        self.completenesses = completenesses
        self.approx_count_distincts = approx_count_distincts
        self.inferred_types_and_counts = inferred_types_and_counts
        self.known_types = known_types

    def get_column_type(self, column):
        if column in self.known_types:
            return self.known_types[column]
        return self.inferred_types_and_counts[column][0]


class NumericStatistics:

    def __init__(self, minimums, maximums, means, stddevs, sums, approx_quantiles):
        self.minimums = minimums
        self.maximums = maximums
        self.means = means
        self.stddevs = stddevs
        self.sums = sums
        self.approx_quantiles = approx_quantiles


class CategoricalStatistics:

    def __init__(self, histograms):
        self.histograms = histograms
