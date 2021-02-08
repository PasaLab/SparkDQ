class Profiles:

    def __init__(self, profiles, num_records):
        self.profiles = profiles
        self.num_records = num_records

    def __str__(self):
        return "NumRecords: {}\n{}".format(self.num_records,
                                           "\n".join(map(lambda p: "Column '{}':\n{}\n".format(p[0], p[1]),
                                                         self.profiles.items())))


class StandardProfile:

    def __init__(self, column, completeness, approx_count_distinct, data_type, is_inferred_type, type_counts,
                 histogram):
        self.column = column
        self.completeness = completeness
        self.approx_count_distinct = approx_count_distinct
        self.data_type = data_type
        self.is_inferred_type = is_inferred_type
        self.type_counts = type_counts
        self.histogram = histogram

    def __str__(self):
        return "\tCompleteness: {}\n\tApproxCountDistinct: {}\n\tDataType: {}".format(
            self.completeness, self.approx_count_distinct, self.data_type.name)


class NumericProfile:

    def __init__(self, column, completeness, approx_count_distinct, data_type, is_inferred_type, type_counts, histogram,
                 minimum, maximum, summation, mean, stddev, approx_quantiles):
        self.column = column
        self.completeness = completeness
        self.approx_count_distinct = approx_count_distinct
        self.data_type = data_type
        self.is_inferred_type = is_inferred_type
        self.type_counts = type_counts
        self.histogram = histogram
        self.minimum = minimum
        self.maximum = maximum
        self.summation = summation
        self.mean = mean
        self.stddev = stddev
        self.approx_quantiles = approx_quantiles

    def __str__(self):
        return "\tCompleteness: {}\n\tApproxCountDistinct: {}\n\tDataType: {}\n\tMinimum: {}\n\tMaximum: {}\n\tMean: {}"\
            .format(self.completeness, self.approx_count_distinct, self.data_type.name, self.minimum, self.maximum,
                    self.mean)
