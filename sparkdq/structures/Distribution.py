from sparkdq.structures.DistributionValue import DistributionValue


class Distribution:
    """
    Represent data histogram, values are of type DistributionValue, each element can be regarded as a bin

    Args:
        values: map of all the values with the target value key, absolute count and ratio of type Distribution value
    """
    def __init__(self, values, number_of_bins):
        self.values = values
        self.number_of_bins = number_of_bins

    def __str__(self):
        values_str = list(map(lambda item: "({}, {})".format(item[0], item[1]), self.values.items()))
        return "Number of bins: {}, Values: {}".format(self.number_of_bins, ", ".join(values_str))

    def to_json(self):
        return {
            "NumberOfBins": self.number_of_bins,
            "Values": {k: v.to_json() for k, v in self.values.items()}
        }

    @staticmethod
    def from_json(d):
        number_of_bins = d["NumberOfBins"]
        values = {k: DistributionValue.from_json(v) for k, v in d["Values"].items()}
        return Distribution(values, number_of_bins)
