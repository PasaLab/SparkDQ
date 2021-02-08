class DistributionValue:
    """
    Represent each bin's absolute count and ratio from the whole data
    """
    def __init__(self, absolute, ratio):
        self.absolute = absolute
        self.ratio = ratio

    def __str__(self):
        return "Absolute: {}, Ratio: {}".format(self.absolute, self.ratio)

    def to_json(self):
        return {
            "Absolute": self.absolute,
            "Ratio": self.ratio
        }

    @staticmethod
    def from_json(d):
        return DistributionValue(d["Absolute"], d["Ratio"])
