class ResultKey:
    """
    Key for storing and searching Metrics.
    """
    def __init__(self, data_set_date, tags):
        self.data_set_date = data_set_date
        self.tags = tags

    def __str__(self):
        return "DataSetDate: {}\nTags: {}".format(self.data_set_date, self.tags)

    def __eq__(self, other):
        if self.data_set_date != other.data_set_date:
            return False
        if len(self.tags) != len(other.tags):
            return False
        for k, v in self.tags.items():
            if (k not in other.tags) or (v != other.tags[k]):
                return False
        return True

    def __hash__(self):
        return hash((self.data_set_date, frozenset(self.tags.items())))

    def to_json(self):
        return {
            "DataSetDate": self.data_set_date,
            "Tags": self.tags
        }

    @staticmethod
    def from_json(d):
        return ResultKey(d["DataSetDate"], d["Tags"])
