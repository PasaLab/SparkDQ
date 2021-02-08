from sparkdq.profiling.Profiler import DEFAULT_CARDINALITY_THRESHOLD
from sparkdq.profiling.ProfilerRunner import ProfilerRunner


class ProfilerRunBuilder:

    def __init__(self, data):
        self.data = data
        self.specified_columns = None
        self.low_cardinality_threshold = DEFAULT_CARDINALITY_THRESHOLD

    def run(self):
        return ProfilerRunner.do_profile_run(self.data, self.specified_columns, self.low_cardinality_threshold)

    def set_specified_columns(self, new_specified_columns):
        self.specified_columns = new_specified_columns
        return self

    def set_low_cardinality_threshold(self, new_low_cardinality_threshold):
        self.low_cardinality_threshold = new_low_cardinality_threshold
        return self
