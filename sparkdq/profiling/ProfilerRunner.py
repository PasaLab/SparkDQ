from sparkdq.profiling.Profiler import Profiler


class ProfilerRunner:

    @staticmethod
    def on_data(data):
        from sparkdq.profiling.ProfilerRunBuilder import ProfilerRunBuilder
        return ProfilerRunBuilder(data)

    @staticmethod
    def do_profile_run(data, specified_columns, low_cardinality_threshold):
        profiles = Profiler.profile(data, specified_columns, low_cardinality_threshold)
        return profiles

