from sparkdq.profiling.ProfilerRunBuilder import ProfilerRunBuilder


class ProfilerSuite:

    @staticmethod
    def on_data(data):
        return ProfilerRunBuilder(data)
