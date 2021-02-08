from sparkdq.analytics.runners.StateProviderOptions import StateProviderOptions


class RepairRunBuilder:

    def __init__(self, data):
        self.data = data
        self.cleans = []
        self.state_provider = StateProviderOptions()

    def add_clean(self, clean):
        self.cleans.append(clean)
        return self

    def add_cleans(self, cleans):
        self.cleans.extend(cleans)
        return self

    def copy(self, repair_builder):
        self.data = repair_builder.data
        self.cleans = repair_builder.cleans
        return self

    def load_state_with(self, state_provider):
        self.state_provider = state_provider
        return self

    def run(self):
        from sparkdq.repairs.RepairSuite import RepairSuite
        for clean in self.cleans:
            self.data = RepairSuite.do_repair_run(self.data, clean.transformers, self.state_provider)
        return self.data
