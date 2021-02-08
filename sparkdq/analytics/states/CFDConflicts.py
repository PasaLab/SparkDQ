from sparkdq.analytics.states.State import State


class CFDConflicts(State):

    def __init__(self, total_violations):
        self.total_violations = total_violations

    # def sum(self, other):
    #     pass

    def violations(self):
        return self.total_violations
