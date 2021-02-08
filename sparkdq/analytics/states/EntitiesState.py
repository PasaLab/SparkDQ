from sparkdq.analytics.states.State import DoubleValuedState


class EntitiesState(DoubleValuedState):

    def __init__(self, entities_count):
        self.entities_count = entities_count

    # def sum(self, other):
    #     pass

    def metric_value(self):
        return self.entities_count
