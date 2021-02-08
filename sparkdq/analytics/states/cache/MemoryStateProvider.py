from sparkdq.analytics.states.cache.StateProvider import StateProvider


states = {}


class MemoryStateProvider(StateProvider):

    def has_state(self, analyzer):
        state_key = analyzer.__hash__()
        return state_key in states

    def load(self, analyzer):
        state_key = analyzer.__hash__()
        if state_key in states:
            return states[state_key]
        return None

    def persist(self, analyzer, state):
        state_key = analyzer.__hash__()
        states[state_key] = state
