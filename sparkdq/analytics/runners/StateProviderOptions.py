class StateProviderOptions:
    """
    StateProvider options including state provider and whether directly use the state from provider.
    """
    def __init__(self, state_provider=None, reuse_state=False):
        self.state_provider = state_provider
        self.reuse_state = reuse_state
