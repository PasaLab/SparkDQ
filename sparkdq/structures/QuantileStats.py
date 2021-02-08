class QuantileStats:

    def __init__(self, value, g, delta):
        self.value = value
        self.g = g
        self.delta = delta

    def __str__(self):
        return "{}, {}, {}".format(self.value, self.g, self.delta)

    def copy(self, value=None, g=None, delta=None):
        if value is not None:
            self.value = value
        if g is not None:
            self.g = g
        if delta is not None:
            self.delta = delta
