import math

from sparkdq.analytics.states.State import DoubleValuedState


class CorrelationState(DoubleValuedState):

    def __init__(self, n, x_avg, y_avg, ck, x_mk, y_mk):
        self.n = n
        self.x_avg = x_avg
        self.y_avg = y_avg
        self.ck = ck
        self.x_mk = x_mk
        self.y_mk = y_mk

    def metric_value(self):
        return self.ck / math.sqrt(self.x_mk * self.y_mk)

    # def sum(self, other):
    #     n1 = self.n
    #     n2 = other.n
    #     new_n = n1 + n2
    #     dx = other.x_avg - self.x_avg
    #     dx_n = 0.0 if new_n == 0.0 else dx / new_n
    #     dy = other.y_avg - self.y_avg
    #     dy_n = 0.0 if new_n == 0.0 else dy / new_n
    #     new_x_avg = self.x_avg + dx_n * n2
    #     new_y_avg = self.y_avg + dy_n * n2
    #     new_ck = self.ck + other.ck + dx * dy_n * n1 * n2
    #     new_x_mk = self.x_mk + other.x_mk + dx * dx_n * n1 * n2
    #     new_y_mk = self.y_mk + other.y_mk + dy * dy_n * n1 * n2
    #     return CorrelationState(new_n, new_x_avg, new_y_avg, new_ck, new_x_mk, new_y_mk)
