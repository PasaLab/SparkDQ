"""
    Customized Assertions functions
"""


def equal_to(val):
    def _equal_to(x):
        return x == val
    return _equal_to


def is_one():
    def _is_one(x):
        return x == 1.0
    return _is_one


def is_zero():
    def _is_zero(x):
        return x == 0
    return _is_zero


def is_positive():
    def _is_positive(x):
        return x > 0
    return _is_positive


def is_negative():
    def _is_negative(x):
        return x < 0
    return _is_negative


def greater_than(value):
    def _greater_than(x):
        return x > value
    return _greater_than


def greater_than_or_equal_to(value):
    def _greater_than_or_equal_to(x):
        return x >= value
    return _greater_than_or_equal_to


def less_than(value):
    def _less_than(x):
        return x < value
    return _less_than


def less_than_or_equal_to(value):
    def _less_than_or_equal_to(x):
        return x <= value
    return _less_than_or_equal_to


def in_range(left_value, right_value, left_closed=False, right_closed=False):
    def _in_range(x):
        if left_closed and right_closed:
            return (x >= left_value) and (x <= right_value)
        elif left_closed and not right_closed:
            return (x >= left_value) and (x < right_value)
        elif not left_closed and right_closed:
            return (x > left_value) and (x <= right_value)
        else:
            return (x > left_value) and (x < right_value)
    return _in_range


def has_violations():
    def _has_violations(x):
        total_vios = x.total_vios()
        return total_vios > 0
    return _has_violations


def has_no_violations():
    def _has_no_violations(x):
        total_vios = x.total_vios()
        return total_vios == 0
    return _has_no_violations
