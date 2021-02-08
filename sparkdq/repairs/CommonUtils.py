from enum import Enum


class ReplaceWays(Enum):
    # generic
    MODE = "mode"
    FORWARD = "forward"
    BACKWARD = "backward"
    FIXED = "fixed"

    # only numeric
    MAX = "max"
    MIN = "min"
    MEAN = "mean"
    MEDIAN = "median"


STRING_SUPPORTED_WAYS = [ReplaceWays.MODE.value, ReplaceWays.FORWARD.value,
                         ReplaceWays.BACKWARD.value, ReplaceWays.FIXED.value]
NUMERIC_SUPPORTED_WAYS = [ReplaceWays.MAX.value, ReplaceWays.MIN.value, ReplaceWays.MEAN.value,
                          ReplaceWays.MEDIAN.value, ReplaceWays.MODE.value, ReplaceWays.FORWARD.value,
                          ReplaceWays.BACKWARD.value, ReplaceWays.FIXED.value]
WAYS_NEED_PREPARE = [ReplaceWays.MODE.value, ReplaceWays.MAX.value, ReplaceWays.MIN.value,
                     ReplaceWays.MEAN.value, ReplaceWays.MEDIAN.value]


class KeepType(Enum):
    DEFAULT = "default"
    FIRST = "first"
    LAST = "last"
