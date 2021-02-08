from enum import IntEnum


class DataTypeInstances(IntEnum):
    """
    Supported data types for histogram analysis, unknown types include null values and other types
    """
    Unknown = 0
    Fractional = 1
    Integral = 2
    Boolean = 3
    String = 4
