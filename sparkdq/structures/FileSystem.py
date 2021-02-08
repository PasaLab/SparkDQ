from enum import IntEnum


class FileSystem(IntEnum):
    """
    File Systems supported for storing metrics and temporary results.
    """
    LOCAL = 0
    HDFS = 1
