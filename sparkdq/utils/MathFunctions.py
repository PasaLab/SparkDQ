import ctypes
import math


def unsigned_right_shift(n, i):
    if n < 0:
        n = ctypes.c_uint32(n).value
    if i < 0:
        return -int_overflow(n << abs(i))
    return int_overflow(n >> i)


def int_overflow(val):
    maxint = 2147483647
    if not -maxint-1 <= val <= maxint:
        val = (val + (maxint + 1)) % (2 * (maxint + 1)) - maxint - 1
    return val


def binary_search(arr, from_idx, to_idx, key):
    low = from_idx
    high = to_idx - 1
    while low <= high:
        mid = unsigned_right_shift(low + high, 1)
        mid_val = arr[mid]
        if math.isclose(mid_val, key):
            return low
        elif mid_val > key:
            high = low - 1
        else:
            low = mid + 1
    return -(low + 1)

