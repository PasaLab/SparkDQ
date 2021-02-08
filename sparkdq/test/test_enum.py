# from enum import IntEnum
#
#
# class CheckStatus(IntEnum):
#     SUCCESS = 0
#     WARNING = 1
#     ERROR = 2


# x = [CheckStatus.ERROR, CheckStatus.WARNING]
# print(max(x))


# class MyException(Exception):
#     pass
#
#
# a = {1: 1, "2": 2}
# print(3 in a)
#
#
# def raise_exception():
#     raise MyException("test")
#
#
# try:
#     raise_exception()
# except Exception as e:
#     print(e)


import re


# fd = "1, 2 -> 3"
# pattern = "^.+->.+$"

# sides = fd.split("->")
# print(sides)
#
# lhs = sides[0].split(",")
# rhs = sides[1].split(",")
# print(lhs)
# print(rhs)

a = [1,2,3]
b = set([3, 2])
print(b.issubset(a))