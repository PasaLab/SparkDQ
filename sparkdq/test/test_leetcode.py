class A:
    a = 2
    b = 3


if __name__ == "__main__":
    a = A()
    print(a.__getattribute__("a"))
