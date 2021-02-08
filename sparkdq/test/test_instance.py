from abc import abstractmethod


class A:
    @abstractmethod
    def a(self):
        pass

    def b(self):
        self.a()


class B(A):
    @abstractmethod
    def a(self):
        pass


class C(A):
    def a(self):
        print("a")


class MyException(Exception):
    pass


# c = C()
# c.b()
# exp = MyException("hello")
# print(exp)
# print(c)

# a = (1, 2)
# print(list(a))

