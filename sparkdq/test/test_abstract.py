class A:

    def __str__(self):
        return "22"

class B(A):

    pass


b = B()
print(b)