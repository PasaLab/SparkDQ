if __name__ == "__main__":
    import sys

    a = 100
    b = True
    d = 1.1
    e = ""
    f = []
    g = ()
    h = {}
    i = set([])

    print(" %s size is %d " % (type(a), sys.getsizeof(a)))
    print(" %s size is %d " % (type(b), sys.getsizeof(b)))
    print(" %s size is %d " % (type(d), sys.getsizeof(d)))
    print(" %s size is %d " % (type(e), sys.getsizeof(e)))
    print(" %s size is %d " % (type(f), sys.getsizeof(f)))
    print(" %s size is %d " % (type(g), sys.getsizeof(g)))
    print(" %s size is %d " % (type(h), sys.getsizeof(h)))
    print(" %s size is %d " % (type(i), sys.getsizeof(i)))