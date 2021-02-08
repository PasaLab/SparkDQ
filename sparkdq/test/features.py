class A:
    def __init__(self, a, b):
        self.a = a
        self.b = b

    def __hash__(self):
        return hash(self.b)

    def __eq__(self, other):
        return (self.a == other.a) & (self.b == other.b)

a = {2: 2, 3: 4}
print(str(a))



# x = "2,3;4,6/5,8;9,10#4,5/2,7#4,0/2,0"
# total_violations = []
# for cfd_vios in x.split("#"):
#     total_violations.append([[(attr_vios.split(",")[0], attr_vios.split(",")[1])
#                               for attr_vios in pat_vios.split(";")]
#                              for pat_vios in cfd_vios.split("/")])
# print(total_violations)