class Violation:
    """
    Violations for one rhs in one pattern, including constant violation and variable violation
    """
    def __init__(self, const_vio, var_vio):
        self.const_vio = const_vio
        self.var_vio = var_vio

    def __str__(self):
        return "ConstVio: {}, VarVio: {}".format(self.const_vio, self.var_vio)

    def to_json(self):
        return {
            "ConstVio": self.const_vio,
            "VarVio": self.var_vio
        }

    @staticmethod
    def from_json(d):
        return Violation(d["ConstVio"], d["VarVio"])
