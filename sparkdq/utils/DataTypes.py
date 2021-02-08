def is_number_type(type_str):
    numerical_types = ["DoubleType", "FloatType", "IntegerType", "LongType", "ShortType"]
    return type_str in numerical_types
