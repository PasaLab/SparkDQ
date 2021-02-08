def get_class_by_name(module_name, class_name):
    """
    Get the class by the class name(not the class object)
    :param module_name: the module name
    :param class_name: the class name
    :return:
    """
    module_meta = __import__(module_name, globals(), locals(), [class_name])
    class_meta = getattr(module_meta, class_name)
    return class_meta
