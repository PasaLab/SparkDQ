def is_sub_dict(parent_dict, child_dict):
    for k, v in child_dict.items():
        if (k not in parent_dict) or (parent_dict[k] != v):
            return False
    return True


def list_equal(list1, list2):
    if len(list1) != len(list2):
        return False
    sorted_list1 = sorted(list1)
    sorted_list2 = sorted(list2)
    for i in range(list1):
        if sorted_list1[i] != sorted_list2[i]:
            return False
    return True
