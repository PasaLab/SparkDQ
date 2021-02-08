from collections import defaultdict
import sys


class ClusterAggregator:

    def __init__(self):
        """
        Forward is partition-level cluster id to global-level cluster id
        Reverse is global-level cluster id to partition-level cluster ids
        Next global id is the global cluster id
        """
        self.forward = defaultdict(default_value)
        self.reverse = defaultdict(set)
        self.next_global_id = 0

    def __add__(self, other):
        """
        Add another ClusterAggregator or row of data formatted as (row_id, list of pairs as (part_id, label))
        :param other:
        :return:
        """
        if type(other) == ClusterAggregator:
            for item in other.reverse.items():
                self + item
        else:
            row_id, part_labels = other
            new_pls = set(part_labels)
            first_pl = next(iter(new_pls))
            if first_pl[1] != -1:
                global_id = self.next_global_id
                for new_pl in new_pls:
                    if new_pl in self.forward:
                        global_id = min(global_id, self.forward[new_pl])
                if global_id == self.next_global_id:
                    self.next_global_id += 1
                else:
                    overlaps = [self.forward[new_pl] for new_pl in new_pls if new_pl in self.forward]
                    for gl_id in overlaps:
                        if gl_id != global_id:
                            for pl in self.reverse[gl_id]:
                                self[pl] = global_id
                            del self.reverse[gl_id]
                for new_pl in new_pls:
                    self[new_pl] = global_id
        return self

    def __setitem__(self, key, value):
        """
        Set forward and reverse dict
        :param key: key is part_label meaning the cluster id in a partition
        :param value: value is the global cluster id
        :return:
        """
        self.forward[key] = value
        self.reverse[value].add(key)


def default_value():
    return sys.maxsize


if __name__ == "__main__":
    t1 = ClusterAggregator()
    t1 += (0, ((0, 0), (1, 1)))
    t1 += (1, ((1, 1), (2, 0)))
    t1 += (2, ((3, 1),))
    t1 += (3, ((4, 1),))
    # should be {0: {(2, 0), (0, 0), (1, 1)}, 1: {(3, 1)}, 2: {(4, 1)}}
    print(t1.reverse)

    t2 = ClusterAggregator()
    t2 += (0, ((4, 1), (3, 1)))
    t2 += (1, ((5, 0), (0, 0)))
    # should be {0: {(3, 1), (4, 1)}, 1: {(0, 0), (5, 0)}}
    print(t2.reverse)

    t1 += t2
    # should be {0: {(2, 0), (0, 0), (1, 1), (5, 0)}, 1: {(3, 1), (4, 1)}}
    print(t1.reverse)
