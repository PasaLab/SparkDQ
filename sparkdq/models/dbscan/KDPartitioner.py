from queue import Queue

import numpy as np

from sparkdq.models.dbscan.BoundingBox import BoundingBox

SUPPORTED_METHODS = ["min_var", "rotation"]


class KDPartitioner:

    def __init__(self, data, max_partitions=5, k=None, split_method="min_var"):
        """
        Parting data to several partitions similar in size
        :type data: RDD
        :param data: data formatted as (row_id, k-dim vector of Type np.array)
        :param max_partitions: the max partition size
        :param k: the dimension of data vector
        :param split_method: split method
        """
        self.split_method = split_method if split_method in SUPPORTED_METHODS else "min_var"
        self.k = k if k is not None else len(data.first()[1])
        self.max_partitions = max_partitions if max_partitions is not None else 4 ** self.k

        data.cache()
        box = data.aggregate(
            BoundingBox(self.k),
            lambda total, row: total.union(BoundingBox(row[1])),
            lambda box1, box2: box1.union(box2)
        )
        first_partition = data.map(lambda row: ((row[0], 0), row[1]))
        self._create_partitions(first_partition, box)

    def get_partitions(self):
        return self.partitions

    def get_bounding_boxes(self):
        return self.bounding_boxes

    def _create_partitions(self, data, box):
        """
        :type data: RDD
        :param data: data formatted as ((row_id, part_id), k-dim vector of Type np.array)
        :type box: BoundingBox
        :param box: BoundingBox for the whole data
        :return:
        """
        # 当前维度，当前维度下待分的分区、已分的分区
        current_axis = 0
        todo_q = Queue()
        todo_q.put(0)
        done_q = Queue()

        self.partitions = {0: data}
        self.bounding_boxes = {0: box}
        next_label = 1

        while next_label < self.max_partitions:
            if not todo_q.empty():
                # 当前的待分的分区号，当前的分区，当前的边界线
                current_label = todo_q.get()
                current_partition = self.partitions[current_label]
                if current_partition.isEmpty():
                    break
                current_box = self.bounding_boxes[current_label]
                # 将待分分区分成两块，生成一块新分区
                if self.split_method == 'min_var':
                    (part1, part2, median), current_axis = min_var_split(current_partition, self.k, next_label)
                else:
                    part1, part2, median = median_search_split(
                        current_partition,
                        current_axis,
                        next_label
                    )

                box1, box2 = current_box.split(current_axis, median)
                self.partitions[current_label] = part1
                self.partitions[next_label] = part2
                self.bounding_boxes[current_label] = box1
                self.bounding_boxes[next_label] = box2
                done_q.put(current_label)
                done_q.put(next_label)
                next_label += 1
            else:
                # 下一轮分区
                todo_q = done_q
                done_q = Queue()
                current_axis = (current_axis + 1) % self.k
        return self.partitions


def median_search_split(partition, axis, next_part):
    """
    Split the partition on the current axis into two mostly equal sized partitions
    :param partition: the partition to split
    :param axis: the axis on which to split
    :param next_part: the next partition id
    :return:
    """
    sorted_values = partition.map(lambda row: row[1][axis]).sortBy(lambda v: v).collect()
    median = sorted_values[len(sorted_values) / 2]
    part1 = partition.filter(lambda row: row[1][axis] < median)
    part2 = partition.filter(lambda row: row[1][axis] >= median).map(lambda row: ((row[0][0], next_part), row[1]))
    return part1, part2, median


def mean_var_split(partition, axis, next_label, mean, variance):
    """
    Search for the boundary to split the data into two mostly equal sized partitions
    :param partition: the partition to split
    :param axis: the current axis to split
    :param next_label: the next partition id
    :param mean: mean of the axis
    :param variance: variance of the axis
    :return:
    """
    std_dev = np.sqrt(variance)
    bounds = np.array([mean + (i - 3) * 0.3 * std_dev for i in range(7)])
    counts = partition.aggregate(
        np.zeros(7),
        lambda x, row: x + 2 * (row[1][axis] < bounds) - 1,
        lambda x, y: x + y
    )
    counts = np.abs(counts)
    boundary = bounds[np.argmin(counts)]
    part1 = partition.filter(lambda row: row[1][axis] < boundary)
    part2 = partition.filter(lambda row: row[1][axis] >= boundary)\
        .map(lambda row: ((row[0][0], next_label), row[1]))
    return part1, part2, boundary


def min_var_split(partition, k, next_label):
    """
    Split the given partition into equal sized partitions along the axis with the greatest variance
    :type partition: RDD
    :param partition: the partition to split
    :param k: the data dimension
    :param next_label: the next partition id
    :return:
    """
    moments = partition.aggregate(
        np.zeros((3, k)),
        lambda x, row: x + np.array([np.ones(k), row[1], row[1] ** 2]),
        lambda x, y: x + y
    )
    means = moments[1] / moments[0]
    variances = moments[2] / moments[0] - means ** 2
    axis = np.argmax(variances)
    return mean_var_split(partition, axis, next_label, means[axis], variances[axis]), axis
