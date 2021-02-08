from operator import add

import numpy as np
from pyspark.sql.types import StructField, StructType, IntegerType
from scipy.spatial.distance import euclidean
import sklearn.cluster as skc

from sparkdq.conf.Context import Context
from sparkdq.models.CommonUtils import DEFAULT_CLUSTER_COL, DEFAULT_INDEX_COL
from sparkdq.models.dbscan.ClusterAggregator import ClusterAggregator
from sparkdq.models.dbscan.KDPartitioner import KDPartitioner


class DBSCAN:

    def __init__(self, eps=0.5, min_pts=5, dist_type="euclidean", max_partitions=5, prediction_col=DEFAULT_CLUSTER_COL):
        self._eps = eps
        self._min_pts = min_pts
        self._dist_type = dist_type
        self._max_partitions = max_partitions
        self._prediction_col = prediction_col

    def set_params(self, eps=0.5, min_pts=5, dist_type="euclidean", max_partitions=5,
                   prediction_col=DEFAULT_CLUSTER_COL):
        self._eps = eps
        self._min_pts = min_pts
        self._dist_type = dist_type
        self._max_partitions = max_partitions
        self._prediction_col = prediction_col

    def transform(self, data, columns, index_col=DEFAULT_INDEX_COL):
        total_columns = [index_col] + columns
        index_type = data.schema[index_col]

        rdd = data.select(*total_columns).rdd.map(lambda row: (row[0], np.array(row[1:])))
        partitioner = KDPartitioner(rdd, max_partitions=self._max_partitions)
        bounding_boxes = partitioner.get_bounding_boxes()
        expanded_boxes = {}

        # create neighbors
        neighbors = {}
        new_data = rdd.context.emptyRDD()
        for label, box in bounding_boxes.items():
            expanded_box = box.expand(2 * self._eps)
            expanded_boxes[label] = expanded_box
            neighbors[label] = rdd.filter(lambda row: expanded_box.contains(row[1])) \
                .map(lambda row: ((row[0], label), row[1]))
            new_data = new_data.union(neighbors[label])
        rdd = new_data

        rdd = rdd.map(lambda row: (row[0][1], (row[0][0], row[1])))\
            .partitionBy(len(partitioner.get_partitions()))\
            .map(lambda row: ((row[1][0], row[0]), row[1][1]))

        if self._dist_type == "euclidean":
            params = {"eps": self._eps, "min_samples": self._min_pts, "metric": euclidean}
        else:
            raise Exception("unsupported metric type {}".format(self._dist_type))

        rdd = rdd.mapPartitions(lambda iterable: dbscan_partition(iterable, params))

        # remap cluster ids
        labeled_points = rdd.groupByKey()
        labeled_points.cache()
        mapper = labeled_points.aggregate(ClusterAggregator(), add, add)
        bc_forward_mapper = rdd.context.broadcast(mapper.forward)
        rdd = labeled_points.map(lambda x: map_cluster_id(x, bc_forward_mapper)).sortByKey()

        # convert rdd to df
        tmp_schema = StructType([
            index_type,
            StructField(DEFAULT_CLUSTER_COL, IntegerType(), False)
        ])
        tmp_df = Context().spark.createDataFrame(rdd, tmp_schema)
        return data.join(tmp_df, on=index_col, how="inner")


def dbscan_partition(iterable, params):
    """
    Perform a DBSCAN on a given partition
    :param iterable:
    :param params:
    :return:
    """
    data = []
    for x in iterable:
        data.append(x)
    if len(data) > 0:
        x = np.array([row[1] for row in data])
        parts = [row[0][1] for row in data]
        y = np.array([row[0][0] for row in data])
        model = skc.DBSCAN(**params)
        c = model.fit_predict(x)
        for i in range(len(c)):
            yield (y[i], (parts[i], c[i]))


def map_cluster_id(row_id_labels, bc_forward_mapper):
    row_id = int(row_id_labels[0])
    labels = []
    for label in row_id_labels[1]:
        labels.append(label)
    cluster_id = next(iter(labels))
    cluster_dict = bc_forward_mapper.value
    if (cluster_id[1] != -1) and (cluster_id in cluster_dict):
        return row_id, int(cluster_dict[cluster_id])
    else:
        return row_id, -1


if __name__ == "__main__":
    pass
    # spark = Context().spark
    # rdd = spark.sparkContext.parallelize([
    #     (1, "A", 19, 181, 67),
    #     (2, "C", 17, 179, 67),
    #     (3, 'E', 18, 180, 68),
    #     (4, 'E', 29, 180, 68),
    #     (5, 'E', 18, 180, 68),
    #     (6, 'E', 18, 180, 68),
    #     (7, 'E', 18, 180, 68),
    #     (8, 'E', 18, -180, 68),
    #     (9, 'F', 28, 21, 7),
    #     (10, 'F', 28, 22, 8),
    #     (11, 'F', 28, 22, 8),
    #     (12, 'F', 28, 22, 8),
    #     (13, 'F', 28, 22, 8),
    #     (14, 'F', 28, 23, 7),
    # ])
    # from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType
    #
    # schema = StructType([
    #     StructField("id", LongType(), True),
    #     StructField("name", StringType(), True),
    #     StructField("age", LongType(), True),
    #     StructField("height", IntegerType(), True),
    #     StructField("weight", IntegerType(), True)
    # ])
    # df = spark.createDataFrame(rdd, schema)
    #
    # db = DBSCAN(max_partitions=3)
    # db.fit(df, ["height", "weight"], "id")
    # print(db.detect())
