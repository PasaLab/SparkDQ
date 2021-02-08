from sparkdq.conf.Context import Context
from sparkdq.utils.HBaseHelper import row_transform


def from_text(file_path):
    return Context().spark.read.text(file_path)


def from_csv(file_path, schema=None, sep=None, header=None, infer_schema=None):
    return Context().spark.read.csv(file_path, schema=schema, sep=sep, header=header, inferSchema=infer_schema)


def from_orc(file_path):
    return Context().spark.read.orc(file_path)


def from_json(file_path, schema=None):
    return Context().spark.read.csv(file_path, schema=schema)


def from_parquet(file_path):
    return Context().spark.read.parquet(file_path)


def from_pandas(pdf):
    return Context().spark.createDataFrame(pdf)


def from_jdbc(url, table, properties=None):
    return Context().spark.read.jdbc(url=url, table=table, properties=properties)


def from_hbase(table, columns):
    """
    :param table: table name
    :param columns: column name with family name such as "family:column"
    :return:
    """
    hbase_host = Context().config.get_config["hbase.host"]
    conf = {
        "hbase.zookeeper.quorum": hbase_host,
        "hbase.mapreduce.inputtable": table
    }
    keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"
    hbase_rdd = Context().sc.newAPIHadoopRDD("org.apache.hadoop.hbase.mapreduce.TableInputFormat",
                                             "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
                                             "org.apache.hadoop.hbase.client.Result",
                                             keyConverter=keyConv,
                                             valueConverter=valueConv,
                                             conf=conf)
    rdd_with_data = hbase_rdd.map(lambda x: (x[0], x[1].split('\n')))
    rdd_with_columns = rdd_with_data.map(lambda x: (x[0], row_transform(x[1], columns)))
    rdd_data = rdd_with_columns.map(lambda x: [x[0]] + [x[1][i] for i in x[1]])
    df_data = Context().spark.createDataFrame(rdd_data, ["row_key"] + columns)
    return df_data


def from_hive(database, table):
    sql_str = "select * from " + database + "." + table
    return Context().spark.sql(sql_str)
