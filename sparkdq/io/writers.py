from sparkdq.conf.Context import Context


def to_text(df, file_path):
    df.write.text(file_path)


def to_csv(df, file_path, sep=None, header=None):
    df.write.csv(file_path, sep=sep, header=header)


def to_orc(df, file_path):
    df.write.orc(file_path)


def to_json(df, file_path):
    df.write.json(file_path)


def to_parquet(df, file_path):
    df.write.parquet(file_path)


def to_pandas(df):
    df.toPandas()


def to_jdbc(df, url, table, properties=None):
    df.write.jdbc(url=url, table=table, properties=properties)


def to_hbase(df, table, row_key="row_key"):
    """
    Column names must be as "family:column", must contain row_key
    :param table: table name
    :param row_key: default row key column
    :return:
    """
    hbase_host = Context().config.get_config["hbase.host"]
    conf = {"hbase.zookeeper.quorum": hbase_host,
            "hbase.mapred.outputtable": table,
            "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
            "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
    keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"
    data_columns = df.columns
    data_columns.remove(row_key)
    df.rdd.flatMap(
        lambda row: [(row[row_key], [row[row_key], c.split(":")[0], c.split(":")[1], row[c]]) for c in data_columns]
    ).saveAsNewAPIHadoopDataset(conf=conf, keyConverter=keyConv, valueConverter=valueConv)


def to_hive(df, database, table):
    df.write.format("hive").mode("overwrite").saveAsTable("{}.{}".format(database, table))
