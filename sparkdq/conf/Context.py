import os
from os import path

from bigdl.util.common import create_spark_conf
from pyspark import SparkContext
from pyspark.sql.session import SparkSession

from sparkdq.conf.Config import Configuration
from sparkdq.utils.Singleton import singleton


@singleton
class Context:

    sc = None
    spark = None

    def __init__(self):
        self._conf = Configuration()
        self._mode = "local"
        self._sc = None
        self._spark = None

    def init_spark(self, mode="local", threads=1):
        self._mode = mode.lower()
        config_map = self._conf.get_config
        jar_file = path.join(path.dirname(path.dirname(path.abspath(__file__))), "jars", config_map["jars.name"])
        if self._mode == "local":
            spark_conf = create_spark_conf()\
                .setAppName("test")\
                .setMaster("local[{}]".format(threads)) \
                .set("spark.jars", jar_file)
            self._sc = SparkContext(conf=spark_conf)
            self._spark = SparkSession(self._sc)
            # Context.__spark = SparkSession.builder \
            #     .appName("test") \
            #     .master("local") \
            #     .config(key="spark.jars", value="/Users/qiyang/project/dqlib/target/dqlib-1.0.jar") \
            #     .getOrCreate()
        elif self._mode == "yarn":
            os.environ["JAVA_HOME"] = config_map['java.home']
            os.environ["HADOOP_HOME"] = config_map['hadoop.home']
            os.environ["YARN_CONF_DIR"] = config_map['yarn.conf.dir']
            os.environ["HADOOP_CONF_DIR"] = config_map['hadoop.conf.dir']
            os.environ["PYSPARK_PYTHON"] = config_map['spark.pyspark.python']

            self._spark = SparkSession.builder \
                .appName(config_map['app.name']) \
                .master('yarn') \
                .config(key="spark.yarn.queue", value=config_map['spark.yarn.queue']) \
                .config(key="spark.executor.cores", value=int(config_map['spark.executor.cores'])) \
                .config(key="spark.executor.instances", value=int(config_map['spark.executor.instances'])) \
                .config(key="spark.executor.memory", value=config_map['spark.executor.memory']) \
                .config(key="spark.yarn.am.memory", value=config_map['spark.yarn.am.memory']) \
                .config(key="spark.jars", value=jar_file) \
                .enableHiveSupport() \
                .getOrCreate()

    def restart_spark(self, mode=None):
        if self._spark is not None:
            self._spark.stop()
        if mode is None:
            mode = self._mode
        self.init_spark(mode=mode)

    @property
    def spark(self):
        return self._spark

    @property
    def context(self):
        return self._sc

    @property
    def config(self):
        return self._conf
