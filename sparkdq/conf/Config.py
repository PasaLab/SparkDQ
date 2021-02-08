import os
from os import path
import configparser


class Configuration:

    def __init__(self):
        self._config_map = {}
        config_file = path.join(path.dirname(path.dirname(path.abspath(__file__))), "conf", "SparkDQ.ini")
        if os.path.exists(config_file):
            self._config = configparser.ConfigParser()
            self._config.read(config_file, encoding="utf-8")
            for primary_key in self._config:
                for key in self._config[primary_key]:
                    self._config_map[key] = self._config[primary_key][key]
        else:
            self._config_map['app.name'] = 'SparkDQ'
            self._config_map['spark.executor.instances'] = 3
            self._config_map['spark.executor.cores'] = 4
            self._config_map['spark.executor.memory'] = '4g'
            self._config_map['spark.yarn.am.memory'] = '1g'
            self._config_map['spark.yarn.queue'] = 'pasa'
            self._config_map['spark.pyspark.python'] = '/home/experiment/anaconda3/bin/python'
            self._config_map['java.home'] = '/usr/java/latest'
            self._config_map['hadoop.home'] = '/opt/cloudera/parcels/CDH/lib/hadoop'
            self._config_map['hadoop.conf.dir'] = '/opt/cloudera/parcels/CDH/lib/hadoop/etc/hadoop'
            self._config_map['yarn.conf.dir'] = '/opt/cloudera/parcels/CDH/lib/hadoop/etc/hadoop'

    @property
    def get_config(self):
        return self._config_map

    def set_app_name(self, app_name):
        assert isinstance(app_name, str), 'app name is not a string'
        self._config_map['app.name'] = app_name

    def set_executor_instances(self, instances_num):
        assert isinstance(instances_num, int), 'executor instances is not a integer'
        self._config_map['spark.executor.instances'] = instances_num

    def set_executor_memory(self, mem_size):
        assert isinstance(mem_size, str), 'executor memory is not a string'
        self._config_map['spark.executor.memory'] = mem_size

    def set_yarn_am_memory(self, mem_size):
        assert isinstance(mem_size, str), 'yarn am memory is not a string'
        self._config_map['spark.yarn.am.memory'] = mem_size

    def set_executor_cores(self, cores_num):
        assert isinstance(cores_num, int), 'executor cores is not a integer'
        self._config_map['spark.executor.cores'] = cores_num

    def set_yarn_queue(self, yarn_queue):
        assert isinstance(yarn_queue, str), 'yarn queue is not a string'
        self._config_map['spark.yarn.queue'] = yarn_queue

    def set_java_home(self, java_home):
        assert isinstance(java_home, str), 'java home is not a string'
        self._config_map['java.home'] = java_home

    def set_pyspark_python(self, pyspark_python):
        assert isinstance(pyspark_python, str), 'pyspark python is not a string'
        self._config_map['spark.pyspark.python'] = pyspark_python

    def set_hadoop_conf_dir(self, hadoop_conf_dir):
        assert isinstance(hadoop_conf_dir, str), 'hadoop conf dir is not a string'
        self._config_map['hadoop.conf.dir'] = hadoop_conf_dir

    def set_yarn_conf_dir(self, yarn_conf_dir):
        assert isinstance(yarn_conf_dir, str), 'yarn conf dir is not a string'
        self._config_map['yarn.conf.dir'] = yarn_conf_dir
