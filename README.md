# SparkDQ

This is the code repository for the big data quality management paper titled 'SparkDQ: Efficient Generic Big Data Quality Management on Distributed Data-Parallel Computing'.

The project is based on Apache Spark, a unified analytics engine for big data. The Scala library for SparkDQ can be assemblied by maven.

The detailed setup and running methods are the same as an Apache Spark application.

# Prerequisites

- Git
- Java 1.8
- Hadoop 2.7.4
- Spark 2.2.0
- Python 3 with pip
- Anaconda 3
- Maven 3.5.4

When installing hadoop and spark, make sure that you have basic environment variables for them, such as `HADOOP_HOME`, `SPARK_HOME` and `PATH`.

To install SparkDQ, all nodes of your distributed cluster should be have an Anaconda environment. 

# Quick Start

The Scala library can be packaged with the command `mvn assembly:assembly`. Packaged JARs should be put in a Python application directory.

1. Clone source code
```
  git clone https://github.com/PasaLab/SparkDQ
```

2. Generate SparkDQ installer
```
  python setup.py sdist
```
> An distributed installer could be found in `dist` directory.

3. Deploy SparkDQ in all nodes' anaconda environment
```
  pip install sparkdq-1.0.tar.gz
```

4. Prepare SparkDQ applications

> SparkDQ presents a number of test applications in `test` directory. You can use them directly, or try to write your own application accordingly.

5. Test SparkDQ

> SparkDQ presents an interactive webpage to import SparkDQ applications and run it. Launch Jupyter notebook like this: 

```
  nohup $JUPYTER_PATH$ notebook > log.out 2>&1 &
```

> You can see detailed log in the directory where you run this command.

> After that, you can visit the webpage, like `slave001:8888`, and then import packages for tests.
> The default port is `8888`. You can change it according to your requirements.
