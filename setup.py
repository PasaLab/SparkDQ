#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

packages = ['sparkdq']

setup(
    name='sparkdq',  # 项目名称
    version=1.0,  # 版本
    author='qiyang',
    author_email='qy1295143818@gmail.com',
    description="Spark data quality detection and repair system",
    license='',
    url='github.com//xxx',
    packages=find_packages(),  # 项目根目录下需要打包的目录
    include_package_data=True,
    install_requires=[  # 依赖的包
        'pyspark == 2.2.0',
        'hdfs >= 2.1.0',
        'py4j == 0.10.4'
    ],
    scripts=[],
)
