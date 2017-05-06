#!/usr/bin/env python
# -*- coding: utf-8 -*-
# **************************************************************************
# Copyright © 2017 jianglin
# File Name: setup.py
# Author: jianglin
# Email: xiyang0807@gmail.com
# Created: 2017-04-16 15:18:54 (CST)
# Last Update:星期六 2017-5-6 13:46:30 (CST)
#          By:
# Description:
# **************************************************************************
from setuptools import setup


setup(
    name='flask-msearch',
    version='0.1.3',
    url='https://github.com/honmaple/flask-msearch',
    license='BSD',
    author='honmaple',
    author_email='xiyang0807@gmail.com',
    description='full text search with whoosh for flask',
    long_description='Please visit https://github.com/honmaple/flask-msearch',
    packages=['flask_msearch'],
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    install_requires=[
        'Flask',
    ],
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
