#!/usr/bin/env python
# -*- coding: utf-8 -*-
# **************************************************************************
# Copyright © 2017 jianglin
# File Name: setup.py
# Author: jianglin
# Email: xiyang0807@gmail.com
# Created: 2017-04-16 15:18:54 (CST)
# Last Update:星期四 2017-4-20 16:58:54 (CST)
#          By:
# Description:
# **************************************************************************
from setuptools import setup


setup(
    name='flask-msearch',
    version='0.1.1',
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
