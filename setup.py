#!/usr/bin/env python
# -*- coding: utf-8 -*-
# **************************************************************************
# Copyright © 2017-2020 jianglin
# File Name: setup.py
# Author: jianglin
# Email: mail@honmaple.com
# Created: 2017-04-16 15:18:54 (CST)
# Last Update: Friday 2021-06-04 10:14:45 (CST)
#          By:
# Description:
# **************************************************************************
from setuptools import setup

setup(
    name='flask-msearch',
    version='0.2.9.2',
    url='https://github.com/honmaple/flask-msearch',
    license='BSD',
    author='honmaple',
    author_email='mail@honmaple.com',
    description='full text search with whoosh for flask',
    long_description='Please visit https://github.com/honmaple/flask-msearch',
    packages=['flask_msearch'],
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    install_requires=['Flask', 'Flask-SQLAlchemy'],
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ])
