#!/usr/bin/env python
# -*- coding: utf-8 -*-
# **************************************************************************
# Copyright © 2017-2020 jianglin
# File Name: __init__.py
# Author: jianglin
# Email: mail@honmaple.com
# Created: 2017-04-15 20:03:18 (CST)
# Last Update: Friday 2021-06-04 10:14:13 (CST)
#          By:
# Description:
# **************************************************************************
from werkzeug.utils import import_string


class Search(object):
    def __init__(self, app=None, db=None, analyzer=None):
        """
        You can custom analyzer by::

            from jieba.analyse import ChineseAnalyzer
            search = Search(analyzer = ChineseAnalyzer)
        """
        self.db = db
        self.analyzer = analyzer
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        app.config.setdefault('MSEARCH_BACKEND', 'simple')
        msearch_backend = app.config['MSEARCH_BACKEND']
        if msearch_backend == 'simple':
            backend = import_string(
                "flask_msearch.simple_backend.SimpleSearch")
        elif msearch_backend == 'whoosh':
            backend = import_string(
                "flask_msearch.whoosh_backend.WhooshSearch")
        elif msearch_backend == 'elasticsearch':
            backend = import_string(
                "flask_msearch.elasticsearch_backend.ElasticSearch")
        else:
            raise ValueError('backends {} not exists.'.format(msearch_backend))
        self._backend = backend(app, self.db, self.analyzer)

    def __getattr__(self, name):
        if name == "_backend":
            raise AttributeError("The flask app has not been initialized")
        return getattr(self._backend, name)
