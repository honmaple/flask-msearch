#!/usr/bin/env python
# -*- coding: utf-8 -*-
# **************************************************************************
# Copyright © 2017 jianglin
# File Name: __init__.py
# Author: jianglin
# Email: xiyang0807@gmail.com
# Created: 2017-04-15 20:03:18 (CST)
# Last Update:星期日 2018-01-07 01:33:46 (CST)
#          By:
# Description:
# **************************************************************************
from .simple_backend import SimpleSearch

try:
    from .whoosh_backend import WhooshSearch
except ImportError:
    WhooshSearch = None

try:
    from .elasticsearch_backend import ElasticSearch
except ImportError:
    ElasticSearch = None


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
        app.config.setdefault('MSEARCH_BACKEND', 'whoosh')
        msearch_backend = app.config['MSEARCH_BACKEND']
        if msearch_backend == 'simple':
            self._backend = SimpleSearch(app, self.db, self.analyzer)
        elif msearch_backend == 'whoosh':
            self._backend = WhooshSearch(app, self.db, self.analyzer)
        elif msearch_backend == 'elasticsearch':
            self._backend = ElasticSearch(app, self.db, self.analyzer)
        else:
            raise ValueError('backends {} not exists.'.format(msearch_backend))

    def __getattr__(self, name):
        return getattr(self._backend, name)
