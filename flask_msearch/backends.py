#!/usr/bin/env python
# -*- coding: utf-8 -*-
# **************************************************************************
# Copyright © 2017 jianglin
# File Name: backends.py
# Author: jianglin
# Email: xiyang0807@gmail.com
# Created: 2017-04-15 20:03:27 (CST)
# Last Update:星期六 2017-5-6 12:56:36 (CST)
#          By:
# Description:
# **************************************************************************
import logging
import sys

log_console = logging.StreamHandler(sys.stderr)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(log_console)


class BaseBackend(object):
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
        self.app = app
        if not self.db:
            self.db = self.app.extensions['sqlalchemy'].db
        self.db.Model.query_class = self._query_class(
            self.db.Model.query_class)

    def _query_class(self, q):
        _self = self

        class Query(q):
            def msearch(self, query, fields=None, limit=None, or_=False):
                model = self._mapper_zero().class_
                return _self.msearch(model, query, fields, limit, or_)

        return Query

    def msearch(self, m, query, fields=None, limit=None, or_=False):
        raise NotImplementedError
