#!/usr/bin/env python
# -*- coding: utf-8 -*-
# **************************************************************************
# Copyright © 2017 jianglin
# File Name: __init__.py
# Author: jianglin
# Email: xiyang0807@gmail.com
# Created: 2017-04-15 20:03:18 (CST)
# Last Update:星期四 2017-5-4 22:48:58 (CST)
#          By:
# Description:
# **************************************************************************
from .whoosh_backend import WhooshSearch
from .simple_backend import SimpleSearch
from .elasticsearch_backend import ElasticSearch
from .backends import BaseBackend


class Search(BaseBackend):
    def init_app(self, app):
        app.config.setdefault('MSEARCH_BACKEND', 'whoosh')
        msearch_backend = app.config['MSEARCH_BACKEND']
        if msearch_backend == 'simple':
            self = SimpleSearch(app, self.db, self.analyzer)
        elif msearch_backend == 'elasticsearch':
            self = ElasticSearch(app, self.db, self.analyzer)
        elif msearch_backend == 'whoosh':
            self = WhooshSearch(app, self.db, self.analyzer)
        else:
            raise ValueError('backends {} not exists.'.format(msearch_backend))
