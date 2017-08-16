#!/usr/bin/env python
# -*- coding: utf-8 -*-
# **************************************************************************
# Copyright © 2017 jianglin
# File Name: __init__.py
# Author: jianglin
# Email: xiyang0807@gmail.com
# Created: 2017-04-15 20:03:18 (CST)
# Last Update:星期三 2017-8-16 12:51:15 (CST)
#          By:
# Description:
# **************************************************************************
from .backends import BaseBackend


class Search(BaseBackend):
    def init_app(self, app):
        app.config.setdefault('MSEARCH_BACKEND', 'whoosh')
        msearch_backend = app.config['MSEARCH_BACKEND']
        if msearch_backend == 'simple':
            from .simple_backend import SimpleSearch
            self._backend = SimpleSearch(app, self.db, self.analyzer)
        elif msearch_backend == 'whoosh':
            from .whoosh_backend import WhooshSearch
            self._backend = WhooshSearch(app, self.db, self.analyzer)
        else:
            raise ValueError('backends {} not exists.'.format(msearch_backend))

    def __getattr__(self, name):
        return getattr(self._backend, name)
