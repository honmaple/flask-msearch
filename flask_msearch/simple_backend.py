#!/usr/bin/env python
# -*- coding: utf-8 -*-
# **************************************************************************
# Copyright © 2017 jianglin
# File Name: simple_backend.py
# Author: jianglin
# Email: xiyang0807@gmail.com
# Created: 2017-04-22 10:23:24 (CST)
# Last Update:星期六 2017-5-6 12:56:29 (CST)
#          By:
# Description:
# **************************************************************************
from sqlalchemy import or_ as _or
from sqlalchemy import and_ as _and
from .backends import BaseBackend


class SimpleSearch(BaseBackend):
    def msearch(self, m, query, fields=None, limit=None, or_=False):
        if fields is None:
            fields = m.__searchable__
        f = []
        if self.analyzer is not None:
            keywords = self.analyzer(query)
        else:
            keywords = query.split(' ')
        for field in fields:
            query = [getattr(m, field).contains(keyword)
                     for keyword in keywords if keyword]
            if not or_:
                f.append(_and(*query))
            else:
                f.append(_or(*query))
        results = m.query.filter(_or(*f))
        if limit is not None:
            results = results.limit(limit)
        return results
