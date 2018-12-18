#!/usr/bin/env python
# -*- coding: utf-8 -*-
# **************************************************************************
# Copyright © 2017 jianglin
# File Name: backends.py
# Author: jianglin
# Email: mail@honmaple.com
# Created: 2017-04-15 20:03:27 (CST)
# Last Update: Monday 2018-12-17 10:27:38 (CST)
#          By:
# Description:
# **************************************************************************
import logging
import sys

log_console = logging.StreamHandler(sys.stderr)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(log_console)


def relation_column(instance, fields):
    '''
    such as: user.username
    such as: replies.content
    '''
    relation = getattr(instance.__class__, fields[0]).property
    _field = getattr(instance, fields[0])
    if relation.lazy == 'dynamic':
        _field = _field.first()
    return getattr(_field, fields[1]) if _field else ''


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

    def create_index(self,
                     model='__all__',
                     update=False,
                     delete=False,
                     yield_per=100):
        if model == '__all__':
            return self.create_all_index(update, delete)
        ix = self._index(model)
        instances = model.query.enable_eagerloads(False).yield_per(yield_per)
        for instance in instances:
            self.create_one_index(instance, update, delete, False)
        ix.commit()
        return ix

    def create_all_index(self, update=False, delete=False, yield_per=100):
        all_models = self.db.Model._decl_class_registry.values()
        models = [i for i in all_models if hasattr(i, '__searchable__')]
        ixs = []
        for m in models:
            ix = self.create_index(m, update, delete, yield_per)
            ixs.append(ix)
        return ixs

    def update_one_index(self, instance, commit=True):
        return self.create_one_index(instance, update=True, commit=commit)

    def delete_one_index(self, instance, commit=True):
        return self.delete_one_index(instance, delete=True, commit=commit)

    def update_all_index(self, yield_per=100):
        return self.create_all_index(update=True, yield_per=yield_per)

    def delete_all_index(self, yield_per=100):
        return self.create_all_index(delete=True, yield_per=yield_per)

    def update_index(self, model='__all__', yield_per=100):
        return self.create_index(model, update=True, yield_per=yield_per)

    def delete_index(self, model='__all__', yield_per=100):
        return self.create_index(model, delete=True, yield_per=yield_per)

    def _index_signal(self, sender, changes):
        for change in changes:
            instance = change[0]
            operation = change[1]
            if hasattr(instance, '__searchable__'):
                if operation == 'insert':
                    self.create_one_index(instance)
                elif operation == 'update':
                    self.create_one_index(instance, update=True)
                elif operation == 'delete':
                    self.create_one_index(instance, delete=True)

            prepare = [i for i in dir(instance) if i.startswith('msearch_')]
            for p in prepare:
                if operation == 'delete':
                    attrs = getattr(instance, p)(delete=True)
                else:
                    attrs = getattr(instance, p)()
                ix = self._index(attrs.pop('_index'))
                if attrs['attrs']:
                    for attr in attrs['attrs']:
                        ix.update(**self._fields(attr))
                    ix.commit()

    def whoosh_search(self, m, query, fields=None, limit=None, or_=False):
        logger.warning(
            'whoosh_search has been replaced by msearch.please use msearch')
        return self.msearch(m, query, fields, limit, or_)

    # def msearch(self, m, query, fields=None, limit=None, or_=False):
    #     raise NotImplementedError
