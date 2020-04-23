#!/usr/bin/env python
# -*- coding: utf-8 -*-
# **************************************************************************
# Copyright © 2017-2020 jianglin
# File Name: backends.py
# Author: jianglin
# Email: mail@honmaple.com
# Created: 2017-04-15 20:03:27 (CST)
# Last Update: Monday 2020-03-09 16:49:20 (CST)
#          By:
# Description:
# **************************************************************************
import logging
import sys

from flask_sqlalchemy import models_committed, before_models_committed
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.inspection import inspect
from werkzeug.utils import import_string

from .signal import default_signal

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(sys.stderr))


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


class BaseSchema(object):
    def __init__(self, index):
        self.index = index

    def _fields(self):
        return dict()

    @property
    def fields(self):
        model = self.index.model
        schema_fields = self._fields()
        primary_keys = [key.name for key in inspect(model).primary_key]

        schema = getattr(model, "__msearch_schema__", dict())
        for field in self.index.searchable:
            if '.' in field:
                fields = field.split('.')
                field_attr = getattr(
                    getattr(model, fields[0]).property.mapper.class_,
                    fields[1])
            else:
                field_attr = getattr(model, field)

            if field in schema:
                field_type = schema[field]
                if isinstance(field_type, str):
                    schema_fields[field] = self.fields_map(field_type)
                else:
                    schema_fields[field] = field_type
                continue

            if hasattr(field_attr, 'descriptor') and isinstance(
                    field_attr.descriptor, hybrid_property):
                schema_fields[field] = self.fields_map("text")
                continue

            if field in primary_keys:
                schema_fields[field] = self.fields_map("primary")
                continue

            field_type = field_attr.property.columns[0].type
            schema_fields[field] = self.fields_map(field_type)
        return schema_fields


class BaseBackend(object):
    def __init__(self, app=None, db=None, analyzer=None):
        """
        You can custom analyzer by::

            from jieba.analyse import ChineseAnalyzer
            search = Search(analyzer = ChineseAnalyzer)
        """
        self._signal = None
        self._indexs = dict()
        self.db = db
        self.analyzer = analyzer
        if app is not None:
            self.init_app(app)

    def _setdefault(self, app):
        app.config.setdefault("MSEARCH_PRIMARY_KEY", "id")
        app.config.setdefault("MSEARCH_INDEX_NAME", "msearch")
        app.config.setdefault("MSEARCH_INDEX_SIGNAL", default_signal)
        app.config.setdefault("MSEARCH_ANALYZER", None)
        app.config.setdefault("MSEARCH_ENABLE", True)

    def _signal_connect(self, app):
        if app.config["MSEARCH_ENABLE"]:
            signal = app.config["MSEARCH_INDEX_SIGNAL"]
            if isinstance(signal, str):
                self._signal = import_string(signal)
            else:
                self._signal = signal
            before_models_committed.connect(self._before_models_committed)
            models_committed.connect(self.index_signal)

    def _before_models_committed(self, sender, changes):
        self.db.session.flush()
        for instance, operation in changes:
            if hasattr(instance, '__searchable__'):
                for field in getattr(instance, '__searchable__', []):
                    if '.' in field:
                        splits = field.split('.')
                        getattr(instance, splits[0])

    def index_signal(self, sender, changes):
        return self._signal(self, sender, changes)

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
        ix = self.index(model)
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

    def whoosh_search(self, m, query, fields=None, limit=None, or_=False):
        logger.warning(
            'whoosh_search has been replaced by msearch.please use msearch')
        return self.msearch(m, query, fields, limit, or_)

    # def msearch(self, m, query, fields=None, limit=None, or_=False):
    #     raise NotImplementedError
