#!/usr/bin/env python
# -*- coding: utf-8 -*-
# **************************************************************************
# Copyright © 2017-2020 jianglin
# File Name: whoosh_backend.py
# Author: jianglin
# Email: mail@honmaple.com
# Created: 2017-04-15 20:03:27 (CST)
# Last Update: Monday 2021-03-22 23:06:13 (CST)
#          By:
# Description:
# **************************************************************************
import os
import sys

import sqlalchemy
from sqlalchemy import types
from whoosh import index as whoosh_index
from whoosh.analysis import StemmingAnalyzer
from whoosh.fields import BOOLEAN, DATETIME, ID, NUMERIC, TEXT
from whoosh.fields import Schema as _Schema
from whoosh.qparser import AndGroup, MultifieldParser, OrGroup

from .backends import BaseBackend, BaseSchema, relation_column, get_mapper

DEFAULT_ANALYZER = StemmingAnalyzer()

if sys.version_info[0] < 3:
    str = unicode


class Schema(BaseSchema):
    def __init__(self, index):
        self.index = index
        self.pk = index.pk
        self.analyzer = index.analyzer
        self.schema = _Schema(**self.fields)

    def fields_map(self, field_type):
        if field_type == "primary":
            return ID(stored=True, unique=True)
        type_map = {
            'date': types.Date,
            'datetime': types.DateTime,
            'boolean': types.Boolean,
            'integer': types.Integer,
            'float': types.Float
        }
        if isinstance(field_type, str):
            field_type = type_map.get(field_type, types.Text)

        if not isinstance(field_type, type):
            field_type = field_type.__class__

        if issubclass(field_type, (types.DateTime, types.Date)):
            return DATETIME(stored=True, sortable=True)
        elif issubclass(field_type, types.Integer):
            return NUMERIC(stored=True, numtype=int)
        elif issubclass(field_type, types.Float):
            return NUMERIC(stored=True, numtype=float)
        elif issubclass(field_type, types.Boolean):
            return BOOLEAN(stored=True)
        return TEXT(stored=True, analyzer=self.analyzer, sortable=False)

    def _fields(self):
        return {self.pk: ID(stored=True, unique=True)}


class Index(object):
    def __init__(self, model, name, pk, analyzer, path=""):
        self.model = model
        self.path = path
        self.name = getattr(
            model,
            "__msearch_index__",
            name,
        )
        self.pk = getattr(
            model,
            "__msearch_primary_key__",
            pk,
        )
        self.analyzer = getattr(
            model,
            "__msearch_analyzer__",
            analyzer,
        )
        self.searchable = set(
            getattr(
                model,
                "__msearch__",
                getattr(model, "__searchable__", []),
            ))
        self._schema = Schema(self)
        self._writer = None
        self._client = self.init()

    def init(self):
        ix_path = os.path.join(self.path, self.name)
        if whoosh_index.exists_in(ix_path):
            return whoosh_index.open_dir(ix_path)
        if not os.path.exists(ix_path):
            os.makedirs(ix_path)
        return whoosh_index.create_in(ix_path, self.schema)

    @property
    def index(self):
        return self

    @property
    def fields(self):
        return self.schema.names()

    @property
    def schema(self):
        return self._schema.schema

    def create(self, *args, **kwargs):
        if self._writer is None:
            self._writer = self._client.writer()
        return self._writer.add_document(**kwargs)

    def update(self, *args, **kwargs):
        if self._writer is None:
            self._writer = self._client.writer()
        return self._writer.update_document(**kwargs)

    def delete(self, *args, **kwargs):
        if self._writer is None:
            self._writer = self._client.writer()
        return self._writer.delete_by_term(**kwargs)

    def commit(self):
        if self._writer is None:
            self._writer = self._client.writer()
        r = self._writer.commit()
        self._writer = None
        return r

    def search(self, *args, **kwargs):
        return self._client.searcher().search(*args, **kwargs)


class WhooshSearch(BaseBackend):
    def init_app(self, app):
        self._setdefault(app)
        self._signal_connect(app)
        if self.analyzer is None:
            self.analyzer = app.config["MSEARCH_ANALYZER"] or DEFAULT_ANALYZER
        self.pk = app.config["MSEARCH_PRIMARY_KEY"]
        self.index_name = app.config["MSEARCH_INDEX_NAME"]
        super(WhooshSearch, self).init_app(app)

    def index(self, model):
        '''
        get index
        '''
        name = model.__table__.name
        if name not in self._indexs:
            self._indexs[name] = Index(
                model,
                name,
                self.pk,
                self.analyzer,
                self.index_name,
            )
        return self._indexs[name]

    def create_one_index(self,
                         instance,
                         update=False,
                         delete=False,
                         commit=True):
        '''
        :param instance: sqlalchemy instance object
        :param update: when update is True,use `update_document`,default `False`
        :param delete: when delete is True,use `delete_by_term` with id(primary key),default `False`
        :param commit: when commit is True,writer would use writer.commit()
        :raise: ValueError:when both update is True and delete is True
        :return: instance
        '''
        if update and delete:
            raise ValueError("update and delete can't work togther")
        ix = self.index(instance.__class__)
        pk = ix.pk
        attrs = {pk: str(getattr(instance, pk))}

        for field in ix.fields:
            if '.' in field:
                attrs[field] = str(relation_column(instance, field.split('.')))
            else:
                attrs[field] = str(getattr(instance, field))
        if delete:
            self.logger.debug('deleting index: {}'.format(instance))
            ix.delete(fieldname=pk, text=str(getattr(instance, pk)))
        elif update:
            self.logger.debug('updating index: {}'.format(instance))
            ix.update(**attrs)
        else:
            self.logger.debug('creating index: {}'.format(instance))
            ix.create(**attrs)
        if commit:
            ix.commit()
        return instance

    def _fields(self, index, attr):
        return attr

    def msearch(self, m, query, fields=None, limit=None, or_=True, **kwargs):
        '''
        set limit make search faster
        '''
        ix = self.index(m)
        if fields is None:
            fields = ix.fields

        def _parser(fieldnames, schema, group, **kwargs):
            return MultifieldParser(fieldnames, schema, group=group, **kwargs)

        group = OrGroup if or_ else AndGroup
        parser = getattr(m, "__msearch_parser__", _parser)(
            fields,
            ix.schema,
            group,
            **kwargs,
        )
        return ix.search(parser.parse(query), limit=limit)

    def _query_class(self, q):
        _self = self

        class Query(q):
            def whoosh_search(self, query, fields=None, limit=None, or_=False):
                self.logger.warning(
                    'whoosh_search has been replaced by msearch.please use msearch'
                )
                return self.msearch(query, fields, limit, or_)

            def msearch(
                    self,
                    query,
                    fields=None,
                    limit=None,
                    or_=False,
                    rank_order=False,
                    **kwargs):
                model = get_mapper(self).class_
                ix = _self.index(model)
                results = _self.msearch(
                    model,
                    query,
                    fields,
                    limit,
                    or_,
                    **kwargs,
                )
                if not results:
                    return self.filter(False)
                result_set = set()
                for i in results:
                    result_set.add(i[ix.pk])
                result_query = self.filter(
                    getattr(model, ix.pk).in_(result_set))
                if rank_order:
                    result_query = result_query.order_by(
                        sqlalchemy.sql.expression.case(
                            {r[ix.pk]: r.rank
                             for r in results},
                            value=getattr(model, ix.pk)))
                return result_query

        return Query
