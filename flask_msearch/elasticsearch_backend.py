#!/usr/bin/env python
# -*- coding: utf-8 -*-
# **************************************************************************
# Copyright © 2017-2019 jianglin
# File Name: elasticsearch_backend.py
# Author: jianglin
# Email: mail@honmaple.com
# Created: 2017-09-20 15:13:22 (CST)
# Last Update: Tuesday 2018-12-18 10:59:36 (CST)
#          By:
# Description:
# **************************************************************************
from flask_sqlalchemy import models_committed
from sqlalchemy import types
from elasticsearch import Elasticsearch
from .backends import BaseBackend, BaseSchema, logger, relation_column
import sqlalchemy


class Schema(BaseSchema):
    def fields_map(self, field_type):
        if field_type == "primary":
            return {'type': 'keyword'}

        type_map = {
            'date': types.Date,
            'datetime': types.DateTime,
            'boolean': types.Boolean,
            'integer': types.Integer,
            'float': types.Float,
            'binary': types.Binary
        }
        if isinstance(field_type, str):
            field_type = type_map.get(field_type, types.Text)

        if field_type in (types.DateTime, types.Date):
            return {'type': 'date'}
        elif field_type == types.Integer:
            return {'type': 'long'}
        elif field_type == types.Float:
            return {'type': 'float'}
        elif field_type == types.Boolean:
            return {'type': 'boolean'}
        elif field_type == types.Binary:
            return {'type': 'binary'}
        return {'type': 'string'}


# https://medium.com/@federicopanini/elasticsearch-6-0-removal-of-mapping-types-526a67ff772
class Index(object):
    def __init__(self, client, name, doc_type):
        self._client = client
        self.name = name
        self.doc_type = doc_type
        self.init()

    def init(self):
        if not self._client.indices.exists(index=self.name):
            self._client.indices.create(index=self.name)

    def create(self, **kwargs):
        "Create document not create index."
        kw = dict(index=self.name, doc_type=self.doc_type)
        kw.update(**kwargs)
        return self._client.index(**kw)

    def update(self, **kwargs):
        "Update document not update index."
        kw = dict(index=self.name, doc_type=self.doc_type, ignore=[404])
        kw.update(**kwargs)
        return self._client.update(**kw)

    def delete(self, **kwargs):
        "Delete document not delete index."
        kw = dict(index=self.name, doc_type=self.doc_type, ignore=[404])
        kw.update(**kwargs)
        return self._client.delete(**kw)

    def search(self, **kwargs):
        kw = dict(index=self.name, doc_type=self.doc_type)
        kw.update(**kwargs)
        return self._client.search(**kw)

    def commit(self):
        return self._client.indices.refresh(index=self.name)


class ElasticSearch(BaseBackend):
    def init_app(self, app):
        self._indexs = {}
        self.index_name = app.config.get('MSEARCH_INDEX_NAME', 'msearch')
        self._client = Elasticsearch(**app.config.get('ELASTICSEARCH', {}))
        if app.config.get('MSEARCH_ENABLE', True):
            models_committed.connect(self._index_signal)
        super(ElasticSearch, self).init_app(app)

    @property
    def indices(self):
        return self._client.indices

    def create_one_index(self,
                         instance,
                         update=False,
                         delete=False,
                         commit=True):
        if update and delete:
            raise ValueError("update and delete can't work togther")
        table = instance.__class__
        searchable = table.__searchable__
        ix = self._index(table)
        attrs = dict()
        for field in searchable:
            if '.' in field:
                attrs[field] = str(relation_column(instance, field.split('.')))
            else:
                attrs[field] = str(getattr(instance, field))
        if delete:
            logger.debug('deleting index: {}'.format(instance))
            r = ix.delete(id=instance.id)
        elif update:
            logger.debug('updating index: {}'.format(instance))
            r = ix.update(id=instance.id, body={"doc": attrs})
        else:
            logger.debug('creating index: {}'.format(instance))
            r = ix.create(id=instance.id, body=attrs)
        if commit:
            ix.commit()
        return r

    def _index(self, model):
        '''
        Elasticsearch multi types has been removed
        Use multi index unless set __msearch_index__.
        '''
        doc_type = model
        if not isinstance(model, str):
            doc_type = model.__table__.name

        index_name = doc_type
        if hasattr(model, "__msearch_index__"):
            index_name = model.__msearch_index__

        if doc_type not in self._indexs:
            self._indexs[doc_type] = Index(self._client, index_name, doc_type)
        return self._indexs[doc_type]

    def _fields(self, attr):
        return {'id': attr.pop('id'), 'body': {"doc": attr}}

    def msearch(self, m, query=None):
        ix = self._index(m)
        return ix.search(body=query)

    def _query_class(self, q):
        _self = self

        class Query(q):
            def msearch(self,
                        query,
                        fields=None,
                        limit=None,
                        or_=False,
                        params=dict()):
                model = self._mapper_zero().class_
                # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html
                query_string = {
                    "fields": fields or model.__searchable__,
                    "query": query,
                    "default_operator": "OR" if or_ else "AND",
                    "analyze_wildcard": True
                }
                query_string.update(**params)
                query = {
                    "query": {
                        "query_string": query_string
                    },
                    "size": limit or -1,
                }
                results = _self.msearch(model, query)['hits']['hits']
                if not results:
                    return self.filter(sqlalchemy.text('null'))
                result_set = set()
                for i in results:
                    result_set.add(i["_id"])
                return self.filter(getattr(model, "id").in_(result_set))

        return Query
