#!/usr/bin/env python
# -*- coding: utf-8 -*-
# **************************************************************************
# Copyright © 2017 jianglin
# File Name: elasticsearch_backend.py
# Author: jianglin
# Email: xiyang0807@gmail.com
# Created: 2017-09-20 15:13:22 (CST)
# Last Update: Monday 2018-05-09 11:45:38 (CST)
#          By:
# Description:
# **************************************************************************
from flask_sqlalchemy import models_committed
from sqlalchemy import types, inspect
from elasticsearch import Elasticsearch
from .backends import BaseBackend, logger


class Schema(object):
    def __init__(self, table, analyzer=None):
        self.table = table
        self.analyzer = analyzer

    @property
    def fields(self):
        model = self.table
        schema_fields = {}
        searchable = set(model.__searchable__)
        primary_keys = [key.name for key in inspect(model).primary_key]
        for field in searchable:
            field_type = getattr(model, field).property.columns[0].type
            if field in primary_keys:
                schema_fields[field] = {'type': 'keyword'}
            elif field_type in (types.DateTime, types.Date):
                schema_fields[field] = {'type': 'date'}
            elif field_type == types.Integer:
                schema_fields[field] = {'type': 'integer'}
            elif field_type == types.Float:
                schema_fields[field] = {'type': 'float'}
            elif field_type == types.Boolean:
                schema_fields[field] = {'type': 'boolean'}
            elif field_type == types.Binary:
                schema_fields[field] = {'type': 'binary'}
            else:
                schema_fields[field] = {'type': 'text'}

        return schema_fields


class Index(object):
    def __init__(self, client, name, doc_type):
        self._client = client
        self.name = name
        self.doc_type = doc_type

    def init(self):
        if not self._client.indices.exists(index=self.name):
            self._client.indices.create(index=self.name, ignore=400)

    def create(self, **kwargs):
        kwargs.update(index=self.name, doc_type=self.doc_type)
        return self._client.create(**kwargs)

    def update(self, **kwargs):
        kwargs.update(
            index=self.name, doc_type=self.doc_type, id=kwargs.pop('id'))
        return self._client.update(**kwargs)

    def delete(self, **kwargs):
        kwargs.update(index=self.name, doc_type=self.doc_type)
        return self._client.delete(**kwargs)

    def search(self, **kwargs):
        kwargs.update(index=self.name, doc_type=self.doc_type)
        return self._client.search(**kwargs)

    def commit(self):
        return self._client.indices.refresh(index=self.name)


class ElasticSearch(BaseBackend):
    def init_app(self, app):
        self._indexs = {}
        es_setting = app.config.get('ELASTICSEARCH', {})
        self.index_name = app.config.get('MSEARCH_INDEX_NAME', 'msearch')
        self._client = Elasticsearch(**es_setting)
        if not self._client.indices.exists(index=self.index_name):
            self._client.indices.create(index=self.index_name, ignore=400)
        if app.config.get('MSEARCH_ENABLE', True):
            models_committed.connect(self._index_signal)
        super(ElasticSearch, self).init_app(app)

    def create_one_index(self,
                         instance,
                         update=False,
                         delete=False,
                         commit=True):
        if update and delete:
            raise ValueError("update and delete can't work togther")
        doc_type = instance.__class__
        ix = self._index(doc_type)
        body = {i: getattr(instance, i) for i in doc_type.__searchable__}
        if delete:
            logger.debug('deleting index: {}'.format(instance))
            r = ix.delete(id=instance.id)
        elif update:
            logger.debug('updating index: {}'.format(instance))
            r = ix.update(id=instance.id, body=body)
        else:
            logger.debug('creating index: {}'.format(instance))
            r = ix.create(id=instance.id, body=body)
        if commit:
            ix.commit()
        return r

    def _index(self, model):
        name = model
        if not isinstance(model, str):
            name = model.__table__.name
        if name not in self._indexs:
            self._indexs[name] = Index(self._client, self.index_name, name)
        return self._indexs[name]

    def _fields(self, attr):
        return {'id': attr.pop('id'), 'body': attr}

    def msearch(self, m, query):
        ix = self._index(m)
        return ix.search(body=query)
