#!/usr/bin/env python
# -*- coding: utf-8 -*-
# **************************************************************************
# Copyright © 2017 jianglin
# File Name: elasticsearch.py
# Author: jianglin
# Email: xiyang0807@gmail.com
# Created: 2017-04-22 12:13:47 (CST)
# Last Update:星期六 2017-5-6 12:54:46 (CST)
#          By:
# Description:
# **************************************************************************
from flask_sqlalchemy import models_committed
from sqlalchemy import types, inspect
from elasticsearch import Elasticsearch
from .backends import BaseBackend, logger


class ElasticSearch(BaseBackend):
    def init_app(self, app):
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
        body = {i: getattr(instance, i) for i in doc_type.__searchable__}
        if delete:
            logger.debug('deleting index: {}'.format(instance))
            r = self._client.delete(
                index=self.index_name, doc_type=doc_type, id=instance.id)
        elif update:
            logger.debug('updating index: {}'.format(instance))
            r = self._client.update(
                index=self.index_name,
                doc_type=doc_type,
                id=instance.id,
                body=body)
        else:
            logger.debug('creating index: {}'.format(instance))
            r = self._client.create(
                index=self.index_name,
                doc_type=doc_type,
                id=instance.id,
                body=body)
        if commit:
            self._client.indices.refresh(index=self.index_name)
        return r

    def create_index(self, model='__all__', update=False, delete=False):
        if model == '__all__':
            return self.create_all_index(update, delete)
        instances = model.query.enable_eagerloads(False).yield_per(100)
        for instance in instances:
            self.create_one_index(instance, update, delete, False)
        self._client.indices.refresh(index=self.index_name)

    def create_all_index(self, update=False, delete=False):
        all_models = self.db.Model._decl_class_registry.values()
        models = [i for i in all_models if hasattr(i, '__searchable__')]
        for m in models:
            self.create_index(m, update, delete)

    def _schema(self, model):
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

    def _mapping(self, model):
        name = model.__table__.name
        return {name: {'properties': self._schema(model)}}

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

    def msearch(self, model, query):
        doc_type = model.__table__.name
        return self._client.search(
            index=self.index_name, doc_type=doc_type, body=query)
