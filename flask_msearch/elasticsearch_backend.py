#!/usr/bin/env python
# -*- coding: utf-8 -*-
# **************************************************************************
# Copyright © 2017 jianglin
# File Name: elasticsearch.py
# Author: jianglin
# Email: xiyang0807@gmail.com
# Created: 2017-04-22 12:13:47 (CST)
# Last Update:星期四 2017-5-4 22:33:17 (CST)
#          By:
# Description:
# **************************************************************************
from sqlalchemy import types
from elasticsearch import Elasticsearch
from .backends import BaseBackend, logger


class ElasticSearchBackend(BaseBackend):
    def create_one_index(self,
                         instance,
                         writer=None,
                         update=False,
                         delete=False,
                         commit=True):
        if update and delete:
            raise ValueError("update and delete can't work togther")
        ix = self._index(instance.__class__)
        searchable = ix.schema.names()
        if not writer:
            writer = ix.writer()
        attrs = {'id': str(instance.id)}
        for i in searchable:
            attrs[i] = str(getattr(instance, i))
        ix.create(index='1', data=attrs)

    def create_index(self, model='__all__', update=False, delete=False):
        if model == '__all__':
            return self.create_all_index(update, delete)
        ix = self._index(model)
        writer = ix.writer()
        instances = model.query.enable_eagerloads(False).yield_per(100)
        for instance in instances:
            self.create_one_index(instance, writer, update, delete, False)
        writer.commit()
        return ix

    def create_all_index(self, update=False, delete=False):
        all_models = self.db.Model._decl_class_registry.values()
        models = [i for i in all_models if hasattr(i, '__searchable__')]
        ixs = []
        for m in models:
            ix = self.create_index(m, update, delete)
            ixs.append(ix)
        return ixs

    def _index(self, model):
        es = Elasticsearch()
        name = model.__table__.name
        if not es.indices.exists(index=name):
            body = {"mappings": self._mapping(model)}
            es.indices.create(index=name, body=body)
        return es

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

    def msearch(self, model, keyword):
        ix = self._index(model)
        ix.search('')
