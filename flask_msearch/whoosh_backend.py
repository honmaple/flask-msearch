#!/usr/bin/env python
# -*- coding: utf-8 -*-
# **************************************************************************
# Copyright © 2017 jianglin
# File Name: whoosh_backend.py
# Author: jianglin
# Email: xiyang0807@gmail.com
# Created: 2017-04-15 20:03:27 (CST)
# Last Update:星期三 2017-8-16 14:10:31 (CST)
#          By:
# Description:
# **************************************************************************
import os
import os.path
import sys

import sqlalchemy
from inspect import isclass
from flask_sqlalchemy import models_committed
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.inspection import inspect
from sqlalchemy.types import Boolean, Date, DateTime, Float, Integer, Text
from whoosh import index as whoosh_index
from whoosh.analysis import StemmingAnalyzer
from whoosh.fields import BOOLEAN, DATETIME, ID, NUMERIC, TEXT, Schema
from whoosh.qparser import AndGroup, MultifieldParser, OrGroup

from .backends import BaseBackend, logger, relation_column

DEFAULT_WHOOSH_INDEX_NAME = 'whoosh_index'
DEFAULT_ANALYZER = StemmingAnalyzer()
DEFAULT_PRIMARY_KEY = 'id'

if sys.version_info[0] < 3:
    str = unicode


class WhooshSearch(BaseBackend):
    def init_app(self, app):
        self._indexs = {}
        self.whoosh_path = DEFAULT_WHOOSH_INDEX_NAME
        if self.analyzer is None:
            self.analyzer = DEFAULT_ANALYZER
        whoosh_path = app.config.get('MSEARCH_INDEX_NAME')
        if whoosh_path is not None:
            self.whoosh_path = whoosh_path
        if not os.path.exists(self.whoosh_path):
            os.mkdir(self.whoosh_path)
        if app.config.get('MSEARCH_ENABLE', True):
            models_committed.connect(self._index_signal)
        super(WhooshSearch, self).init_app(app)

    def create_one_index(self,
                         instance,
                         writer=None,
                         update=False,
                         delete=False,
                         commit=True):
        '''
        :param instance: sqlalchemy instance object
        :param writer: whoosh writer,default `None`
        :param update: when update is True,use `update_document`,default `False`
        :param delete: when delete is True,use `delete_by_term` with id(primary key),default `False`
        :param commit: when commit is True,writer would use writer.commit()
        :raise: ValueError:when both update is True and delete is True
        :return: instance
        '''
        if update and delete:
            raise ValueError("update and delete can't work togther")
        ix = self._index(instance.__class__)
        searchable = ix.schema.names()
        if not writer:
            writer = ix.writer()
        attrs = {'id': str(instance.id)}

        for field in searchable:
            if '.' in field:
                attrs[field] = str(relation_column(instance, field.split('.')))
            else:
                attrs[field] = str(getattr(instance, field))
        if delete:
            logger.debug('deleting index: {}'.format(instance))
            writer.delete_by_term('id', str(instance.id))
        elif update:
            logger.debug('updating index: {}'.format(instance))
            writer.update_document(**attrs)
        else:
            logger.debug('creating index: {}'.format(instance))
            writer.add_document(**attrs)
        if commit:
            writer.commit()
        return instance

    def create_index(self,
                     model='__all__',
                     update=False,
                     delete=False,
                     yield_per=100):
        if model == '__all__':
            return self.create_all_index(update, delete)
        ix = self._index(model)
        writer = ix.writer()
        instances = model.query.enable_eagerloads(False).yield_per(yield_per)
        for instance in instances:
            self.create_one_index(instance, writer, update, delete, False)
        writer.commit()
        return ix

    def create_all_index(self, update=False, delete=False, yield_per=100):
        all_models = self.db.Model._decl_class_registry.values()
        models = [i for i in all_models if hasattr(i, '__searchable__')]
        ixs = []
        for m in models:
            ix = self.create_index(m, update, delete, yield_per)
            ixs.append(ix)
        return ixs

    def update_one_index(self, instance, writer=None, commit=True):
        return self.create_one_index(
            instance, writer, update=True, commit=commit)

    def delete_one_index(self, instance, writer=None, commit=True):
        return self.delete_one_index(
            instance, writer, delete=True, commit=commit)

    def update_all_index(self, yield_per=100):
        return self.create_all_index(update=True, yield_per=yield_per)

    def delete_all_index(self, yield_per=100):
        return self.create_all_index(delete=True, yield_per=yield_per)

    def update_index(self, model='__all__', yield_per=100):
        return self.create_index(model, update=True, yield_per=yield_per)

    def delete_index(self, model='__all__', yield_per=100):
        return self.create_index(model, delete=True, yield_per=yield_per)

    def _index(self, model):
        '''
        get index
        '''
        name = model
        if not isinstance(model, str):
            name = model.__table__.name
        if name not in self._indexs:
            ix_path = os.path.join(self.whoosh_path, name)
            if whoosh_index.exists_in(ix_path):
                ix = whoosh_index.open_dir(ix_path)
            else:
                if not os.path.exists(ix_path):
                    os.makedirs(ix_path)
                if hasattr(model, '__whoosh_schema__'):
                    schema = getattr(model, '__whoosh_schema__')
                else:
                    schema = self._schema(model)
                ix = whoosh_index.create_in(ix_path, schema)
            self._indexs[name] = ix
        return self._indexs[name]

    def _schema(self, model):
        schema_fields = {'id': ID(stored=True, unique=True)}
        searchable = set(model.__searchable__)
        analyzer = getattr(model, '__whoosh_analyzer__') if hasattr(
            model, '__whoosh_analyzer__') else self.analyzer
        primary_keys = [key.name for key in inspect(model).primary_key]

        for field in searchable:
            if '.' in field:
                fields = field.split('.')
                field_attr = getattr(
                    getattr(model, fields[0]).property.mapper.class_,
                    fields[1])
            else:
                field_attr = getattr(model, field)
            if hasattr(field_attr, 'descriptor') and isinstance(
                    field_attr.descriptor, hybrid_property):
                field_type = Text
                type_hint = getattr(field_attr, 'type_hint', None)
                if type_hint is not None:
                    type_hint_map = {
                        'date': Date,
                        'datetime': DateTime,
                        'boolean': Boolean,
                        'integer': Integer,
                        'float': Float
                    }
                    field_type = type_hint if isclass(
                        type_hint) else type_hint_map.get(type_hint.lower(),
                                                          Text)
            else:
                field_type = field_attr.property.columns[0].type
            if field in primary_keys:
                schema_fields[field] = ID(stored=True, unique=True)
            elif field_type in (DateTime, Date):
                schema_fields[field] = DATETIME(stored=True, sortable=True)
            elif field_type == Integer:
                schema_fields[field] = NUMERIC(stored=True, numtype=int)
            elif field_type == Float:
                schema_fields[field] = NUMERIC(stored=True, numtype=float)
            elif field_type == Boolean:
                schema_fields[field] = BOOLEAN(stored=True)
            else:
                schema_fields[field] = TEXT(
                    stored=True, analyzer=analyzer, sortable=False)
        return Schema(**schema_fields)

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
                    writer = ix.writer()
                    # logger.debug('updating index: {}'.format(instance))
                    for attr in attrs['attrs']:
                        writer.update_document(**attr)
                    writer.commit()

    def whoosh_search(self, m, query, fields=None, limit=None, or_=False):
        logger.warning(
            'whoosh_search has been replaced by msearch.please use msearch')
        return self.msearch(m, query, fields, limit, or_)

    def msearch(self, m, query, fields=None, limit=None, or_=False):
        '''
        set limit make search faster
        '''
        ix = self._index(m)
        if fields is None:
            fields = ix.schema.names()
        group = OrGroup if or_ else AndGroup
        parser = MultifieldParser(fields, ix.schema, group=group)
        results = ix.searcher().search(parser.parse(query), limit=limit)
        return results

    def _query_class(self, q):
        _self = self

        class Query(q):
            def whoosh_search(self, query, fields=None, limit=None, or_=False):
                logger.warning(
                    'whoosh_search has been replaced by msearch.please use msearch'
                )
                return self.msearch(query, fields, limit, or_)

            def msearch(self, query, fields=None, limit=None, or_=False):
                model = self._mapper_zero().class_
                results = _self.msearch(model, query, fields, limit, or_)
                if not results:
                    return self.filter(sqlalchemy.text('null'))
                result_set = set()
                for i in results:
                    result_set.add(i[DEFAULT_PRIMARY_KEY])
                return self.filter(
                    getattr(model, DEFAULT_PRIMARY_KEY).in_(result_set))

        return Query
