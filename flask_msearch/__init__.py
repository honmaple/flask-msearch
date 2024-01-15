#!/usr/bin/env python
# -*- coding: utf-8 -*-

from werkzeug.utils import import_string


class Search(object):
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
        app.config.setdefault('MSEARCH_BACKEND', 'simple')

        backend = app.config['MSEARCH_BACKEND']
        backends = {
            "simple": "flask_msearch.simple_backend.SimpleSearch",
            "whoosh": "flask_msearch.whoosh_backend.WhooshSearch",
            "elasticsearch": "flask_msearch.elasticsearch_backend.ElasticSearch",
        }
        if backend not in backends:
            raise ValueError('backends {} not exists.'.format(backend))
        self._backend = import_string(backends[backend])(
            app,
            self.db,
            self.analyzer,
        )

    def __getattr__(self, name):
        if name == "_backend":
            raise AttributeError("The flask app has not been initialized")
        return getattr(self._backend, name)
