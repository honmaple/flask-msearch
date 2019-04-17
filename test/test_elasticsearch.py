#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ********************************************************************************
# Copyright © 2019 jianglin
# File Name: test_elasticsearch.py
# Author: jianglin
# Email: mail@honmaple.com
# Created: 2019-04-01 10:16:57 (CST)
# Last Update:
#          By:
# Description:
# ********************************************************************************
from test import (TestMixin, SearchTestBase, mkdtemp, Flask, SQLAlchemy,
                  Search, unittest, ModelSaveMixin)
from random import sample
from string import ascii_lowercase, digits


class TestSearch(TestMixin, SearchTestBase):
    def setUp(self):
        class TestConfig(object):
            SQLALCHEMY_TRACK_MODIFICATIONS = True
            SQLALCHEMY_DATABASE_URI = 'sqlite://'
            DEBUG = True
            TESTING = True
            MSEARCH_INDEX_NAME = ''.join(sample(ascii_lowercase + digits, 8))
            MSEARCH_BACKEND = 'elasticsearch'
            ELASTICSEARCH = {"hosts": ["127.0.0.1:9200"]}

        self.app = Flask(__name__)
        self.app.config.from_object(TestConfig())
        self.db = SQLAlchemy(self.app)
        self.search = Search(self.app, db=self.db)

        db = self.db

        class Post(db.Model, ModelSaveMixin):
            __tablename__ = 'basic_posts'
            __searchable__ = ['title', 'content']

            id = db.Column(db.Integer, primary_key=True)
            title = db.Column(db.String(49))
            content = db.Column(db.Text)

            def __repr__(self):
                return '<Post:{}>'.format(self.title)

        self.Post = Post
        self.init_data()


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromNames([
        'test_elasticsearch.TestSearch',
    ])
    unittest.TextTestRunner(verbosity=1).run(suite)
