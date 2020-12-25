#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ********************************************************************************
# Copyright © 2019 jianglin
# File Name: test_elasticsearch.py
# Author: jianglin
# Email: mail@honmaple.com
# Created: 2019-04-01 10:16:57 (CST)
# Last Update: Monday 2020-05-18 22:32:10 (CST)
#          By:
# Description:
# ********************************************************************************
from test import (
    TestMixin,
    SearchTestBase,
    Flask,
    SQLAlchemy,
    Search,
    unittest,
    ModelSaveMixin,
)
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

    def test_fuzzy_search(self):
        with self.app.test_request_context():
            post1 = self.Post(
                title="this is a fuzzy search", content="do search")
            post1.save(self.db)

            post2 = self.Post(
                title="this is a fuzzysearch", content="normal title")
            post2.save(self.db)

            post3 = self.Post(
                title="this is a normal search", content="do FFFsearchfuzzy")
            post3.save(self.db)

            results = self.Post.query.msearch('title:search').all()
            self.assertEqual(len(results), 2)

            results = self.Post.query.msearch('content:search').all()
            self.assertEqual(len(results), 1)

            results = self.Post.query.msearch(
                'title:search OR content:title').all()
            self.assertEqual(len(results), 3)

            results = self.Post.query.msearch('*search').all()
            self.assertEqual(len(results), 3)

            results = self.Post.query.msearch('search*').all()
            self.assertEqual(len(results), 2)

            results = self.Post.query.msearch('abc').all()
            self.assertEqual(len(results), 0)


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromNames(
        [
            'test_elasticsearch.TestSearch',
        ])
    unittest.TextTestRunner(verbosity=1).run(suite)
