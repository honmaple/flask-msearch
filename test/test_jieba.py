#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ********************************************************************************
# Copyright © 2018 jianglin
# File Name: test_jieba.py
# Author: jianglin
# Email: mail@honmaple.com
# Created: 2018-05-18 13:22:46 (CST)
# Last Update: Tuesday 2018-12-18 11:32:46 (CST)
#          By:
# Description:
# ********************************************************************************
from test import (SearchTestBase, mkdtemp, Flask, SQLAlchemy, Search, unittest,
                  ModelSaveMixin)
from jieba.analyse import ChineseAnalyzer

titles = [
    "买水果然后来世博园。",
    "The second one 你 中文测试中文 is even more interesting! 吃水果",
    "吃苹果",
    "吃橘子",
]


class TestSearch(SearchTestBase):
    def setUp(self):
        class TestConfig(object):
            SQLALCHEMY_TRACK_MODIFICATIONS = True
            SQLALCHEMY_DATABASE_URI = 'sqlite://'
            DEBUG = True
            TESTING = True
            MSEARCH_INDEX_NAME = mkdtemp()
            MSEARCH_BACKEND = 'whoosh'

        self.app = Flask(__name__)
        self.app.config.from_object(TestConfig())
        self.db = SQLAlchemy(self.app)
        self.search = Search(self.app, db=self.db, analyzer=ChineseAnalyzer())

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

        with self.app.test_request_context():
            self.db.create_all()
            for (i, title) in enumerate(titles, 1):
                post = self.Post(title=title, content='content%d' % i)
                post.save(self.db)

    def test_basic_search(self):
        with self.app.test_request_context():
            results = self.Post.query.msearch('水果').all()
            self.assertEqual(len(results), 2)

            results = self.Post.query.msearch('苹果').all()
            self.assertEqual(len(results), 1)

            results = self.Post.query.msearch('世博园').all()
            self.assertEqual(len(results), 1)


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromNames([
        'test_jieba.TestSearch',
    ])
    unittest.TextTestRunner(verbosity=1).run(suite)
