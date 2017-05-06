#!/usr/bin/env python
# -*- coding: utf-8 -*-
# **************************************************************************
# Copyright © 2017 jianglin
# File Name: test.py
# Author: jianglin
# Email: xiyang0807@gmail.com
# Created: 2017-04-20 10:45:25 (CST)
# Last Update:星期六 2017-5-6 13:47:31 (CST)
#          By:
# Description:
# **************************************************************************
from unittest import TestCase, main
from tempfile import mkdtemp
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_msearch import Search


class TestSearch(TestCase):
    def setUp(self):
        class config(object):
            MSEARCH_INDEX_NAME = mkdtemp()
            SQLALCHEMY_DATABASE_URI = 'sqlite://'
            DEBUG = True
            TESTING = True
            # MSEARCH_BACKEND = 'simple'

        app = Flask(__name__)
        app.config.from_object(config)
        db = SQLAlchemy()
        search = Search(db=db)

        class Post(db.Model):
            __tablename__ = 'posts'
            __searchable__ = ['title', 'content']

            id = db.Column(db.Integer, primary_key=True)
            title = db.Column(db.String(49))
            content = db.Column(db.Text)

            def save(self):
                if not self.id:
                    db.session.add(self)
                    db.session.commit()
                else:
                    db.session.commit()

            def delete(self):
                db.session.delete(self)
                db.session.commit()

            def __repr__(self):
                return '<Post:{}>'.format(self.title)

        db.init_app(app)
        search.init_app(app)
        with app.test_request_context():
            db.create_all()

        self.Post = Post
        self.db = db
        self.app = app
        self.search = search

    def tearDown(self):
        with self.app.test_request_context():
            self.db.drop_all()

    def test_search(self):
        with self.app.test_request_context():
            title1 = 'watch a movie'
            title2 = 'read a book'
            title3 = 'write a book'
            title4 = 'listen to a music'
            title5 = 'I have a book'
            post1 = self.Post(title=title1, content='content1')
            post1.save()
            self.assertEqual(self.Post.query.all(), [post1])

            post2 = self.Post(title=title2, content='content2')
            post2.save()

            post3 = self.Post(title=title3, content='content3')
            post3.save()

            post4 = self.Post(title=title4, content='content3')
            post4.save()

            post5 = self.Post(title=title5, content='content3')
            post5.save()

            results = self.Post.query.msearch('book').all()
            self.assertEqual(len(results), 3)
            self.assertEqual(results[0].title, title2)
            self.assertEqual(results[1].title, title3)

            # test limit
            results = self.Post.query.msearch('book', limit=2).all()
            self.assertEqual(len(results), 2)

            # test and or
            results = self.Post.query.msearch('book movie', or_=False).all()
            self.assertEqual(len(results), 0)

            results = self.Post.query.msearch('book movie', or_=True).all()
            self.assertEqual(len(results), 4)

            # test delete
            post2.delete()

            results = self.Post.query.msearch('book').all()
            self.assertEqual(len(results), 2)

            # test update
            post3.title = 'write a novel'
            post3.save()

            results = self.Post.query.msearch('book').all()
            self.assertEqual(len(results), 1)

            results = self.Post.query.msearch('movie').all()
            self.assertEqual(len(results), 1)

            # test fields
            title1 = 'add one user'
            content1 = 'add one user content 1'
            title2 = 'add two user'
            content2 = 'add two content 2'
            post1 = self.Post(title=title1, content=content1)
            post1.save()

            post2 = self.Post(title=title2, content=content2)
            post2.save()

            results = self.Post.query.msearch('user').all()
            self.assertEqual(len(results), 2)

            results = self.Post.query.msearch('user', fields=['title']).all()
            self.assertEqual(len(results), 2)

            results = self.Post.query.msearch('user', fields=['content']).all()
            self.assertEqual(len(results), 1)


if __name__ == '__main__':
    main()
