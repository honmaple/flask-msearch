import datetime
import logging
import unittest
import os
import sys

sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from tempfile import mkdtemp
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_msearch import Search
from sqlalchemy.ext.hybrid import hybrid_property

# do not clutter output with log entries
logging.disable(logging.CRITICAL)

# db = None

titles = [
    'watch a movie',
    'read a book',
    'write a book',
    'listen to a music',
    'I have a book',
]


class ModelSaveMixin(object):
    def save(self, db):
        if not self.id:
            db.session.add(self)
        db.session.commit()

    def delete(self, db):
        db.session.delete(self)
        db.session.commit()


class SearchTestBase(unittest.TestCase):
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

    def init_data(self):
        with self.app.test_request_context():
            self.db.create_all()
            for (i, title) in enumerate(titles, 1):
                post = self.Post(title=title, content='content%d' % i)
                post.save(self.db)

    def tearDown(self):
        with self.app.test_request_context():
            self.db.drop_all()
            self.db.metadata.clear()


class TestMixin(object):
    def test_basic_search(self):
        with self.app.test_request_context():
            results = self.Post.query.msearch('book').all()
            self.assertEqual(len(results), 3)
            self.assertEqual(results[0].title, titles[1])
            self.assertEqual(results[1].title, titles[2])
            results = self.Post.query.msearch('movie').all()
            self.assertEqual(len(results), 1)

    def test_search_limit(self):
        with self.app.test_request_context():
            results = self.Post.query.msearch('book', limit=2).all()
            self.assertEqual(len(results), 2)

    def test_boolean_operators(self):
        with self.app.test_request_context():
            results = self.Post.query.msearch('book movie', or_=False).all()
            self.assertEqual(len(results), 0)
            results = self.Post.query.msearch('book movie', or_=True).all()
            self.assertEqual(len(results), 4)

    def test_delete(self):
        with self.app.test_request_context():
            r = self.Post.query.filter_by(title='read a book').first()
            self.db.session.delete(r)
            self.db.session.commit()
            results = self.Post.query.msearch('book').all()
            self.assertEqual(len(results), 2)

    def test_update(self):
        with self.app.test_request_context():
            post = self.Post.query.filter_by(title='write a book').one()
            post.title = 'write a novel'
            post.save(self.db)
            results = self.Post.query.msearch('book').all()
            self.assertEqual(len(results), 2)

    def test_field_search(self):
        with self.app.test_request_context():
            title1 = 'add one user'
            content1 = 'add one user content 1'
            title2 = 'add two user'
            content2 = 'add two content 2'
            post1 = self.Post(title=title1, content=content1)
            post1.save(self.db)

            post2 = self.Post(title=title2, content=content2)
            post2.save(self.db)

            results = self.Post.query.msearch('user').all()
            self.assertEqual(len(results), 2)

            results = self.Post.query.msearch('user', fields=['title']).all()
            self.assertEqual(len(results), 2)

            results = self.Post.query.msearch('user', fields=['content']).all()
            self.assertEqual(len(results), 1)
