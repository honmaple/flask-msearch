#!/usr/bin/env python
# -*- coding: utf-8 -*-
from test import (
    TestMixin, SearchTestBase, mkdtemp, Flask, SQLAlchemy, Search, unittest,
    ModelSaveMixin, hybrid_property, datetime)

from whoosh.analysis import RegexTokenizer, Filter
from whoosh.fields import TEXT


class CaseSensitivizer(Filter):
    def __call__(self, tokens):
        for t in tokens:
            yield t
            text = t.text.lower()
            if text != t.text:
                t.text = text
                yield t


class TestSearch(TestMixin, SearchTestBase):
    def setUp(self):
        super(TestSearch, self).setUp()

        self.init_data()

    def test_rank_order(self):
        with self.app.test_request_context():
            post1 = self.Post(title="buy car", content="result1")
            post1.save(self.db)
            post2 = self.Post(title="sale car", content="result2")
            post2.save(self.db)
            post3 = self.Post(title="tim sale car", content="result3")
            post3.save(self.db)

            results = self.Post.query.msearch('car', rank_order=False).all()
            self.assertEqual(results[0].id, post1.id)

            results = self.Post.query.msearch(
                "'sale car' OR 'car'", rank_order=False).all()
            self.assertEqual(results[0].id, post1.id)

            results = self.Post.query.msearch(
                "'sale car' OR 'car'", rank_order=True).all()
            self.assertEqual(results[0].id, post2.id)
            results = self.Post.query.msearch(
                "'car' OR 'sale car'", rank_order=True).all()
            self.assertEqual(results[0].id, post2.id)

            results = self.Post.query.msearch(
                "'sale car' OR 'car' OR 'tim sale car'",
                rank_order=False).all()
            self.assertEqual(results[0].id, post1.id)

            results = self.Post.query.msearch(
                "'sale car' OR 'car' OR 'tim sale car'",
                rank_order=True).all()
            self.assertEqual(results[0].id, post3.id)


class TestCaseSearch(SearchTestBase):
    def setUp(self):
        super(TestCaseSearch, self).setUp()

        db = self.db

        class Post(db.Model, ModelSaveMixin):
            __tablename__ = 'posts'
            __searchable__ = ['title', 'content']
            __msearch_schema__ = {
                "title": TEXT(
                    stored=True,
                    analyzer=RegexTokenizer() | CaseSensitivizer(),
                    sortable=False),
                "content": TEXT(
                    stored=True,
                    analyzer=RegexTokenizer(),
                    sortable=False,
                )
            }

            id = db.Column(db.Integer, primary_key=True)
            title = db.Column(db.String(49))
            content = db.Column(db.Text)

        self.Post = Post
        with self.app.test_request_context():
            db.create_all()
            self.Post(
                title="I have an apple",
                content="aPPLe Test",
            ).save(self.db)
            self.Post(
                title="I have an aPPLe",
                content="apple Test",
            ).save(self.db)

    def test_case_search(self):
        with self.app.test_request_context():
            results = self.Post.query.msearch('apple', fields=['title']).all()
            self.assertEqual(len(results), 2)
            results = self.Post.query.msearch(
                'apple', fields=['content']).all()
            self.assertEqual(len(results), 1)
            results = self.Post.query.msearch('apple').all()
            self.assertEqual(len(results), 2)


class TestRelationSearch(TestMixin, SearchTestBase):
    def setUp(self):
        super(TestRelationSearch, self).setUp()
        db = self.db

        class Tag(db.Model, ModelSaveMixin):
            __tablename__ = 'tag'

            id = db.Column(db.Integer, primary_key=True)
            name = db.Column(db.String(49))

            def msearch_post_tag(self, delete=False):
                from sqlalchemy.sql import select
                post = Post.__table__
                sql = select([post.c.id]).where(post.c.tag_id == self.id)
                return {
                    'attrs': [
                        {
                            # id is Post's primary key
                            'id': str(i[0]),
                            'tag.name': self.name
                        } for i in db.engine.execute(sql)
                    ],
                    '_index': Post
                }

                # or use sql text
                from sqlalchemy import text
                sql = text('select id from post where tag_id=' + str(self.id))
                return {
                    'attrs': [
                        {
                            # id is Post's primary key
                            'id': str(i[0]),
                            'tag.name': self.name
                        } for i in db.engine.execute(sql)
                    ],
                    '_index': Post
                }

        class Post(db.Model, ModelSaveMixin):
            __tablename__ = 'post'
            __searchable__ = ['title', 'content', 'tag.name']

            id = db.Column(db.Integer, primary_key=True)
            title = db.Column(db.String(49))
            content = db.Column(db.Text)

            # one to one
            tag_id = db.Column(db.Integer, db.ForeignKey('tag.id'))
            tag = db.relationship(
                Tag, backref=db.backref('post', uselist=False), uselist=False)

            def __repr__(self):
                return '<Post:{}>'.format(self.title)

        self.Post = Post
        self.Tag = Tag
        self.init_data()

    def test_field_search(self):
        with self.app.test_request_context():
            title1 = 'add one user'
            content1 = 'add one user content 1'
            title2 = 'add two user'
            content2 = 'add two content 2'

            post1 = self.Post(title=title1, content=content1)
            post1.save(self.db)

            tag1 = self.Tag(name='tag hello1', post=post1)
            tag1.save(self.db)

            post2 = self.Post(title=title2, content=content2)
            post2.save(self.db)

            tag2 = self.Tag(name='tag hello2', post=post2)
            tag2.save(self.db)

            results = self.Post.query.msearch('tag').all()
            self.assertEqual(len(results), 2)

            results = self.Post.query.msearch('tag', fields=['title']).all()
            self.assertEqual(len(results), 0)

            results = self.Post.query.msearch('tag', fields=['tag.name']).all()
            self.assertEqual(len(results), 2)

            tag1.name = 'post hello1'
            tag1.save(self.db)

            results = self.Post.query.msearch('tag', fields=['tag.name']).all()
            self.assertEqual(len(results), 1)

            tag1.name = 'post tag'
            tag1.save(self.db)

            results = self.Post.query.msearch('tag', fields=['tag.name']).all()
            self.assertEqual(len(results), 2)


class TestSearchHybridProp(TestMixin, SearchTestBase):
    def setUp(self):
        super(TestSearchHybridProp, self).setUp()

        db = self.db

        class PostHybrid(db.Model, ModelSaveMixin):
            __tablename__ = 'hybrid_posts'
            __searchable__ = ['fts_text', 'title', 'content']

            id = db.Column(db.Integer, primary_key=True)
            title = db.Column(db.String(49))
            content = db.Column(db.Text)

            @hybrid_property
            def fts_text(self):
                return ' '.join([self.title, self.content])

            @fts_text.expression
            def fts_text(cls):
                # sqlite don't support concat
                # return db.func.concat(cls.title, ' ', cls.content)
                return cls.title.op('||')(' ').op('||')(cls.content)

            def __repr__(self):
                return '<Post:{}>'.format(self.title)

        self.Post = PostHybrid
        self.init_data()

    def test_fts_text(self):
        with self.app.test_request_context():
            results = self.Post.query.msearch(
                'book', fields=['fts_text']).all()
            self.assertEqual(len(results), 3)


class TestHybridPropTypeHint(SearchTestBase):
    def setUp(self):
        super(TestHybridPropTypeHint, self).setUp()

        db = self.db

        class Post(db.Model, ModelSaveMixin):
            __tablename__ = 'posts'
            __searchable__ = ['fts_int', 'fts_date']
            __msearch_schema__ = {"fts_int": "integer", "fts_date": "date"}

            id = db.Column(db.Integer, primary_key=True)
            name = db.Column(db.String(20))
            int1 = db.Column(db.Integer)
            int2 = db.Column(db.Integer)

            @hybrid_property
            def fts_int(self):
                return self.int1 + self.int2

            @fts_int.expression
            def fts_int(cls):
                return db.func.sum(cls.int1, cls.int2)

            @hybrid_property
            def fts_date(self):
                return max(
                    datetime.date(2017, 5, 4), datetime.date(2017, 5, 3))

            @fts_date.expression
            def fts_date(cls):
                return db.func.max(
                    datetime.date(2017, 5, 4), datetime.date(2017, 5, 3))

            def __repr__(self):
                return '<Post:{}>'.format(self.name)

        self.Post = Post
        with self.app.test_request_context():
            db.create_all()
            for i in range(10):
                name = 'post %d' % i
                post = self.Post(name=name, int1=i, int2=i * 2)
                post.save(self.db)

    def test_int_prop(self):
        with self.app.test_request_context():
            results = self.Post.query.msearch('0', fields=['fts_int']).all()
            self.assertEqual(len(results), 1)
            results = self.Post.query.msearch('27', fields=['fts_int']).all()
            self.assertEqual(len(results), 1)

    def test_date_prop(self):
        with self.app.test_request_context():
            results = self.Post.query.msearch(
                '2017-05-04', fields=['fts_date']).all()
            self.assertEqual(len(results), 10)


class TestPrimaryKey(TestMixin, SearchTestBase):
    def setUp(self):
        class TestConfig(object):
            SQLALCHEMY_TRACK_MODIFICATIONS = True
            SQLALCHEMY_DATABASE_URI = 'sqlite://'
            DEBUG = True
            TESTING = True
            MSEARCH_INDEX_NAME = mkdtemp()
            MSEARCH_BACKEND = 'whoosh'
            MSEARCH_PRIMARY_KEY = 'pk'

        self.app = Flask(__name__)
        self.app.config.from_object(TestConfig())
        self.db = SQLAlchemy(self.app)
        self.search = Search(self.app, db=self.db)

        db = self.db

        class Post1(db.Model, ModelSaveMixin):
            __tablename__ = 'primary_posts'
            __searchable__ = ['title', 'content']

            pk = db.Column(db.Integer, primary_key=True)
            title = db.Column(db.String(49))
            content = db.Column(db.Text)

            def __repr__(self):
                return '<Post:{}>'.format(self.title)

            def save(self, db):
                if not self.pk:
                    db.session.add(self)
                db.session.commit()

        self.Post = Post1
        self.init_data()


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromNames(
        [
            'test_whoosh.TestSearch',
            # 'test_whoosh.TestPrimaryKey',
            'test_whoosh.TestCaseSearch',
            'test_whoosh.TestRelationSearch',
            'test_whoosh.TestSearchHybridProp',
            'test_whoosh.TestHybridPropTypeHint',
        ])
    unittest.TextTestRunner(verbosity=1).run(suite)
