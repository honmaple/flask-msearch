#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ********************************************************************************
# Copyright © 2019-2020 jianglin
# File Name: signal.py
# Author: jianglin
# Email: mail@honmaple.com
# Created: 2019-09-11 00:02:50 (CST)
# Last Update: Monday 2020-03-09 16:49:32 (CST)
#          By:
# Description:
# ********************************************************************************


def default_signal(backend, sender, changes):
    '''
    Flask-SQLalchemy default signal commit function, it can be customized by:
    ```
     # app.py
     app.config["MSEARCH_INDEX_SIGNAL"] = celery_signal
     # or use string as variable
     app.config["MSEARCH_INDEX_SIGNAL"] = "modulename.tasks.celery_signal"
     search = Search(app)

     # tasks.py
     from flask_msearch.signal import default_signal

     @celery.task(bind=True)
     def celery_signal_task(self, backend, sender, changes):
         default_signal(backend, sender, changes)
         return str(self.request.id)

     def celery_signal(backend, sender, changes):
         return celery_signal_task.delay(backend, sender, changes)
    ```
    '''
    for change in changes:
        instance = change[0]
        operation = change[1]
        if hasattr(instance, '__searchable__'):
            if operation == 'insert':
                backend.create_one_index(instance)
            elif operation == 'update':
                backend.create_one_index(instance, update=True)
            elif operation == 'delete':
                backend.create_one_index(instance, delete=True)

        delete = True if operation == 'delete' else False
        prepare = [i for i in dir(instance) if i.startswith('msearch_')]
        for p in prepare:
            attrs = getattr(instance, p)(delete=delete)
            ix = backend.index(attrs.pop('_index'))
            if attrs['attrs']:
                for attr in attrs['attrs']:
                    ix.update(**backend._fields(ix, attr))
                ix.commit()


def celery_signal(backend, sender, changes):
    return default_signal(backend, sender, changes)
