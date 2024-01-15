#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from flask.helpers import locked_cached_property
except ImportError:
    from werkzeug.utils import cached_property
    locked_cached_property = cached_property
