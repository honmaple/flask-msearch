#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys

if sys.version_info[:2] >= (3, 8):
    from importlib.metadata import version
else:
    from pkg_resources import get_distribution

    def version(n):
        return get_distribution(n).version


if version("flask") < "2.3":
    from flask.helpers import locked_cached_property
else:
    from werkzeug.utils import cached_property
    locked_cached_property = cached_property

if version("flask-sqlalchemy") < "3.0":
    from flask_sqlalchemy import models_committed
else:
    from flask_sqlalchemy.track_modifications import models_committed
