import pathlib
import sys
sys.path.insert(0, pathlib.Path(__file__).parents[2].resolve().as_posix())

from tablecache import __version__ as tablecache_version  # noqa


project = 'tablecache'
copyright = '2023, 2024, Marc Lehmann'
author = 'Marc Lehmann'
version = tablecache_version
release = tablecache_version

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.intersphinx',
    'myst_parser',
]

templates_path = ['_templates']
exclude_patterns = []

html_theme = 'alabaster'
html_static_path = []

intersphinx_mapping = {
    'asyncpg': ('https://magicstack.github.io/asyncpg/current', None),
    'sortedcontainers': ('https://grantjenks.com/docs/sortedcontainers', None),
    'redis': ('https://redis.readthedocs.io/en/stable', None),
}
