# Copyright 2023, 2024 Marc Lehmann

# This file is part of tablecache.
#
# tablecache is free software: you can redistribute it and/or modify it under
# the terms of the GNU Affero General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# tablecache is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
# details.
#
# You should have received a copy of the GNU Affero General Public License
# along with tablecache. If not, see <https://www.gnu.org/licenses/>.

import itertools as it
import pathlib
import setuptools

from tablecache import __version__ as tablecache_version

requirements_path = (pathlib.Path(__file__).parent / 'requirements').absolute()
requirements = {}
for requirements_file in requirements_path.glob('*.txt'):
    with requirements_file.open() as f:
        requirements[requirements_file.stem] = frozenset(f.readlines())
requirements['test'] = frozenset(it.chain(
    *[reqs for extra, reqs in requirements.items()
      if extra not in ['base', 'dev', 'docs']]))
requirements['dev'] = frozenset(it.chain(
    *[reqs for extra, reqs in requirements.items()if extra != 'base']))
extras_requirements = {extra: reqs for extra,
                       reqs in requirements.items() if extra != 'base'}
with (pathlib.Path(__file__).parent / 'README.md').absolute().open() as f:
    readme = f.read()

setuptools.setup(
    name='tablecache', version=tablecache_version,
    description='Simple cache for unwieldily joined relations.',
    long_description_content_type='text/markdown', long_description=readme,
    author="Marc Lehmann", author_email="marc.lehmann@gmx.de",
    project_urls={
        'Github': 'https://github.com/dddsnn/tablecache',
        'Documentation': 'https://tablecache.readthedocs.io'},
    python_requires='>=3.12', install_requires=requirements['base'],
    extras_require=extras_requirements, license='AGPL-3.0-or-later')
