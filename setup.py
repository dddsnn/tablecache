# Copyright 2023 Marc Lehmann

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

import pathlib
import setuptools

requirements_path = (pathlib.Path(__file__).parent / 'requirements').absolute()
with (requirements_path / 'base.txt').open() as f:
    requirements = f.readlines()
with (requirements_path / 'test.txt').open() as f:
    test_requirements = f.readlines()
with (requirements_path / 'dev.txt').open() as f:
    dev_requirements = test_requirements + f.readlines()
with (pathlib.Path(__file__).parent / 'README.md').absolute().open() as f:
    readme = f.read()

setuptools.setup(
    name='tablecache', version='3.2.0',
    description='Simple cache for unwieldily joined relations.',
    long_description_content_type='text/markdown', long_description=readme,
    author="Marc Lehmann", author_email="marc.lehmann@gmx.de",
    url='https://github.com/dddsnn/tablecache', python_requires='>=3.12',
    install_requires=requirements,
    extras_require={'test': test_requirements,
                    'dev': dev_requirements}, license='AGPL-3.0-or-later')
