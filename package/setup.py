#
# nuna_sql_tools: Copyright 2022 Nuna Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Nuna Sql Tools contains utilities to create and manipulate schemas and sql statements.

This package contains Python modules for helping developers
produce and maintain data analysis projects. In particular:

* `dataschema`: a library for defining data schemas using Python
dataclasses, that that can be easily converted between protobuffers,
Scala case classes, sql (ClickHouse) create table statements,
Parquet Arrow schemas and so on, from a central Python based
representation. Includes facilities to generate sample date and
compare schemas for validations.

* `sql_analyze`: a library for analyzing SQL statements. In particular
the raw SQL statments are parsed and converted to a Python based
data structure. From here, they can be converted to a data graph,
visualized, and information about the lineage of tables and columns
can be infered. Support for now SparkSql and ClickHouse dialects
for parsing.

For more details please check the
[GitHub project](https://github.com/NunaInc/sql_tools).
"""

import glob

from setuptools import find_packages
from setuptools import setup

PROJECT = 'nuna_sql_tools'
with open('VERSION', 'r', encoding='utf-8') as f:
    VERSION = f.read().strip()
with open('requirements.txt', 'r', encoding='utf-8') as f:
    lines = [l.strip() for l in f.readlines()]
    REQUIRED_PACKAGES = [l for l in lines if l and not l.startswith('#')]
DOCLINES = __doc__.split('\n')
ENTRY_POINTS = [
    'sql_analyze-viewer=sql_analyze.viewer.viewer:main',
]
PACKAGES = find_packages(where='src')
print(f'Packages: {PACKAGES}')
PACKAGE_DATA = glob.glob('src/sql_analyze/viewer/web/**/*.*', recursive=True)
print(f'Package data: {PACKAGE_DATA}')
print(f'Required packages: {REQUIRED_PACKAGES}')

setup(
    name=PROJECT,
    version=VERSION,
    description=DOCLINES[0],
    long_description='\n'.join(DOCLINES[2:]),
    long_description_content_type="text/markdown",
    url='https://github.com/NunaInc/sql_tools',
    download_url='https://github.com/NunaInc/sql_tools',
    author='Nuna Inc.',
    author_email='catalin@nuna.com',
    license='Apache 2.0',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries',
        'Programming Language :: SQL',
        'Programming Language :: Python :: 3 :: Only',
        'License :: OSI Approved :: Apache Software License',
        'Intended Audience :: Developers',
    ],
    packages=PACKAGES,
    package_dir={'': 'src'},
    package_data={'sql_analyze.viewer': PACKAGE_DATA},
    include_package_data=True,
    entry_points={'console_scripts': ENTRY_POINTS},
    install_requires=REQUIRED_PACKAGES,
    zip_safe=False,
)
