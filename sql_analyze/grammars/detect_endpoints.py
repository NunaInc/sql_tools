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
"""Extracts all original source and destination tables from sql files."""

import glob
import logging
import os
from absl import app, flags
from sql_analyze.grammars import parse_sql_lib, statement
from typing import Dict, List, NewType, Tuple

FLAGS = flags.FLAGS
flags.DEFINE_list('sql', '*.sql',
                  'File pattern(s) or directories for finding SQL files')


def find_sql_files(sql_patterns: List[str]):
    sqls = []
    if not sql_patterns:
        return sqls
    for s in sql_patterns:
        if os.path.isdir(s):
            sqls.extend(glob.glob(f'{s}/**/*.sql', recursive=True))
        else:
            sqls.extend(glob.glob(s))
    return sqls


def sql_to_graph(sql_file: str):
    with open(sql_file, mode='r', encoding='utf-8') as f:
        sql_code = f.read()
    logging.info('Processing: %s', sql_file)
    last_error = None
    for dialect in ('Hive', 'ClickHouse'):
        try:
            return parse_sql_lib.sql_to_graph(sql_code, dialect)
        except Exception as e:  # pylint: disable=broad-except
            last_error = e
    logging.error('Error in SQL file %s - Skipping: %s', sql_file, last_error)
    return None


FileTables = NewType('FileTables', Dict[str, List[str]])


def update_dict(d: FileTables, fname: str, tables: List[statement.Table]):
    for table in tables:
        name = table.original_name
        if name in d:
            d[name].add(fname)
        else:
            d[name] = {fname}


def process_sql(sql_patterns: List[str]) -> Tuple[FileTables, FileTables]:
    fnames = find_sql_files(sql_patterns)
    input_tables = {}
    output_tables = {}
    for fname in fnames:
        graph = sql_to_graph(fname)
        if graph is None:
            continue
        update_dict(input_tables, fname, graph.get_input_tables())
        update_dict(output_tables, fname, graph.get_output_tables())
    return (input_tables, output_tables)


def print_file_tables(d: FileTables, prefix: str):
    sorted_tables = list(d.keys())
    sorted_tables.sort()
    for k in sorted_tables:
        print(f'{prefix} {k}')
        v = list(d[k])
        v.sort()
        for f in v:
            print(f'F    {f}')


def run(argv):
    del argv
    (inputs, outputs) = process_sql(FLAGS.sql)
    print_file_tables(inputs, 'I')
    print_file_tables(outputs, 'O')


def main():
    app.run(run)


if __name__ == '__main__':
    main()
