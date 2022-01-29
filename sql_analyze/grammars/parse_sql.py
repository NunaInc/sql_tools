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
"""Utility to parse and generate svg files for sql file(s)"""
import os
import sys
from absl import app, flags
from sql_analyze.grammars import parse_sql_flags, parse_sql_lib

FLAGS = flags.FLAGS

flags.DEFINE_string('input_file_list', None,
                    'A file containing files to process - one per line.')


def main(argv):
    del argv
    opt = parse_sql_flags.parse_options_from_flags()
    if opt.input_file and FLAGS.input_file_list:
        sys.exit('Don`t specify both --input and --input_file_list')
    svg_dir = None
    dot_dir = None
    url_prefix = None
    if opt.input_file:
        files = [opt.input_file]
    elif FLAGS.input_file_list:
        with open(FLAGS.input_file_list, 'r', encoding='utf-8') as f:
            files = [fn.strip() for fn in f.readlines()]
        svg_dir = opt.svg_file
        dot_dir = opt.dot_file
        url_prefix = opt.graph_url
    else:
        sys.exit('Specify either --input and --input_file_list')

    for file_name in files:
        print(f'Processing: {opt.input_file}')
        opt.input_file = file_name
        escaped_file_root = file_name.replace('/', '_')
        if escaped_file_root.endswith('.sql'):
            escaped_file_root = escaped_file_root[:-4]
        if svg_dir:
            opt.svg_file = os.path.join(svg_dir, f'{escaped_file_root}.svg')
        if dot_dir:
            opt.dot_file = os.path.join(dot_dir, f'{escaped_file_root}.dot')
        if url_prefix:
            opt.graph_url = f'{url_prefix}/{file_name}'
        result = parse_sql_lib.parse_sql(opt)
        if result.errors:
            error_str = (f'Errors encountered for: {file_name}: ' +
                         '\n'.join(result.errors))
            sys.stderr.write(f'{error_str}\n')
        if result.dot_file:
            print(f'DOT file: {result.dot_file}')
        if result.svg_file:
            print(f'SVG file: {result.svg_file}')


if __name__ == '__main__':
    app.run(main)
