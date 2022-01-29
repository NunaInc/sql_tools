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
"""Flags for parsing SQL statements and getting insights."""
from absl import flags
from sql_analyze.grammars import parse_sql_lib

FLAGS = flags.FLAGS
flags.DEFINE_string('input', None,
                    'Input sql statement file, or - to read from stdin')
flags.DEFINE_bool('print_input', False,
                  'Prints the sql that we read from input.')
flags.DEFINE_bool('print_parsed_tree', False,
                  'Prints a recomposition of the parse tree.')
flags.DEFINE_bool('print_parsed_sql', False,
                  'Prints a recomposition to SQL of the parse tree.')
flags.DEFINE_bool('print_tree', False, 'Prints the extracted statement tree.')
flags.DEFINE_bool('print_kids', False,
                  'Prints the extracted statement child structure.')
flags.DEFINE_bool('print_sql', False, 'Prints the recomposed statement sql.')
flags.DEFINE_bool('print_all', False, 'Prints everything.')
flags.DEFINE_bool('view_graph', False, 'View the corresponding statement graph')
flags.DEFINE_integer(
    'graph_detail', 5,
    'What to view in the graph. Between 0 (minimal detail) and 10 (full detail)'
)
flags.DEFINE_string(
    'graph_dir', 'detect',
    'Direction of the graph LR - Left->Right, TB - Top->Bottom, '
    'other options BT, RL. If `detect` we try to figure out the '
    'best look for the generated graph.')
flags.DEFINE_string('graph_url', '',
                    'When specified, we link the graph title to this URL')
flags.DEFINE_string(
    'dot_file', '',
    'Generate the dot file to this location, if unused, we create '
    'a temporary.')
flags.DEFINE_bool(
    'dot_mute_style', False,
    'When true, all style attributes are removed in the dot file.')

flags.DEFINE_string(
    'svg_file', '',
    'Generate the svg file to this location, if unused, we create '
    'a temporary.')
flags.DEFINE_bool(
    'open_svg', True,
    'Open the generated svg graph (on mac in your browser w/ open)')
flags.DEFINE_integer('line_len', 60,
                     'Format expressions to this maximum line length')
flags.DEFINE_string(
    'dialect', 'Hive', 'The dialect to parse. Use: `Hive` for Hive/Spark, '
    '`ClickHouse` for ClickHouse')


def parse_options_from_flags():
    return parse_sql_lib.ParseSqlOptions(
        dialect=FLAGS.dialect,
        input_file=FLAGS.input,
        input_str=None,
        print_input=FLAGS.print_input,
        print_parsed_tree=FLAGS.print_parsed_tree,
        print_parsed_sql=FLAGS.print_parsed_sql,
        print_tree=FLAGS.print_tree,
        print_kids=FLAGS.print_kids,
        print_sql=FLAGS.print_sql,
        print_all=FLAGS.print_all,
        view_graph=FLAGS.view_graph,
        graph_detail=FLAGS.graph_detail,
        graph_dir=FLAGS.graph_dir,
        graph_url=FLAGS.graph_url,
        dot_mute_style=FLAGS.dot_mute_style,
        dot_file=FLAGS.dot_file,
        svg_file=FLAGS.svg_file,
        open_svg=FLAGS.open_svg,
        line_len=FLAGS.line_len)
