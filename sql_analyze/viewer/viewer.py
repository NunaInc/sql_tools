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
"""Utility server for viewing and analyzing sql dot.

This is based on the dumb SimpleHTTPRequestHandler, it is not thread-safe
and is to be run only locally.
"""

import glob
import http.server
import json
import os
import socketserver
import tempfile
import traceback
from absl import app, flags
from sql_analyze.grammars import graphing, parse_sql_lib, statement
from typing import Any, Dict, List

FLAGS = flags.FLAGS
flags.DEFINE_integer('port', 8000, 'Port for running the http server')
flags.DEFINE_string('root_dir', os.path.join(os.path.dirname(__file__), 'web'),
                    'Root of document serving')
flags.DEFINE_list('stocksql_dirs',
                  [os.path.join(os.path.dirname(__file__), 'examples')],
                  'Directories for stock sql lookup')
flags.DEFINE_integer('max_cache_size', 10,
                     'Maximum cache of parsed sql statements')

_MAX_CACHE = 10


class SqlAnalyzeServer:
    """Global level data. Note that this is totally **NOT** thread-safe."""

    def __init__(self, max_cache_size: int, stocksql_dirs: List[str]):
        self.sql_cache = {}
        self.sql_cache_entries = []
        self.max_cache_size = max_cache_size
        self.stock_sqls = self.find_stock_sql_files(stocksql_dirs)
        self.stock_sqls_set = set(self.stock_sqls)

    def add_to_cache(self, sql, dialect, graph):
        if not self.max_cache_size:
            return
        while len(self.sql_cache_entries) > self.max_cache_size:
            del self.sql_cache[self.sql_cache_entries.pop()]
        self.sql_cache[(sql, dialect.upper())] = graph
        self.sql_cache_entries.append(sql)

    def find_stock_sql_files(self, stocksql_dirs: List[str]):
        sqls = []
        if not stocksql_dirs:
            return sqls
        for subdir in stocksql_dirs:
            sqls.extend(glob.glob(f'{subdir}/**/*.sql', recursive=True))
        return sqls

    def sql_to_graph(self, sql: str, dialect: str):
        key = (sql, dialect.upper())
        if self.max_cache_size and key in self.sql_cache:
            return self.sql_cache[key]
        sql_code = self.stock_sql(sql)
        if sql_code is None:
            sql_code = sql
        graph = parse_sql_lib.sql_to_graph(sql_code, dialect)
        self.add_to_cache(sql, dialect, graph)
        return graph

    def stock_sql(self, sql):
        if sql not in self.stock_sqls_set:
            return None
        with open(sql, mode='r', encoding='utf-8') as f:
            return f.read()


_SERVER = None


class SqlAnalyzeHandler(http.server.SimpleHTTPRequestHandler):
    """Handles requests for our http server."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=FLAGS.root_dir, **kwargs)

    def _write_response(self, content, code=200):
        self.send_response(code)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(content.encode('utf-8'))

    def do_GET(self):
        if self.path == '/stocksql':
            self._write_response('\n'.join(_SERVER.stock_sqls))
        else:
            return super().do_GET()

    def _process_sql_data(self, data: Dict[str, Any]) -> str:
        return self._generate_dot(
            _SERVER.sql_to_graph(data['sql'], data.get('dialect', 'Hive')),
            data)

    def _generate_dot(self, graph: statement.Graph, data: Dict[str,
                                                               Any]) -> str:
        options = graphing.DotOptions(int(data.get('detail', '6')))
        if all(key in data
               for key in ('selection_start_line', 'selection_start_column',
                           'selection_stop_line', 'selection_stop_column')):
            options.start_highlight = (int(data['selection_start_line']),
                                       int(data['selection_start_column']))
            options.stop_highlight = (int(data['selection_stop_line']),
                                      int(data['selection_stop_column']))
        if 'attrs' in data:
            for k, v in data['attrs'].items():
                setattr(options.attrs, k, v)
        if 'highlight_node' in data:
            node_id = int(data['highlight_node'])
            if node_id in graph.id2node:
                options.highlighted_nodes = graph.collect_linked_set(
                    graph.node_by_id(node_id))
        options.expression_line_lenght = 60
        return graphing.DotState(graph, options).build_dot()

    def _generate_svg(self, dot: str) -> str:
        tmp_dir = tempfile.mkdtemp()
        dot_file_name = os.path.join(tmp_dir, 'graph.dot')
        svg_file_name = os.path.join(tmp_dir, 'graph.svg')
        with open(dot_file_name, mode='w', encoding='utf-8') as dot_file_ob:
            dot_file_ob.write(dot)
        cmd = f'dot -T svg {dot_file_name} > {svg_file_name}'
        if os.system(cmd):
            raise ValueError(
                f'Error running dot command: "{cmd}" - may need to install dot')
        with open(svg_file_name, mode='r', encoding='utf-8') as svg_file_ob:
            return svg_file_ob.read()

    def _read_sql_data(self) -> Dict[str, Any]:
        length = int(self.headers['content-length'])
        data = self.rfile.read(length).decode('utf-8')
        data_dict = json.loads(data)
        if 'sql' not in data_dict:
            raise ValueError('No SQL provided for parsing')
        return data_dict

    def _parse_sql(self):
        try:
            data = self._read_sql_data()
            dot = self._process_sql_data(data)
            if data.get('generate_svg', 'false') == 'true':
                self._write_response(self._generate_svg(dot))
            else:
                self._write_response(dot)
        except Exception as e:  # pylint: disable=broad-except
            self._write_response(f'Error encountered: {e}', 400)
            traceback.print_exc()

    def _stock_sql(self):
        data = self._read_sql_data()
        sql = data['sql']
        sql_text = _SERVER.stock_sql(sql)
        if sql_text is None:
            raise ValueError(f'Invalid stock sql file: {sql}')
        self._write_response(sql_text)

    def do_POST(self):  # pylint: disable=invalid-name
        try:
            if self.path == '/parsesql':
                self._parse_sql()
            elif self.path == '/stocksql':
                self._stock_sql()
            else:
                f = self.send_head()
                if f:
                    f.close()
        except Exception as e:  # pylint: disable=broad-except
            self._write_response(f'Error encountered: {e}', 400)
            traceback.print_exc()


def run_server(argv):
    """Starts the viewing server - main entry point into the runnable."""
    del argv
    global _SERVER
    _SERVER = SqlAnalyzeServer(FLAGS.max_cache_size, FLAGS.stocksql_dirs)
    with socketserver.TCPServer(('', FLAGS.port), SqlAnalyzeHandler) as httpd:
        print(f'Serving at {FLAGS.port}')
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        httpd.server_close()


def main():
    app.run(run_server)


if __name__ == '__main__':
    main()
