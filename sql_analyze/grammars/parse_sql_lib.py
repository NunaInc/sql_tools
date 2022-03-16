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
"""Utility to parse a SQL statement, and maybe print it."""

import sys

from antlr4.ParserRuleContext import ParserRuleContext
from dataclasses import dataclass
from sql_analyze.grammars import graphing, statement, trees
from sql_analyze.grammars.Hive import parse_sql_lib as parse_sql_hive
from sql_analyze.grammars.ClickHouse import parse_sql_lib as parse_sql_clickhouse
from typing import List, Optional


@dataclass
class ParseSqlOptions:
    """Options for parsing (and printing) an SQL statement."""
    dialect: str = None
    input_file: str = None
    input_str: str = None
    print_input: bool = False
    print_parsed_tree: bool = False
    print_parsed_sql: bool = False
    print_tree: bool = False
    print_kids: bool = False
    print_sql: bool = False
    print_all: bool = False
    view_graph: bool = False
    graph_detail: str = 'normal'
    graph_dir: str = 'detect'
    graph_url: str = ''
    dot_mute_style: bool = False
    dot_file: str = ''
    svg_file: str = ''
    open_svg: bool = True
    line_len: int = 60


class ParseSqlResult:
    sql_statement: str = None
    tree: ParserRuleContext = None
    queries: List[statement.Query] = None
    errors: List[str] = []
    dot_file: Optional[str] = None
    svg_file: Optional[str] = None


def _print_info(title: str, body: str):
    print('=' * 80)
    print(f'\n====> {title}:\n{body}\n')


def parse_sql(opt: ParseSqlOptions):
    input_file = None
    result = ParseSqlResult()
    if opt.input_str:
        result.sql_statement = opt.input_str
    elif opt.input_file == '-':
        result.sql_statement = sys.stdin.read()
    elif opt.input_file:
        with open(opt.input_file, mode='r', encoding='utf-8') as f:
            result.sql_statement = f.read()
        input_file = opt.input_file
    if opt.dialect == 'Hive':
        (result.tree, parser,
         result.queries) = parse_sql_hive.parse_hive_sql_statement(
             result.sql_statement)
    elif opt.dialect == 'ClickHouse':
        (result.tree, parser,
         result.queries) = parse_sql_clickhouse.parse_clickhouse_sql_statement(
             result.sql_statement)
    else:
        raise ValueError(f'Unknown SQL dialect `${opt.dialect}`')
    if opt.print_input or opt.print_all:
        _print_info('SELECT STATEMENT', result.sql_statement)
    if opt.print_parsed_tree or opt.print_all:
        _print_info('PARSE TREE', trees.to_string(result.tree, parser))
    if opt.print_parsed_sql or opt.print_all:
        _print_info('RECOMPOSED PARSED SQL', trees.recompose(result.tree))
    result.errors = trees.find_errors(result.tree, result.sql_statement)
    if not result.queries:
        print('No queries.')
        return result
    if opt.print_tree or opt.print_all:
        _print_info('EXTRACTED TREE(s)',
                    '\n'.join(query.tree_str() for query in result.queries))
    if opt.print_kids or opt.print_all:
        _print_info('SELECT STRUCTURE',
                    '\n'.join(query.children_str() for query in result.queries))
    if opt.print_sql or opt.print_all:
        _print_info('RECOMPOSED SQL',
                    '\n'.join(query.recompose(' ') for query in result.queries))
    if opt.view_graph:
        for query in result.queries:
            print(f'Graphing query {query}')
            (result.dot_file, result.svg_file) = graphing.generate_svg(
                query,
                dot_file=opt.dot_file,
                svg_file=opt.svg_file,
                graph_dir=opt.graph_dir,
                graph_detail=opt.graph_detail,
                open_file=opt.open_svg,
                label=f'From File: {input_file}',
                url=opt.graph_url,
                line_len=opt.line_len,
                mute_style=opt.dot_mute_style)
    return result


def sql_to_queries(
    sql_code: str,
    dialect: str,
    schema_provider: Optional[statement.SchemaProvider] = None
) -> List[statement.Source]:
    if dialect.upper() in ('SPARKSQL', 'HIVE'):
        tree, _, queries = parse_sql_hive.parse_hive_sql_statement(sql_code)
    elif dialect.upper() in ('CLICKHOUSE'):
        tree, _, queries = parse_sql_clickhouse.parse_clickhouse_sql_statement(
            sql_code)
    else:
        raise ValueError(f'Unknown SQL dialect `{dialect}`')
    errors = trees.find_errors(tree, sql_code)
    if errors:
        errors_str = '\n'.join(errors)
        raise ValueError(f'Errors in SQL:\n {errors_str}')
    if not schema_provider:
        return queries
    start = trees.CodeLocation(0, 1, 0)
    for query in queries:
        query.apply_schemas(
            statement.SchemaInfo.from_statement(
                trees.code_extract(sql_code, start, query.stop),
                schema_provider))
        start = query.stop
    return queries


def sql_to_graph(
    sql_code: str,
    dialect: str,
    schema_provider: Optional[statement.SchemaProvider] = None
) -> statement.Graph:
    """Converts a sql statement to a statement Graph.

    Arguments:
        sql_code: sql code to process.
        dialect: dialect of sql to use.
    """
    queries = sql_to_queries(sql_code, dialect, schema_provider)
    graph = statement.Graph()
    for query in queries:
        graph.populate(query)
        query.link_graph(graph, [], set())
    return graph
