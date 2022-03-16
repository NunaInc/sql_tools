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
"""Tests if parsing happens correctly for various sql statements."""
import unittest
from dataclasses import dataclass
from dataschema import Schema, python2schema, schema2sql
from sql_analyze.grammars import parse_sql_lib, statement, trees
from sql_analyze.grammars.Hive import parse_sql_lib as parse_sql_hive
from sql_analyze.grammars.ClickHouse import parse_sql_lib as parse_sql_ch
from typing import Optional


@dataclass
class A:
    """Schema example for test."""
    foo: int
    bar: str


@dataclass
class B:
    """Schema example for test."""
    baz: Optional[str]
    qux: int


class TestSchemaProvider(statement.SchemaProvider):
    """Schema provider used for testing."""

    def __init__(self):
        super().__init__()
        self.schemas = {
            'A': python2schema.ConvertDataclass(A),
            'B': python2schema.ConvertDataclass(B),
        }

    def find_schema(self, name: str) -> Optional[Schema.Table]:
        if name in self.schemas:
            return self.schemas[name]
        return None


class ParseSqlTest(unittest.TestCase):

    def test_parse_simple(self):
        self.recompose_test('SELECT foo FROM bar')
        self.recompose_test('SELECT foo AS baz FROM bar')
        self.recompose_test('SELECT baz.foo FROM bar AS baz')
        self.recompose_test('SELECT * FROM bar')
        self.recompose_test('SELECT bar.* FROM bar')
        self.recompose_test('SELECT foo, bar.* FROM bar')
        self.recompose_test('SELECT foo FROM bar WHERE baz = 10')
        self.recompose_test('SELECT COUNT(1) FROM baz WHERE qux > 1')
        self.recompose_test('SELECT DISTINCT foo FROM bar')
        self.ch_recompose_test('SELECT DISTINCT TOP DECIMAL_LITERAL '
                               'foo FROM bar')
        self.ch_recompose_test('SELECT foo FROM generate(source(bar), baz)')

    def test_parse_clauses(self):
        self.recompose_test('SELECT foo, bar FROM baz GROUP BY foo')
        self.recompose_test('SELECT foo, bar FROM baz GROUP BY foo, bar')
        self.recompose_test('SELECT foo, bar FROM baz GROUP BY (1)')
        self.recompose_test('SELECT foo, bar FROM baz GROUP BY (1, 2)')
        self.recompose_test('SELECT foo, bar FROM baz GROUP BY ROLLUP (foo)')
        self.recompose_test('SELECT foo, bar FROM baz GROUP BY foo WITH ROLLUP')
        self.recompose_test('SELECT foo, bar FROM baz HAVING foo > 10')
        self.recompose_test('SELECT foo, bar FROM baz ORDER BY foo ASC')
        self.recompose_test('SELECT foo, bar FROM baz ORDER BY foo NULLS FIRST')
        self.recompose_test(
            'SELECT foo, bar FROM baz ORDER BY foo DESC NULLS FIRST')
        self.hive_recompose_test(
            'SELECT foo, bar FROM baz ORDER BY foo SORT BY bar')
        self.hive_recompose_test(
            'SELECT foo, bar FROM baz SORT BY bar DESC NULLS FIRST')
        self.hive_recompose_test('SELECT foo, bar FROM baz CLUSTER BY bar')
        self.hive_recompose_test('SELECT foo, bar FROM baz CLUSTER BY foo, bar')
        self.hive_recompose_test('SELECT foo, bar FROM baz DISTRIBUTE BY bar')
        self.hive_recompose_test(
            'SELECT foo, bar FROM baz DISTRIBUTE BY foo, bar')
        self.hive_recompose_test(
            'SELECT foo, bar FROM baz GROUP BY foo '
            'WITH ROLLUP GROUPING SETS ((foo), (foo, bar), ()) DISTRIBUTE BY bar'
        )
        self.ch_recompose_test(
            'SELECT foo FROM bar FINAL SAMPLE 2 / 3 OFFSET 1 / 2')
        self.ch_recompose_test('SELECT foo, bar FROM baz GROUP BY CUBE (foo) '
                               'WITH ROLLUP WITH TOTALS ORDER BY bar')
        self.ch_recompose_test('SELECT foo, bar FROM baz ORDER BY foo '
                               'DESC NULLS FIRST COLLATE \'qux\'')

    def test_parse_subquery(self):
        self.recompose_test('SELECT bar + 10 AS foo FROM (SELECT bar FROM baz)')
        self.recompose_test('SELECT foo FROM bar WHERE foo IN '
                            '(SELECT bazkey FROM baz WHERE qux > 1)')
        self.recompose_test('SELECT foo FROM bar WHERE EXISTS'
                            '((SELECT COUNT(1) FROM baz WHERE qux > 1))')

    def test_parse_join(self):
        self.recompose_test(
            'SELECT foo FROM bar LEFT JOIN baz ON (bar.qux = baz.qux)')
        self.recompose_test('SELECT foo FROM bar LEFT JOIN baz USING (qux)')
        self.recompose_test(
            'SELECT foo FROM bar LEFT JOIN baz USING (qux, mux)')
        self.recompose_test(
            'SELECT foo FROM (bar LEFT JOIN baz ON (bar.qux = baz.qux)) '
            'JOIN extra ON bar.foo = extra.foo')
        self.ch_recompose_test('SELECT foo FROM bar LOCAL CROSS JOIN baz')
        self.ch_recompose_test(
            'SELECT foo FROM bar LEFT ARRAY JOIN baz(x), qux(y)')

    def test_parse_with(self):
        self.recompose_test('WITH baz AS (SELECT foo FROM bar) '
                            'SELECT x FROM source '
                            'LEFT JOIN baz ON baz.key = source.key '
                            'WHERE y > 10')
        self.ch_recompose_test('WITH call(20) AS bar ' 'SELECT x FROM source')

    def test_parse_laterals(self):
        self.hive_recompose_test(
            'SELECT * FROM person PIVOT ('
            'SUM(age) AS a, AVG(class) AS c '
            'FOR name IN (\'John\' AS john, \'Mike\' AS mike))')
        self.hive_recompose_test(
            'SELECT * FROM ('
            'SELECT course, earnings, '
            '"a" AS a, "b" AS b, "c" AS c FROM courseSales) '
            'PIVOT (sum(Earnings) FOR Course IN (\'dotNET\', \'Java\'))')
        self.hive_recompose_test(
            'SELECT * FROM ('
            'SELECT earnings, year, s FROM courseSales '
            'JOIN yearsWithComplexTypes ON year = y) '
            'PIVOT (sum(earnings) FOR s IN ((1, \'a\'), (2, \'b\')))')
        self.hive_recompose_test(
            'SELECT * FROM person '
            'LATERAL VIEW EXPLODE(ARRAY(30, 60)) tableName AS c_age, '
            'LATERAL VIEW OUTER EXPLODE(ARRAY(40, 80)) AS d_age')
        self.hive_recompose_test(
            'SELECT * FROM person '
            'LATERAL TABLE (VALUES (\'one\', 1), (\'two\', 2)) '
            'AS numbers (name, value)')

    def test_parse_limit(self):
        self.recompose_test('SELECT foo FROM bar LIMIT 10')
        self.recompose_test('SELECT foo FROM bar LIMIT 10 OFFSET 20')
        self.ch_recompose_test(
            'SELECT foo FROM bar LIMIT 10 OFFSET 20 WITH TIES')
        self.ch_recompose_test(
            'SELECT foo FROM bar LIMIT 10 OFFSET 20 BY check(baz)')

    def test_multi_statement(self):
        _, _, queries = parse_sql_ch.parse_clickhouse_sql_statement(
            'SELECT foo FROM bar; SELECT baz FROM qux')
        self.assertEqual(len(queries), 2)
        self.assertEqual(queries[0].recompose(), 'SELECT foo FROM bar')
        self.assertEqual(queries[1].recompose(), 'SELECT baz FROM qux')
        _, _, queries = parse_sql_hive.parse_hive_sql_statement(
            'SELECT foo FROM bar; SELECT baz FROM qux')
        self.assertEqual(len(queries), 2)
        self.assertEqual(queries[0].recompose(), 'SELECT foo FROM bar')
        self.assertEqual(queries[1].recompose(), 'SELECT baz FROM qux')

    def test_inserts(self):
        _, _, stmts = parse_sql_hive.parse_hive_sql_statement(
            'CREATE OR REPLACE TEMPORARY VIEW foo_view AS '
            'SELECT foo FROM bar')
        self.assertEqual(len(stmts), 1)
        self.assertEqual(stmts[0].name, 'CREATE TEMPORARY VIEW')
        self.assertTrue(isinstance(stmts[0], statement.Create))
        query = stmts[0].query
        self.assertEqual(query.destination.name, 'foo_view')
        self.assertEqual(query.name, 'foo_view')
        self.assertEqual(query.recompose(), 'SELECT foo FROM bar')
        self.assertEqual(stmts[0].destination.name, 'foo_view')
        _, _, stmts = parse_sql_hive.parse_hive_sql_statement(
            'CREATE OR REPLACE TEMP VIEW foo_view '
            'USING csv OPTIONS(path "/foo/bar.csv")')
        self.assertEqual(len(stmts), 1)
        self.assertEqual(stmts[0].name, 'CREATE TEMPORARY VIEW')
        self.assertTrue(isinstance(stmts[0], statement.Create))
        self.assertEqual(stmts[0].destination.name, 'foo_view')
        self.assertEqual(stmts[0].input_format, 'CSV')
        self.assertEqual(stmts[0].input_path, '/foo/bar.csv')
        _, _, stmts = parse_sql_hive.parse_hive_sql_statement(
            'CREATE MATERIALIZED VIEW foo_view AS '
            'SELECT foo FROM bar')
        self.assertEqual(len(stmts), 1)
        self.assertEqual(stmts[0].name, 'CREATE MATERIALIZED VIEW')
        self.assertTrue(isinstance(stmts[0], statement.Create))
        self.assertEqual(stmts[0].destination.name, 'foo_view')
        query = stmts[0].query
        self.assertEqual(query.destination.name, 'foo_view')
        self.assertEqual(query.name, 'foo_view')
        self.assertEqual(query.recompose(), 'SELECT foo FROM bar')
        _, _, stmts = parse_sql_hive.parse_hive_sql_statement(
            'CREATE TABLE foo_table AS '
            'SELECT foo FROM bar')
        self.assertEqual(len(stmts), 1)
        self.assertEqual(stmts[0].name, 'CREATE TABLE')
        self.assertTrue(isinstance(stmts[0], statement.Create))
        self.assertEqual(stmts[0].destination.name, 'foo_table')
        query = stmts[0].query
        self.assertEqual(query.destination.name, 'foo_table')
        self.assertEqual(query.name, 'foo_table')
        self.assertEqual(query.recompose(), 'SELECT foo FROM bar')
        _, _, stmts = parse_sql_ch.parse_clickhouse_sql_statement(
            'INSERT INTO TABLE foo_table '
            'SELECT foo FROM bar')
        self.assertEqual(len(stmts), 1)
        self.assertEqual(stmts[0].name, 'INSERT INTO TABLE')
        self.assertTrue(isinstance(stmts[0], statement.Insert))
        self.assertEqual(stmts[0].destination.name, 'foo_table')
        query = stmts[0].query
        self.assertEqual(query.destination.name, 'foo_table')
        self.assertEqual(query.name, 'foo_table')
        self.assertEqual(query.recompose(), 'SELECT foo FROM bar')
        _, _, stmts = parse_sql_ch.parse_clickhouse_sql_statement(
            'INSERT INTO FUNCTION generate(foo_table) '
            'SELECT foo FROM bar')
        self.assertEqual(stmts[0].name, 'INSERT INTO FUNCTION')
        self.assertTrue(isinstance(stmts[0], statement.Insert))
        self.assertTrue(isinstance(stmts[0].destination, statement.Expression))
        query = stmts[0].query
        self.assertEqual(query.destination.recompose(), 'generate(foo_table)')
        self.assertEqual(query.name, None)
        self.assertEqual(query.recompose(), 'SELECT foo FROM bar')

    def test_ch_over(self):
        self.ch_recompose_test(
            'SELECT xid, date, action, NULL AS nf, '
            'rank() OVER (PARTITION BY xid ORDER BY date) AS rank '
            'FROM ${database}.foo_table '
            'WHERE toYear(${datefield}) = 2022')

        self.ch_recompose_test(
            'SELECT number, avg(number) OVER '
            '(ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS mv4 '
            'FROM foo')

    def test_create_schema(self):
        stmt = parse_sql_ch.parse_clickhouse_sql_create(
            'CREATE TABLE foo.bar ('
            'id Int64, '
            'fint32 UInt32 CODEC(Delta(2), ZSTD(7)), '
            'fsint64 Nullable(Int64), '
            'fdate Nullable(Date), '
            'ftimestamp Nullable(DateTime64(3, "Etc/UTC")), '
            'finitialized Array(String), '
            'fwidth FixedString(32), '
            'fenum Enum(\'foo\' = 1, \'bar\' = 2) '
            ') '
            'ENGINE = MergeTree() '
            'ORDER BY (id, fsint32) '
            'PARTITION BY (toYYYYMM(fdate)) '
            'SETTINGS index_granularity = 8192')
        self.assertTrue(stmt is not None)
        self.assertTrue(stmt.schema is not None)
        sql = schema2sql.TableConverter(
            stmt.schema).to_sql(table_name=stmt.schema.info.full_name)
        self.assertEqual(
            sql, """CREATE TABLE foo.bar (
  id Int64,
  fint32 UInt32 CODEC(Delta(2), ZSTD(7)),
  fsint64 Nullable(Int64),
  fdate Nullable(Date),
  ftimestamp Nullable(DateTime64(3, "Etc/UTC")),
  finitialized Array(String),
  fwidth String,
  fenum LowCardinality(String)
)

ENGINE = MergeTree()
ORDER BY (id, fsint32)
PARTITION BY (toYYYYMM(fdate))
SETTINGS index_granularity = 8192""")

    def recompose_test(self, sql: str, print_tree: bool = False):
        self.hive_recompose_test(sql, print_tree)
        self.ch_recompose_test(sql, print_tree)

    def hive_recompose_test(self, sql: str, print_tree: bool = False):
        tree, parser, queries = parse_sql_hive.parse_hive_sql_statement(sql)
        if print_tree:
            print(f'{sql}\n{trees.to_string(tree, parser)}')
        errors = trees.find_errors(tree, sql)
        self.assertTrue(not errors,
                        f'Errors found in Hive {sql}: ' + '\n'.join(errors))
        self.assertEqual(len(queries), 1, f'For Hive {sql}')
        self.assertEqual(queries[0].recompose(), sql, 'For Hive format')

    def ch_recompose_test(self, sql: str, print_tree: bool = False):
        tree, parser, queries = parse_sql_ch.parse_clickhouse_sql_statement(sql)
        if print_tree:
            print(f'{sql}\n{trees.to_string(tree, parser)}')
        errors = trees.find_errors(tree, sql)
        self.assertTrue(
            not errors,
            f'Errors found in ClickHouse {sql}: ' + '\n'.join(errors))
        self.assertEqual(len(queries), 1, f'For Clickhouse {sql}')
        self.assertEqual(queries[0].recompose(), sql, 'For ClickHouse format')

    def test_attach_schema(self):
        provider = TestSchemaProvider()
        queries = parse_sql_lib.sql_to_queries(
            """
--@Schema: input B
--@Output: A
CREATE TABLE boxes as
SELECT
  COALESCE(input.baz, "") as bar,
  input.qux as foo
FROM input WHERE input.qux > 2000;

--@Schema: boxes A
--@Schema: foxes B
--@Output: B
SELECT boxes.foo as qux, CONCAT("b_", foxes.baz)
FROM boxes LEFT JOIN foxes on boxes.bar = foxes.qux;
""", 'SPARKSQL', provider)
        self.assertEqual(len(queries), 2)
        src1 = statement.find_end_sources(queries[0])
        self.assertEqual(len(src1), 1)
        self.assertEqual(src1[0].name, 'input')
        self.assertEqual(src1[0].schema, provider.schemas['B'])
        dest1 = statement.find_destination(queries[0])
        self.assertEqual(dest1.name, 'boxes')
        self.assertEqual(dest1.schema, provider.schemas['A'])
        src2 = statement.find_end_sources(queries[1])
        self.assertEqual(len(src2), 2)
        self.assertEqual(src2[0].name, 'boxes')
        self.assertEqual(src2[0].schema, provider.schemas['A'])
        self.assertEqual(src2[1].name, 'foxes')
        self.assertEqual(src2[1].schema, provider.schemas['B'])
        dest2 = statement.find_destination(queries[1])
        self.assertEqual(dest2, None)


if __name__ == '__main__':
    unittest.main()
