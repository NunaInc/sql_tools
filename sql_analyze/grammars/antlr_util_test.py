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
"""Tests the antlr utilities for manipulating trees and tokens."""
import unittest
from sql_analyze.grammars import trees, tokens
from sql_analyze.grammars.Hive import parse_sql_lib as parse_sql_hive

SQL = 'SELECT fun(foo.bar + foo.baz) AS qux FROM some_table as foo'
EXP = """
CASE WHEN bus_book_cd != 'MA' AND
((lob_cd = 'ILOW' AND upper(rate_area) IN ('RG11','RG13') AND YEAR(cvr_mnth_dt) = 2019)
OR (lob_cd = 'BHOM' AND YEAR(cvr_mnth_dt) >= 2020))
THEN 'Blue Premier UNC Health Alliance'
WHEN BLUE_METRO_OVRWRT_CAT_TXT = 'Novant_Risk_Pool'
THEN 'Novant Blue Value'
WHEN (YEAR(cvr_mnth_dt) = 2019 AND
BLUE_METRO_OVRWRT_CAT_TXT = 'CHS_Atrium_Risk_Pool')
THEN 'Blue Metro Atrium'
WHEN (YEAR(cvr_mnth_dt) in (2020,2021)
and BLUE_METRO_OVRWRT_CAT_TXT = 'CHS_Atrium_Risk_Pool'
and segment_rammed = 'INDIVIDUAL-ACA' and LOB_CD = 'ICLT') THEN 'Atrium Blue Local U65'
WHEN (YEAR(cvr_mnth_dt) = 2020 and BLUE_METRO_OVRWRT_CAT_TXT = 'CHS_Atrium_Risk_Pool'
and segment_rammed <> 'INDIVIDUAL-ACA' and LOB_CD = 'CHLT')
THEN 'Blue Premier Atrium Health'
WHEN (YEAR(cvr_mnth_dt) = 2021 and BLUE_METRO_OVRWRT_CAT_TXT = 'Atrium CHLT Risk Pool')
THEN 'Blue Premier Atrium Health'
WHEN FIN_ACCTBL_IND = 'Y' THEN attr_sys_nm_bp
ELSE null END as health_system_id
"""


class AntlrUtilTest(unittest.TestCase):

    def test_tree_string(self):
        (tree, parser) = parse_sql_hive.parse_hive_sql_tree(SQL)
        s = trees.to_string(tree, parser)
        self.assertTrue(
            s.startswith("""+-- R statements
  +-- R statement
  |     +-- R execStatement
  |         +-- R queryStatementExpression
  |             +-- R queryStatementExpressionBody"""))
        recomposed = trees.recompose(tree)
        self.assertEqual(recomposed, SQL)

    def test_block(self):
        block = tokens.to_block(
            tokens.from_tree(parse_sql_hive.parse_hive_sql_tree(SQL)[0]))
        self.assertEqual(
            str(block), '{<`SELECT` `fun` {`(` '
            '<`foo` `.` `bar` `+` `foo` `.` `baz` `)`>}'
            ' `AS` `qux` `FROM` `some_table` `as` `foo`>}')
        self.assertEqual(block.recompose(), SQL)
        self.assertEqual(
            block.format(30), 'SELECT fun(foo.bar + foo.baz)\n'
            'AS qux FROM some_table as foo')
        self.assertEqual(
            block.format(20), 'SELECT fun(\n'
            '    foo.bar + foo.baz)\n'
            'AS qux FROM\n'
            'some_table as foo')
        self.assertEqual(block.format(80), SQL)

    def block(self, exp: str):
        tree, _ = parse_sql_hive.parse_hive_expression(exp)
        return tokens.to_block(tokens.from_tree(tree))

    def test_exp_block(self):
        block = self.block(EXP)
        self.assertEqual(
            block.format(50), """
CASE WHEN bus_book_cd != 'MA' AND (
        (lob_cd = 'ILOW' AND upper(rate_area) IN (
                'RG11', 'RG13') AND YEAR(
                cvr_mnth_dt) = 2019) OR (
            lob_cd = 'BHOM' AND YEAR(cvr_mnth_dt)
            >= 2020)) THEN
    'Blue Premier UNC Health Alliance'
WHEN BLUE_METRO_OVRWRT_CAT_TXT =
    'Novant_Risk_Pool' THEN 'Novant Blue Value'
WHEN (YEAR(cvr_mnth_dt) = 2019 AND
        BLUE_METRO_OVRWRT_CAT_TXT =
        'CHS_Atrium_Risk_Pool') THEN
    'Blue Metro Atrium'
WHEN (YEAR(cvr_mnth_dt) in (2020, 2021) and
        BLUE_METRO_OVRWRT_CAT_TXT =
        'CHS_Atrium_Risk_Pool' and segment_rammed =
        'INDIVIDUAL-ACA' and LOB_CD = 'ICLT') THEN
    'Atrium Blue Local U65'
WHEN (YEAR(cvr_mnth_dt) = 2020 and
        BLUE_METRO_OVRWRT_CAT_TXT =
        'CHS_Atrium_Risk_Pool' and segment_rammed <>
        'INDIVIDUAL-ACA' and LOB_CD = 'CHLT') THEN
    'Blue Premier Atrium Health'
WHEN (YEAR(cvr_mnth_dt) = 2021 and
        BLUE_METRO_OVRWRT_CAT_TXT =
        'Atrium CHLT Risk Pool') THEN
    'Blue Premier Atrium Health'
WHEN FIN_ACCTBL_IND = 'Y' THEN attr_sys_nm_bp
ELSE null END""".strip())

    def test_exp_block2(self):
        block = self.block('foo(bar(baz(' + ', '.join(
            f'argument_{i} as column_{i}' for i in range(10)) + ')))')
        self.assertEqual(
            block.format(80), """
foo(bar(baz(argument_0 as column_0, argument_1 as column_1,
            argument_2 as column_2, argument_3 as column_3,
            argument_4 as column_4, argument_5 as column_5,
            argument_6 as column_6, argument_7 as column_7,
            argument_8 as column_8, argument_9 as column_9)))""".strip())


if __name__ == '__main__':
    unittest.main()
