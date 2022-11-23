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
"""Tests the conversion to pandas."""

import pandas
import unittest
from dataschema import (python2schema, schema2pandas, schema_test_data)


class ParquetTest(unittest.TestCase):

    def test_schema2parquet(self):
        table = python2schema.ConvertDataclass(schema_test_data.TestProto)
        pandas_schema = schema2pandas.ConvertTable(table)
        self.assertEqual(
            pandas_schema, {
                'id': pandas.Int64Dtype(),
                'fint32': pandas.UInt32Dtype(),
                'fsint32': pandas.Int32Dtype(),
                'fint64': pandas.UInt64Dtype(),
                'fsint64': pandas.Int64Dtype(),
                'fdouble': pandas.Float64Dtype(),
                'ffloat': pandas.Float32Dtype(),
                'fstring': pandas.StringDtype(),
                'fbytes': 'O',
                'fdate': 'O',
                'ftimestamp': pandas.DatetimeTZDtype(tz='Etc/UTC'),
                'fdqannotated': pandas.StringDtype(),
                'frep_seq': 'O',
                'frep_array': 'O',
                'frep_set': 'O',
                'fdecimal_bigint': 'O',
                'fdecimal_default': 'O',
                'fdecimal_bigdecimal': 'O',
                'with__original__name': pandas.StringDtype(),
                'finitialized': 'O',
                'fwidth': pandas.StringDtype(),
                'flob': pandas.StringDtype(),
                'fcommented': pandas.StringDtype(),
                'fboolean': pandas.BooleanDtype()
            })


if __name__ == '__main__':
    unittest.main()
