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
"""Tests conversion from schema to python code."""
# pylint: disable=unused-import
# Some imports are needed for generated code
import dataclasses
import datetime
import decimal
import sys
import typing
import unittest
from dataschema.entity import Annotate
from dataschema import annotations, python2schema, schema_test_data, schema2python


@dataclasses.dataclass
class A:
    """Bogus class for nesting test."""

    @dataclasses.dataclass
    class B:
        foo: int

        @dataclasses.dataclass
        class C:
            bar: str

    list_test: typing.List[typing.Optional[B]]
    set_test: typing.Set[B]
    map_test: typing.Dict[int, B]


class Schema2PythonTest(unittest.TestCase):

    def test_convert(self):
        table = python2schema.ConvertDataclass(schema_test_data.TestProto)
        py_module_info = schema2python.GetModuleInfo('com.nuna.test.TestProto',
                                                     [table])
        py_code = '\n'.join(py_module_info.py_module_lines())
        print(py_code)
        local_vars = {}
        exec(py_code, globals(), local_vars)  # pylint: disable=exec-used
        # This TestProto is from the generated code:
        table2 = python2schema.ConvertDataclass(local_vars['TestProto'])
        # Full names are different per package diff, beside that, we should have
        # no differences:
        table2.info.full_name = table.info.full_name
        diffs = table.compare(table2)
        self.assertEqual(len(diffs), 0)

    def test_nested(self):
        table = python2schema.ConvertDataclass(A)
        py_module_info = schema2python.GetModuleInfo('com.nuna.test.Nested',
                                                     [table])
        py_lines = py_module_info.py_module_lines()
        py_code = '\n'.join(py_lines[1:])
        # Test just for equality - cannot execute this code per reference to
        # __main__:
        self.assertEqual(
            py_code, """import dataclasses
import typing

JAVA_PACKAGE = "com.nuna.test.Nested"

@dataclasses.dataclass
class A:
    list_test: typing.List[typing.Optional[__main__.A.B]]
    set_test: typing.Set[__main__.A.B]
    map_test: typing.Dict[int, __main__.A.B]""")

    def test_force_nested(self):
        table = python2schema.ConvertDataclass(A)
        py_module_info = schema2python.GetModuleInfo('com.nuna.test.Nested',
                                                     [table], True)
        py_code = '\n'.join(py_module_info.py_module_lines())
        print(py_code)
        local_vars = {}
        exec(py_code, globals(), local_vars)  # pylint: disable=exec-used
        table2 = python2schema.ConvertDataclass(local_vars['A'])
        table2.info.full_name = table.info.full_name
        diffs = table.compare(table2)
        self.assertEqual(len(diffs), 0)


if __name__ == '__main__':
    unittest.main()
