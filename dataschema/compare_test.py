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
"""Tests the schema comparison."""

import datetime
import decimal
import unittest
from dataclasses import dataclass

from dataschema.entity import Annotate
from dataschema import Schema
from dataschema import annotations
from dataschema import python2schema
from typing import List, Optional, Set


class CompareTest(unittest.TestCase):

    def test_compare_labels(self):

        @dataclass
        class A:
            foo: int

        @dataclass
        class B:
            foo: Optional[int]

        @dataclass
        class C:
            foo: List[int]

        ta = python2schema.ConvertDataclass(A)
        tb = python2schema.ConvertDataclass(B)
        tc = python2schema.ConvertDataclass(C)
        self.assertEqual([d.code for d in ta.compare(ta)], [])
        self.assertEqual(
            [d.code for d in ta.compare(tb)],
            [Schema.SchemaDiffCode.NAMES, Schema.SchemaDiffCode.LABEL])
        self.assertEqual(
            [d.code for d in ta.compare(tc)],
            [Schema.SchemaDiffCode.NAMES, Schema.SchemaDiffCode.LABEL])
        self.assertEqual([d.code for d in tb.compare(ta)], [
            Schema.SchemaDiffCode.NAMES, Schema.SchemaDiffCode.LABEL_CONVERTIBLE
        ])
        self.assertEqual(
            [d.code for d in tb.compare(tc)],
            [Schema.SchemaDiffCode.NAMES, Schema.SchemaDiffCode.LABEL])
        self.assertEqual([d.code for d in tc.compare(ta)], [
            Schema.SchemaDiffCode.NAMES, Schema.SchemaDiffCode.LABEL_CONVERTIBLE
        ])
        self.assertEqual([d.code for d in tc.compare(tb)], [
            Schema.SchemaDiffCode.NAMES, Schema.SchemaDiffCode.LABEL_CONVERTIBLE
        ])

    def test_compare_decimal(self):

        @dataclass
        class A:
            foo: Annotate(Optional[decimal.Decimal], annotations.Decimal(12, 2))

        @dataclass
        class B:
            foo: Annotate(Optional[decimal.Decimal], annotations.Decimal(12, 4))

        @dataclass
        class C:
            foo: Annotate(Optional[decimal.Decimal], annotations.Decimal(14, 4))

        ta = python2schema.ConvertDataclass(A)
        tb = python2schema.ConvertDataclass(B)
        tc = python2schema.ConvertDataclass(C)
        self.assertEqual([d.code for d in ta.compare(ta)], [])
        self.assertEqual(
            [d.code for d in ta.compare(tb)],
            [Schema.SchemaDiffCode.NAMES, Schema.SchemaDiffCode.DECIMAL])
        self.assertEqual([d.code for d in ta.compare(tc)], [
            Schema.SchemaDiffCode.NAMES, Schema.SchemaDiffCode.DECIMAL,
            Schema.SchemaDiffCode.DECIMAL
        ])
        self.assertEqual([d.code for d in tb.compare(ta)], [
            Schema.SchemaDiffCode.NAMES,
            Schema.SchemaDiffCode.DECIMAL_CONVERTIBLE
        ])
        self.assertEqual(
            [d.code for d in tb.compare(tc)],
            [Schema.SchemaDiffCode.NAMES, Schema.SchemaDiffCode.DECIMAL])
        self.assertEqual([d.code for d in tc.compare(ta)], [
            Schema.SchemaDiffCode.NAMES,
            Schema.SchemaDiffCode.DECIMAL_CONVERTIBLE,
            Schema.SchemaDiffCode.DECIMAL_CONVERTIBLE
        ])
        self.assertEqual([d.code for d in tc.compare(tb)], [
            Schema.SchemaDiffCode.NAMES,
            Schema.SchemaDiffCode.DECIMAL_CONVERTIBLE
        ])

    def test_compare_datetime(self):

        @dataclass
        class A:
            foo: Annotate(datetime.datetime,
                          annotations.Timestamp(3, 'Etc/UTC'))

        @dataclass
        class B:
            foo: Annotate(datetime.datetime,
                          annotations.Timestamp(3, 'Americas/LosAngeles'))

        @dataclass
        class C:
            foo: Annotate(datetime.datetime,
                          annotations.Timestamp(6, 'Etc/UTC'))

        ta = python2schema.ConvertDataclass(A)
        tb = python2schema.ConvertDataclass(B)
        tc = python2schema.ConvertDataclass(C)
        self.assertEqual([d.code for d in ta.compare(ta)], [])
        self.assertEqual(
            [d.code for d in ta.compare(tb)],
            [Schema.SchemaDiffCode.NAMES, Schema.SchemaDiffCode.DATETIME])
        self.assertEqual(
            [d.code for d in ta.compare(tc)],
            [Schema.SchemaDiffCode.NAMES, Schema.SchemaDiffCode.DATETIME])

    def test_not_found(self):

        @dataclass
        class A:
            foo: int
            bar: str

        @dataclass
        class B:
            foo: int
            baz: str

        ta = python2schema.ConvertDataclass(A)
        tb = python2schema.ConvertDataclass(B)
        self.assertEqual([d.code for d in ta.compare(tb)], [
            Schema.SchemaDiffCode.NAMES, Schema.SchemaDiffCode.NOT_FOUND_DEST,
            Schema.SchemaDiffCode.NOT_FOUND_SRC
        ])
        self.assertEqual([d.code for d in ta.columns[1].compare(tb.columns[1])],
                         [Schema.SchemaDiffCode.NAMES])

    def test_struct_mismatch(self):

        @dataclass
        class A:
            foo: List[int]

        @dataclass
        class B:
            foo: Set[int]

        ta = python2schema.ConvertDataclass(A)
        tb = python2schema.ConvertDataclass(B)
        self.assertEqual([d.code for d in ta.compare(tb)], [
            Schema.SchemaDiffCode.NAMES,
            Schema.SchemaDiffCode.REPEATED_STRUCT_MISMATCH
        ])

    def test_array_mismatch(self):

        @dataclass
        class A:
            foo: List[Optional[int]]

        @dataclass
        class B:
            foo: Set[Optional[int]]

        ta = python2schema.ConvertDataclass(A)
        tb = python2schema.ConvertDataclass(B)
        self.assertEqual([d.code for d in ta.compare(tb)], [
            Schema.SchemaDiffCode.NAMES,
            Schema.SchemaDiffCode.ARRAY_SET_MISMATCH
        ])

    def test_compare_types(self):

        @dataclass
        class A:
            """Test class with fields to which B fields can be converted."""
            int8_bool: Annotate(int, annotations.TypeInt8())
            int16_bool: Annotate(int, annotations.TypeInt16())
            int16_int8: Annotate(int, annotations.TypeInt16())
            int16_uint8: Annotate(int, annotations.TypeInt16())
            int32_bool: Annotate(int, annotations.TypeInt32())
            int32_int8: Annotate(int, annotations.TypeInt32())
            int32_uint8: Annotate(int, annotations.TypeInt32())
            int32_int16: Annotate(int, annotations.TypeInt32())
            int32_uint16: Annotate(int, annotations.TypeInt32())
            int32_date: Annotate(int, annotations.TypeInt32())
            int64_bool: int
            int64_int8: int
            int64_uint8: int
            int64_int16: int
            int64_uint16: int
            int64_int32: int
            int64_uint32: int
            int64_date: int
            int64_datetime: int
            uint8_bool: Annotate(int, annotations.TypeUInt8())
            uint16_bool: Annotate(int, annotations.TypeUInt16())
            uint16_uint8: Annotate(int, annotations.TypeUInt16())
            uint32_bool: Annotate(int, annotations.TypeUInt32())
            uint32_uint8: Annotate(int, annotations.TypeUInt32())
            uint32_uint16: Annotate(int, annotations.TypeUInt32())
            uint64_bool: Annotate(int, annotations.TypeUInt64())
            uint64_uint8: Annotate(int, annotations.TypeUInt64())
            uint64_uint16: Annotate(int, annotations.TypeUInt64())
            uint64_uint32: Annotate(int, annotations.TypeUInt64())
            float64_float32: float

        @dataclass
        class B:
            """Test class with fields that convert to A fields above."""
            int8_bool: bool
            int16_bool: bool
            int16_int8: Annotate(int, annotations.TypeInt8())
            int16_uint8: Annotate(int, annotations.TypeUInt8())
            int32_bool: bool
            int32_int8: Annotate(int, annotations.TypeInt8())
            int32_uint8: Annotate(int, annotations.TypeUInt8())
            int32_int16: Annotate(int, annotations.TypeInt16())
            int32_uint16: Annotate(int, annotations.TypeUInt16())
            int32_date: datetime.date
            int64_bool: bool
            int64_int8: Annotate(int, annotations.TypeInt8())
            int64_uint8: Annotate(int, annotations.TypeUInt8())
            int64_int16: Annotate(int, annotations.TypeInt16())
            int64_uint16: Annotate(int, annotations.TypeUInt16())
            int64_int32: Annotate(int, annotations.TypeInt32())
            int64_uint32: Annotate(int, annotations.TypeUInt32())
            int64_date: datetime.date
            int64_datetime: datetime.datetime
            uint8_bool: bool
            uint16_bool: bool
            uint16_uint8: Annotate(int, annotations.TypeUInt8())
            uint32_bool: bool
            uint32_uint8: Annotate(int, annotations.TypeUInt8())
            uint32_uint16: Annotate(int, annotations.TypeUInt16())
            uint64_bool: bool
            uint64_uint8: Annotate(int, annotations.TypeUInt8())
            uint64_uint16: Annotate(int, annotations.TypeUInt16())
            uint64_uint32: Annotate(int, annotations.TypeUInt32())
            float64_float32: Annotate(float, annotations.TypeFloat32())

        ta = python2schema.ConvertDataclass(A)
        tb = python2schema.ConvertDataclass(B)
        self.assertEqual([d.code for d in ta.compare(tb)], [
            Schema.SchemaDiffCode.NAMES,
        ] + [Schema.SchemaDiffCode.TYPES_DIFFERENT_CONVERTIBLE] * 30)


if __name__ == '__main__':
    unittest.main()
