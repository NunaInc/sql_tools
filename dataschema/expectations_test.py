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
"""Testing the expectation generation for schemas."""

import datetime
import os
import sqlalchemy
import shutil
import tempfile
import unittest

from dataclasses import dataclass
from great_expectations import DataContext
from great_expectations.core.batch import RuntimeBatchRequest
from sqlalchemy.schema import CreateTable

from dataschema import (
    data_writer,
    python2schema,
    schema2expectations,
    schema2sqlalchemy,
    schema_synth,
    schema_test_data,
    annotations
)
from dataschema.schema_types import (Int8, Int16, Int32, Int64, UInt8, UInt16,
                                     UInt32, Float32, Float64, Decimal)
from dataschema.entity import Annotate

@dataclass
class TestProtoWithDqAnnotations(schema_test_data.TestProto):
    fdqannotated_enum_value: Annotate(str, annotations.DqField(enum_values=['a', 'b']))
    fdqannotated_regexp: Annotate(str, annotations.DqField(regexp='^[A-Z]{2}'))

@dataclass
class DataTest:
    field_int8: Int8
    field_int16: Int16
    field_int32: Int32
    field_int64: Int64
    field_uint8: UInt8
    field_uint16: UInt16
    field_uint32: UInt32
    ffloat32: Float32
    ffloat64: Float64
    fstr: str
    fbytes: bytes
    fdate: datetime.date
    fdtime: datetime.datetime
    fdecimal: Decimal(10, 2)


def _filtered_expectation(exp):
    assert exp['meta'] == {}
    del exp['meta']
    return exp


_PANDAS_EXPECTATIONS = [
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'id',
            'column_index': 0,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_not_be_null',
        'kwargs': {
            'column': 'id',
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_in_type_list',
        'kwargs': {
            'column': 'id',
            'type_list': ['int64', 'int'],
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_between',
        'kwargs': {
            'column': 'id',
            'min_value': -9223372036854775808,
            'max_value': 9223372036854775807,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'fint32',
            'column_index': 1,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_not_be_null',
        'kwargs': {
            'column': 'fint32',
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_in_type_list',
        'kwargs': {
            'column': 'fint32',
            'type_list': ['uint32', 'int'],
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_between',
        'kwargs': {
            'column': 'fint32',
            'min_value': 0,
            'max_value': 4294967295,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'fsint32',
            'column_index': 2,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_not_be_null',
        'kwargs': {
            'column': 'fsint32',
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_in_type_list',
        'kwargs': {
            'column': 'fsint32',
            'type_list': ['int32', 'int'],
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_between',
        'kwargs': {
            'column': 'fsint32',
            'min_value': -2147483648,
            'max_value': 2147483647,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'fint64',
            'column_index': 3,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_not_be_null',
        'kwargs': {
            'column': 'fint64',
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_in_type_list',
        'kwargs': {
            'column': 'fint64',
            'type_list': ['uint64', 'int'],
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_between',
        'kwargs': {
            'column': 'fint64',
            'min_value': 0,
            'max_value': 18446744073709551615,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'fsint64',
            'column_index': 4,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_in_type_list',
        'kwargs': {
            'column': 'fsint64',
            'type_list': ['int64', 'O'],
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_between',
        'kwargs': {
            'column': 'fsint64',
            'min_value': -9223372036854775808,
            'max_value': 9223372036854775807,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'fdouble',
            'column_index': 5,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_in_type_list',
        'kwargs': {
            'column': 'fdouble',
            'type_list': ['float64', 'O'],
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'ffloat',
            'column_index': 6,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_in_type_list',
        'kwargs': {
            'column': 'ffloat',
            'type_list': ['float32', 'O'],
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'fstring',
            'column_index': 7,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_in_type_list',
        'kwargs': {
            'column': 'fstring',
            'type_list': ['str', 'O'],
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'fbytes',
            'column_index': 8,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_in_type_list',
        'kwargs': {
            'column': 'fbytes',
            'type_list': ['bytes', 'O'],
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'fdate',
            'column_index': 9,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_in_type_list',
        'kwargs': {
            'column': 'fdate',
            'type_list': ['datetime64[d]', 'O'],
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'ftimestamp',
            'column_index': 10,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_in_type_list',
        'kwargs': {
            'column': 'ftimestamp',
            'type_list': ['datetime64', 'O'],
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'fdqannotated',
            'column_index': 11,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'frep_seq',
            'column_index': 12,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_not_be_null',
        'kwargs': {
            'column': 'frep_seq',
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_of_type',
        'kwargs': {
            'column': 'frep_seq',
            'type_': 'str',
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'frep_array',
            'column_index': 13,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_not_be_null',
        'kwargs': {
            'column': 'frep_array',
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_of_type',
        'kwargs': {
            'column': 'frep_array',
            'type_': 'str',
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'frep_set',
            'column_index': 14,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_not_be_null',
        'kwargs': {
            'column': 'frep_set',
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_of_type',
        'kwargs': {
            'column': 'frep_set',
            'type_': 'list',
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'fdecimal_bigint',
            'column_index': 15,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_in_type_list',
        'kwargs': {
            'column': 'fdecimal_bigint',
            'type_list': ['O', 'float64'],
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'fdecimal_default',
            'column_index': 16,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_in_type_list',
        'kwargs': {
            'column': 'fdecimal_default',
            'type_list': ['O', 'float64'],
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'fdecimal_bigdecimal',
            'column_index': 17,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_in_type_list',
        'kwargs': {
            'column': 'fdecimal_bigdecimal',
            'type_list': ['O', 'float64'],
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'with__original__name',
            'column_index': 18,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_in_type_list',
        'kwargs': {
            'column': 'with__original__name',
            'type_list': ['str', 'O'],
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'finitialized',
            'column_index': 19,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_not_be_null',
        'kwargs': {
            'column': 'finitialized',
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_of_type',
        'kwargs': {
            'column': 'finitialized',
            'type_': 'str',
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'fwidth',
            'column_index': 20,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_in_type_list',
        'kwargs': {
            'column': 'fwidth',
            'type_list': ['str', 'O'],
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_value_lengths_to_be_between',
        'kwargs': {
            'column': 'fwidth',
            'min_value': 0,
            'max_value': 10,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'flob',
            'column_index': 21,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_in_type_list',
        'kwargs': {
            'column': 'flob',
            'type_list': ['str', 'O'],
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'fcommented',
            'column_index': 22,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_in_type_list',
        'kwargs': {
            'column': 'fcommented',
            'type_list': ['str', 'O'],
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'fboolean',
            'column_index': 23,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_in_type_list',
        'kwargs': {
            'column': 'fboolean',
            'type_list': ['bool', 'O'],
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'fdqannotated_enum_value',
            'column_index': 24,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_not_be_null',
        'kwargs': {
            'column': 'fdqannotated_enum_value',
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_of_type',
        'kwargs': {
            'column': 'fdqannotated_enum_value',
            'type_': 'str',
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_in_set',
        'kwargs': {
            'column': 'fdqannotated_enum_value',
            'value_set': ['a', 'b'],
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'fdqannotated_regexp',
            'column_index': 25,
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_not_be_null',
        'kwargs': {
            'column': 'fdqannotated_regexp',
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_of_type',
        'kwargs': {
            'column': 'fdqannotated_regexp',
            'type_': 'str',
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_match_regex',
        'kwargs': {
            'column': 'fdqannotated_regexp',
            'regex': '^[A-Z]{2}',
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_unique',
        'kwargs': {
            'column': 'id',
            'result_format': 'SUMMARY'
        }
    },

]

_SPARK_EXPECTATIONS = [
    {
        'kwargs': {
            'column': 'id',
            'column_index': 0,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_to_exist'
    },
    {
        'kwargs': {
            'column': 'id',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_not_be_null',
    },
    {
        'kwargs': {
            'column': 'id',
            'type_': 'LongType',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_of_type'
    },
    {
        'kwargs': {
            'column': 'id',
            'min_value': -9223372036854775808,
            'max_value': 9223372036854775807,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_between'
    },
    {
        'kwargs': {
            'column': 'fint32',
            'column_index': 1,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_to_exist'
    },
    {
        'kwargs': {
            'column': 'fint32',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_not_be_null',
    },
    {
        'kwargs': {
            'column': 'fint32',
            'type_': 'LongType',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_of_type'
    },
    {
        'kwargs': {
            'column': 'fint32',
            'min_value': 0,
            'max_value': 4294967295,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_between'
    },
    {
        'kwargs': {
            'column': 'fsint32',
            'column_index': 2,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_to_exist'
    },
    {
        'kwargs': {
            'column': 'fsint32',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_not_be_null',
    },
    {
        'kwargs': {
            'column': 'fsint32',
            'type_': 'IntegerType',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_of_type'
    },
    {
        'kwargs': {
            'column': 'fsint32',
            'min_value': -2147483648,
            'max_value': 2147483647,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_between'
    },
    {
        'kwargs': {
            'column': 'fint64',
            'column_index': 3,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_to_exist'
    },
    {
        'kwargs': {
            'column': 'fint64',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_not_be_null',
    },
    {
        'kwargs': {
            'column': 'fint64',
            'type_': 'DecimalType',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_of_type'
    },
    {
        'kwargs': {
            'column': 'fint64',
            'min_value': 0,
            'max_value': 18446744073709551615,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_between'
    },
    {
        'kwargs': {
            'column': 'fsint64',
            'column_index': 4,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_to_exist'
    },
    {
        'kwargs': {
            'column': 'fsint64',
            'type_': 'LongType',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_of_type'
    },
    {
        'kwargs': {
            'column': 'fsint64',
            'min_value': -9223372036854775808,
            'max_value': 9223372036854775807,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_between'
    },
    {
        'kwargs': {
            'column': 'fdouble',
            'column_index': 5,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_to_exist'
    },
    {
        'kwargs': {
            'column': 'fdouble',
            'type_': 'DoubleType',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_of_type'
    },
    {
        'kwargs': {
            'column': 'ffloat',
            'column_index': 6,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_to_exist'
    },
    {
        'kwargs': {
            'column': 'ffloat',
            'type_': 'FloatType',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_of_type'
    },
    {
        'kwargs': {
            'column': 'fstring',
            'column_index': 7,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_to_exist'
    },
    {
        'kwargs': {
            'column': 'fstring',
            'type_': 'StringType',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_of_type'
    },
    {
        'kwargs': {
            'column': 'fbytes',
            'column_index': 8,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_to_exist'
    },
    {
        'kwargs': {
            'column': 'fbytes',
            'type_': 'BinaryType',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_of_type'
    },
    {
        'kwargs': {
            'column': 'fdate',
            'column_index': 9,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_to_exist'
    },
    {
        'kwargs': {
            'column': 'fdate',
            'type_': 'DateType',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_of_type'
    },
    {
        'kwargs': {
            'column': 'ftimestamp',
            'column_index': 10,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_to_exist'
    },
    {
        'kwargs': {
            'column': 'ftimestamp',
            'type_': 'TimestampType',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_of_type'
    },
    {
        'kwargs': {
            'column': 'fdqannotated',
            'column_index': 11,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_to_exist'
    },
    {
        'kwargs': {
            'column': 'frep_seq',
            'column_index': 12,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_to_exist'
    },
    {
        'kwargs': {
            'column': 'frep_seq',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_not_be_null',
    },
    {
        'kwargs': {
            'column': 'frep_seq',
            'type_': 'ArrayType',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_of_type'
    },
    {
        'kwargs': {
            'column': 'frep_array',
            'column_index': 13,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_to_exist'
    },
    {
        'kwargs': {
            'column': 'frep_array',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_not_be_null',
    },
    {
        'kwargs': {
            'column': 'frep_array',
            'type_': 'ArrayType',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_of_type'
    },
    {
        'kwargs': {
            'column': 'frep_set',
            'column_index': 14,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_to_exist'
    },
    {
        'kwargs': {
            'column': 'frep_set',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_not_be_null',
    },
    {
        'kwargs': {
            'column': 'frep_set',
            'type_': 'ArrayType',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_of_type'
    },
    {
        'kwargs': {
            'column': 'fdecimal_bigint',
            'column_index': 15,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_to_exist'
    },
    {
        'kwargs': {
            'column': 'fdecimal_bigint',
            'type_': 'DecimalType',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_of_type'
    },
    {
        'kwargs': {
            'column': 'fdecimal_default',
            'column_index': 16,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_to_exist'
    },
    {
        'kwargs': {
            'column': 'fdecimal_default',
            'type_': 'DecimalType',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_of_type'
    },
    {
        'kwargs': {
            'column': 'fdecimal_bigdecimal',
            'column_index': 17,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_to_exist'
    },
    {
        'kwargs': {
            'column': 'fdecimal_bigdecimal',
            'type_': 'DecimalType',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_of_type'
    },
    {
        'kwargs': {
            'column': 'with__original__name',
            'column_index': 18,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_to_exist'
    },
    {
        'kwargs': {
            'column': 'with__original__name',
            'type_': 'StringType',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_of_type'
    },
    {
        'kwargs': {
            'column': 'finitialized',
            'column_index': 19,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_to_exist'
    },
    {
        'kwargs': {
            'column': 'finitialized',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_not_be_null',
    },
    {
        'kwargs': {
            'column': 'finitialized',
            'type_': 'ArrayType',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_of_type'
    },
    {
        'kwargs': {
            'column': 'fwidth',
            'column_index': 20,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_to_exist'
    },
    {
        'kwargs': {
            'column': 'fwidth',
            'type_': 'StringType',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_of_type'
    },
    {
        'kwargs': {
            'column': 'fwidth',
            'min_value': 0,
            'max_value': 10,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_value_lengths_to_be_between'
    },
    {
        'kwargs': {
            'column': 'flob',
            'column_index': 21,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_to_exist'
    },
    {
        'kwargs': {
            'column': 'flob',
            'type_': 'StringType',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_of_type'
    },
    {
        'kwargs': {
            'column': 'fcommented',
            'column_index': 22,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_to_exist'
    },
    {
        'kwargs': {
            'column': 'fcommented',
            'type_': 'StringType',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_of_type'
    },
    {
        'kwargs': {
            'column': 'fboolean',
            'column_index': 23,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_to_exist'
    },
    {
        'kwargs': {
            'column': 'fboolean',
            'type_': 'BooleanType',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_of_type'
    },
    {
        'kwargs': {
            'column': 'fdqannotated_enum_value',
            'column_index': 24,
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_to_exist',
    },
    {
        'kwargs': {
            'column': 'fdqannotated_enum_value',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_not_be_null',
    },
    {
        'kwargs': {
            'column': 'fdqannotated_enum_value',
            'type_': 'StringType',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_of_type',
    },
    {
        'kwargs': {
            'column': 'fdqannotated_enum_value',
            'value_set': ['a', 'b'],
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_be_in_set',
    },
    {
        'expectation_type': 'expect_column_to_exist',
        'kwargs': {
            'column': 'fdqannotated_regexp',
            'column_index': 25,
            'result_format': 'SUMMARY'
        }
    },
    {
        'kwargs': {
            'column': 'fdqannotated_regexp',
            'result_format': 'SUMMARY'
        },
        'expectation_type': 'expect_column_values_to_not_be_null',
    },
    {
        'expectation_type': 'expect_column_values_to_be_of_type',
        'kwargs': {
            'column': 'fdqannotated_regexp',
            'type_': 'StringType',
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_match_regex',
        'kwargs': {
            'column': 'fdqannotated_regexp',
            'regex': '^[A-Z]{2}',
            'result_format': 'SUMMARY'
        }
    },
    {
        'expectation_type': 'expect_column_values_to_be_unique',
        'kwargs': {
            'column': 'id',
            'result_format': 'SUMMARY'
        },
    },
]
# Test data size (ie number of records synthesized)
_SIZE = 10


class ExpectationsTest(unittest.TestCase):

    def prepare_data_context(self):
        project_dir = os.path.join(self.test_dir, 'test_context')
        os.makedirs(project_dir)
        self.data_context = DataContext.create(project_dir)
        table = python2schema.ConvertDataclass(DataTest)

        data_configs = [{
            'name': 'spark_datasource',
            'class_name': 'Datasource',
            'execution_engine': {
                'module_name': 'great_expectations.execution_engine',
                'class_name': 'SparkDFExecutionEngine'
            },
            'data_connectors': {
                'spark_connector': {
                    'class_name': 'RuntimeDataConnector',
                    'batch_identifiers': ['parquet_batch'],
                },
            },
        }, {
            'name': 'pandas_datasource',
            'class_name': 'Datasource',
            'execution_engine': {
                'module_name': 'great_expectations.execution_engine',
                'class_name': 'PandasExecutionEngine',
            },
            'data_connectors': {
                'pandas_connector': {
                    'class_name': 'RuntimeDataConnector',
                    'batch_identifiers': ['csv_batch'],
                },
            },
        }, {
            'name': 'sql_datasource',
            'class_name': 'Datasource',
            'execution_engine': {
                'module_name': 'great_expectations.execution_engine',
                'class_name': 'SqlAlchemyExecutionEngine',
                'connection_string': f'sqlite:///{self.db_path}'
            },
            'data_connectors': {
                'sql_connector': {
                    'class_name': 'RuntimeDataConnector',
                    'batch_identifiers': ['sql_batch'],
                },
            },
        }]
        for config in data_configs:
            self.data_context.add_datasource(**config)
        self.suites = {}
        for engine in [
                schema2expectations.EngineType.PANDAS,
                schema2expectations.EngineType.SPARK,
                schema2expectations.EngineType.SQL_ALCHEMY
        ]:
            name = f'test_{engine}'
            suite = self.data_context.create_expectation_suite(name)
            expectations = schema2expectations.ConvertTable(
                table, engine, 'SUMMARY')
            for exp in expectations:
                suite.append_expectation(exp)
            self.suites[engine] = suite
        # Another quirk with great expectations:
        #   - after carefully setting up the expectation suites above, obtaining
        #   the expectation suite now, would render
        #     1/ a different object
        #     2/ with an **empty** expectation list contained
        # ie.
        #    self.data_context.get_expectation_suite(name)
        #    would return a totally different object, with no expectations !
        # so we have to save the expectations as to self.suites

    def get_pandas_validator(self):
        batch_request = RuntimeBatchRequest(
            datasource_name='pandas_datasource',
            data_connector_name='pandas_connector',
            data_asset_name='test_csv_data',
            runtime_parameters={'path': self.csv_file},
            batch_identifiers={'csv_batch': 'test_csv_run'},
        )
        return self.data_context.get_validator(
            batch_request=batch_request,
            expectation_suite_name='test_EngineType.PANDAS')

    def get_spark_validator(self):
        batch_request = RuntimeBatchRequest(
            datasource_name='spark_datasource',
            data_connector_name='spark_connector',
            data_asset_name='test_parquet_data',
            runtime_parameters={'path': self.parquet_file},
            batch_identifiers={'parquet_batch': 'test_parquet_run'},
        )
        return self.data_context.get_validator(
            batch_request=batch_request,
            expectation_suite_name='test_EngineType.SPARK')

    def get_sql_validator(self):
        batch_request = RuntimeBatchRequest(
            datasource_name='sql_datasource',
            data_connector_name='sql_connector',
            data_asset_name='test_sql_data',
            runtime_parameters={'query': 'SELECT * from DataTest'},
            batch_identifiers={'sql_batch': 'test_sql_run'},
        )
        return self.data_context.get_validator(
            batch_request=batch_request,
            expectation_suite_name='test_EngineType.SQL_ALCHEMY')

    def prepare_sqldb(self):
        self.db_path = os.path.join(self.test_dir, 'sql_test.db')
        print(f'SQL path: {self.db_path}')
        self.engine = sqlalchemy.create_engine(f'sqlite:///{self.db_path}')
        print(f'SQL engine: {self.engine}')
        self.engine.connect()
        self.sql_meta = sqlalchemy.MetaData()
        table = schema2sqlalchemy.ConvertTable(
            python2schema.ConvertDataclass(DataTest), meta=self.sql_meta)
        for column in table.columns:
            coltype = self.engine.dialect.type_compiler.process(
                column.type, type_expression=column)
            print(f'{column} / {column.type} ==> {coltype}')
        print(f'Sql Alchemy table:\n{CreateTable(table)}')
        self.sql_meta.create_all(self.engine)

    def generate_test_data(self):
        builder = schema_synth.Builder()
        gens = builder.schema_generator(
            output_type=schema_synth.OutputType.DATAFRAME,
            data_classes=[DataTest])
        file_info = [
            schema_synth.FileGeneratorInfo(gen, _SIZE, gen.name())
            for gen in gens
        ]
        self.csv_file = schema_synth.GenerateFiles(
            file_info, data_writer.CsvWriter(),
            os.path.join(self.test_dir, 'csv_data'))['DataTest'][0]
        self.parquet_file = schema_synth.GenerateFiles(
            file_info, data_writer.ParquetWriter(True),
            os.path.join(self.test_dir, 'parquet_data'))['DataTest'][0]
        schema_synth.GenerateFiles(file_info,
                                   data_writer.SqlAlchemyWriter(self.engine),
                                   '')

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        # One more great_expectations quirk/issue:
        #  - they check consider a path as an S3 path iff it contains
        #    's3' string - WHICH IT MAY DO for a mkdtemp path !!
        #  => use a different path
        if 's3' in self.test_dir:
            self.test_dir = '/tmp/expectation_test.dir'
            os.makedirs(self.test_dir, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_conversion(self):
        table = python2schema.ConvertDataclass(TestProtoWithDqAnnotations)
        pd_exp = schema2expectations.ConvertTable(
            table, schema2expectations.EngineType.PANDAS, 'SUMMARY')
        expectations = [
            _filtered_expectation(exp.to_json_dict()) for exp in pd_exp
        ]
        self.assertEqual(len(expectations), len(_PANDAS_EXPECTATIONS))
        for (exp, pd_exp) in zip(expectations, _PANDAS_EXPECTATIONS):
            self.assertEqual(exp, pd_exp)

        spark_exp = schema2expectations.ConvertTable(
            table, schema2expectations.EngineType.SPARK, 'SUMMARY')
        expectations = [
            _filtered_expectation(exp.to_json_dict()) for exp in spark_exp
        ]
        self.assertEqual(len(expectations), len(_SPARK_EXPECTATIONS))
        for (exp, sp_exp) in zip(expectations, _SPARK_EXPECTATIONS):
            self.assertEqual(exp, sp_exp)

        # Note: TestProto cannot be properly converted to a sqlite table
        #    because of missing ARRAY support in the dialect

    def test_expectations(self):
        self.prepare_sqldb()
        self.generate_test_data()
        self.prepare_data_context()

        pandas_validator = self.get_pandas_validator()
        pandas_result = pandas_validator.validate(expectation_suite=self.suites[
            schema2expectations.EngineType.PANDAS])
        print(f'Pandas validation:\n{pandas_result}')
        self.assertTrue(pandas_result['success'])
        print('===> SPARK validation:\n')
        spark_validator = self.get_spark_validator()
        spark_result = spark_validator.validate(
            expectation_suite=self.suites[schema2expectations.EngineType.SPARK])
        print(f'Spark validation:\n{spark_result}')
        self.assertTrue(spark_result['success'])
        print('===> SQL validation:\n')
        sql_validator = self.get_sql_validator()
        sql_result = sql_validator.validate(expectation_suite=self.suites[
            schema2expectations.EngineType.SQL_ALCHEMY])
        print(f'Sql validation:\n{sql_result}')
        self.assertTrue(sql_result['success'])


if __name__ == '__main__':
    unittest.main()
