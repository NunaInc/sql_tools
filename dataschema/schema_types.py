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
"""A bunch of schema types shortcuts."""

import datetime
import decimal

from dataschema import annotations
from dataschema.entity import Annotate
from typing import List, Optional

# All required basic data types
Bool = bool
Int8 = Annotate(int, annotations.TypeInt8())
Int16 = Annotate(int, annotations.TypeInt16())
Int32 = Annotate(int, annotations.TypeInt32())
Int64 = int
UInt8 = Annotate(int, annotations.TypeUInt8())
UInt16 = Annotate(int, annotations.TypeUInt16())
UInt32 = Annotate(int, annotations.TypeUInt32())
UInt64 = Annotate(int, annotations.TypeUInt64())
Float32 = Annotate(float, annotations.TypeFloat32())
Float64 = float
Date = datetime.date
# Use a normal datetime in python and other, but in
# clickhouse represent w/ the leaner DateTime.
DateTime = Annotate(datetime.datetime, annotations.ClickhouseType('DateTime'))
DateTime64 = datetime.datetime
String = str
Bytes = bytes
# Want to represent as an int in scala but have UInt64 in
# clickhouse. (no proper uint64 in scala so is unsafe to
# assume direct conversion)
ChUInt64 = Annotate(int, annotations.ClickhouseType('UInt64'))

# All optional basic data types - All from above w/ Opt.
OptBool = Optional[bool]
OptInt8 = Annotate(Optional[int], annotations.TypeInt8())
OptInt16 = Annotate(Optional[int], annotations.TypeInt16())
OptInt32 = Annotate(Optional[int], annotations.TypeInt32())
OptInt64 = Optional[int]
OptUInt8 = Annotate(Optional[int], annotations.TypeUInt8())
OptUInt16 = Annotate(Optional[int], annotations.TypeUInt16())
OptUInt32 = Annotate(Optional[int], annotations.TypeUInt32())
OptUInt64 = Annotate(Optional[int], annotations.TypeUInt64())
OptFloat32 = Annotate(Optional[float], annotations.TypeFloat32())
OptFloat64 = Optional[float]
OptDate = Optional[datetime.date]
OptDateTime = Annotate(Optional[datetime.datetime],
                       annotations.ClickhouseType('DateTime'))
OptDateTime64 = Optional[datetime.datetime]
OptString = Optional[str]
OptBytes = Optional[bytes]
OptChUInt64 = Annotate(Optional[int], annotations.ClickhouseType('UInt64'))


def Decimal(scale: int, precision: int) -> type:
    """Quickly build a decimal type."""
    return Annotate(decimal.Decimal, annotations.Decimal(scale, precision))


def DecimalList(scale: int, precision: int) -> type:
    """Quickly build a decimal type."""
    return Annotate(List[decimal.Decimal],
                    annotations.Decimal(scale, precision))


def OptDecimal(scale: int, precision: int) -> type:
    """Quickly build an optional decimal type."""
    return Annotate(Optional[decimal.Decimal],
                    annotations.Decimal(scale, precision))


def Lz4(t: type) -> type:
    """Annotate a field for compression w/ LZ4."""
    return Annotate(t, annotations.Compression('LZ4'))


def Lz4Hc(t: type, level: Optional[int] = None) -> type:
    """Annotate a field for compression w/ LZ4HC."""
    return Annotate(t, annotations.Compression('LZ4HC', level))


def Zstd(t: type, level: Optional[int] = None) -> type:
    """Annotate a field for compression w/ ZSTD."""
    return Annotate(t, annotations.Compression('ZSTD', level=level))


def Uncompressed(t: type, level: Optional[int] = None) -> type:
    """Annotate a field as uncompressed in a table w/ default compression."""
    return Annotate(t, annotations.Compression('UNCOMPRESSED', level=level))


def Delta(t: type, delta: Optional[int] = None) -> type:
    """Annotate delta compression for int fields."""
    return Annotate(t, annotations.DeltaCompression(delta))


def LowCardinality(t: type) -> type:
    """Quickly annotate for low cardinality a string."""
    return Annotate(t, annotations.LowCardinality())


def WithChType(t: type, type_name: str) -> type:
    """Annotate a type with the clickhouse type to use."""
    return Annotate(t, annotations.ClickhouseType(type_name))

def NamedTuple(t: type) -> type:
    """In ClickHouse, use named Tuple instead of Nested for a nested type."""
    return Annotate(t, annotations.ClickhouseNestedType('Tuple'))

def OptNamedTuple(t: type) -> type:
    """
    Same as NamedTuple, but allows the field to be optional in output formats besides ClickHouse.
    """
    return Annotate(Optional[t], annotations.ClickhouseNestedType('Tuple'))

def Id(t: type) -> type:
    """Annotate a column as a table identifier."""
    return Annotate(t, annotations.Id())


def Comment(t: type, comment: str) -> type:
    """Annotates a column or table with a comment."""
    return Comment(t, annotations.Comment(comment))
