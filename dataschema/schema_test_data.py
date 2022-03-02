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
"""A test module containing dataclasses equivalent to schema_test.proto."""
import datetime
import decimal
from dataclasses import dataclass
from dataschema.entity import Annotate
from dataschema import annotations
from typing import Optional, List, Set

JAVA_PACKAGE = 'com.nuna.schema.test'


@annotations.entity_class
@annotations.comment(value='Joint Comment')
@dataclass
class TestJoinProto:
    id: Annotate(int, annotations.Id())


@annotations.clickhouse_engine(engine='REPLICATED_MERGE_TREE')
@annotations.order_by(values=['id', 'fsint32'])
@annotations.partition_by(values=['toYYYYMM(fdate)'])
@annotations.index_granularity(value=8192)
@dataclass
class TestProto:
    """A test dataclass - we go through all annotations / types."""
    id: Annotate(int,
                 [annotations.Id(),
                  annotations.JoinReference('TestJoinProto')])
    fint32: Annotate(int, [
        annotations.TypeUInt32(),
        annotations.DeltaCompression(2),
        annotations.Compression('ZSTD', 7)
    ])
    fsint32: Annotate(int, [annotations.TypeInt32()])
    fint64: Annotate(int,
                     [annotations.TypeUInt64(),
                      annotations.Compression('LZ4')])
    fsint64: Optional[int]
    fdouble: Optional[float]
    ffloat: Annotate(Optional[float], annotations.TypeFloat32())
    fstring: Annotate(Optional[str], annotations.LowCardinality())
    fbytes: Optional[bytes]
    fdate: Optional[datetime.date]
    ftimestamp: Annotate(Optional[datetime.datetime],
                         annotations.Timestamp(3, 'Etc/UTC'))
    fdqannotated: Annotate(
        Optional[str],
        annotations.DqField(is_nullable=True,
                            is_ignored=True,
                            format_str='\\w*',
                            enum_values=['a', 'b', 'c'],
                            regexp='\\w*'))
    frep_seq: List[str]
    frep_array: Annotate(List[str], annotations.Array())
    frep_set: Set[str]
    fdecimal_bigint: Annotate(Optional[decimal.Decimal],
                              annotations.Decimal(30, 0))
    fdecimal_default: Annotate(Optional[decimal.Decimal],
                               annotations.Decimal(30, 20))
    fdecimal_bigdecimal: Annotate(
        Optional[decimal.Decimal],
        [annotations.Decimal(30, 20),
         annotations.JavaTypeName('BigDecimal')])
    with__original__name: Annotate(Optional[str],
                                   annotations.ScalaOriginalName('field name'))
    finitialized: Annotate(List[str], annotations.ScalaInitializer(' = Seq()'))
    fwidth: Annotate(Optional[str], annotations.Width(10))
    flob: Annotate(Optional[str], annotations.Lob())
    fcommented: Annotate(Optional[str], annotations.Comment('Some comment'))
    fboolean: Annotate(Optional[bool],
                       annotations.ClickhouseOriginalName('fboolean_sql'))
