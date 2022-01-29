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
"""Tests the scala case class generation from proto."""

import unittest
from dataschema import proto2schema
from dataschema import schema_example
from dataschema import schema_example_pb2
from dataschema import schema_test_data
from dataschema import schema_test_pb2
from dataschema import schema_test_bad_pb2
from dataschema import schema2scala
from dataschema import schema2sql

_DEFAULT_ANNOTATIONS = schema2scala.ScalaAnnotationClasses(
    comment='com.nuna.annotations.Comment',
    dq_field='com.nuna.annotations.dq.DqField',
    join_reference='com.nuna.annotations.JoinReference',
    width='com.nuna.annotations.Width',
)

EXPECTED_SCALA = """package com.nuna.schema.test
import com.nuna.annotations.dq.DqField
import com.nuna.annotations.{Comment, JoinReference, Width}
import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import javax.persistence.{Entity, Id, Lob}
import org.apache.spark.sql.types.Decimal
import scala.math.BigInt

@Entity
@Comment("Joint Comment")
case class TestJoinProto(
  @Id
  id: Long
)


case class TestProto(
  @Id
  @JoinReference(classOf[TestJoinProto])
  id: Long,
  fint32: Long,
  fsint32: Int,
  fint64: BigInt,
  fsint64: Option[Long],
  fdouble: Option[Double],
  ffloat: Option[Float],
  fstring: Option[String],
  fbytes: Option[ByteBuffer],
  fdate: Option[Date],
  ftimestamp: Option[Timestamp],
  @DqField(nullable = true, ignore = true, format = "\\\\w*", enumValues = Array("a", "b", "c"), regexp = "\\\\w*")
  fdqannotated: Option[String],
  frep_seq: Seq[String],
  frep_array: Array[String],
  frep_set: Set[String],
  fdecimal_bigint: Option[BigInt],
  fdecimal_default: Option[Decimal],
  fdecimal_bigdecimal: Option[BigDecimal],
  `field name`: Option[String],
  finitialized: Seq[String] = Seq(),
  @Width(10)
  fwidth: Option[String],
  @Lob
  flob: Option[String],
  @Comment("Some comment")
  fcommented: Option[String],
  fboolean: Option[Boolean]
)"""

EXPECTED_SQL_TESTJOINPROTO = """CREATE TABLE ${database}.${table} (
  id Int64
)

COMMENT 'Joint Comment'"""

EXPECTED_SQL_TESTPROTO = """CREATE TABLE ${database}.${table} (
  id Int64,
  fint32 UInt32 CODEC(Delta(2), ZSTD(7)),
  fsint32 Int32,
  fint64 UInt64 CODEC(LZ4),
  fsint64 Nullable(Int64),
  fdouble Nullable(Float64),
  ffloat Nullable(Float32),
  fstring LowCardinality(Nullable(String)),
  fbytes Nullable(String),
  fdate Nullable(Date),
  ftimestamp Nullable(DateTime64(3, "Etc/UTC")),
  fdqannotated Nullable(String),
  frep_seq Array(String),
  frep_array Array(String),
  frep_set Array(String),
  fdecimal_bigint Nullable(Decimal128(0)),
  fdecimal_default Nullable(Decimal128(20)),
  fdecimal_bigdecimal Nullable(Decimal128(20)),
  with__original__name Nullable(String),
  finitialized Array(String),
  fwidth Nullable(String),
  flob Nullable(String),
  fcommented Nullable(String),
  fboolean Nullable(UInt8)
)

ENGINE = ReplicatedMergeTree(${replicationParams})
ORDER BY (id, fsint32)
PARTITION BY (toYYYYMM(fdate))
SETTINGS index_granularity = 8192"""


class SchemaTest(unittest.TestCase):

    def test_generate_scala_dataclass(self):
        fc = schema2scala.FileConverter(_DEFAULT_ANNOTATIONS).from_module(
            schema_test_data)
        exports = schema2scala.TypeExports()
        fc.fill_exports(exports)
        contents = fc.to_scala(exports)
        print(f'contents: `{contents}`')
        self.assertEqual(contents, EXPECTED_SCALA)

    def test_generate_sql_dataclass(self):
        fc = schema2sql.FileConverter().from_module(schema_test_data)
        fc.validate()
        result = fc.to_sql()
        # self.maxDiff = None  # pylint: disable=invalid-name
        # print(f'TestJoinProto: `{result['TestJoinProto']}`')
        self.assertEqual(result['TestJoinProto'], EXPECTED_SQL_TESTJOINPROTO)
        # print(f'TestProto: `{result['TestProto']}`')
        self.assertEqual(result['TestProto'], EXPECTED_SQL_TESTPROTO)

    def test_generate_scala_proto(self):
        fc = schema2scala.FileConverter(_DEFAULT_ANNOTATIONS).from_proto_file(
            schema_test_pb2.DESCRIPTOR)
        exports = schema2scala.TypeExports()
        fc.fill_exports(exports)
        contents = fc.to_scala(exports)
        # print(f'contents: `{contents}`')
        self.assertEqual(contents, EXPECTED_SCALA)

    def test_generate_sql_proto(self):
        fc = schema2sql.FileConverter().from_proto_file(
            schema_test_pb2.DESCRIPTOR)
        fc.validate()
        result = fc.to_sql()
        # print(f'TestJoinProto: `{result['TestJoinProto']}`')
        self.assertEqual(result['TestJoinProto'], EXPECTED_SQL_TESTJOINPROTO)
        # print(f'TestProto: `{result['TestProto']}`')
        self.assertEqual(result['TestProto'], EXPECTED_SQL_TESTPROTO)

    def test_errors(self):
        with self.assertRaisesRegex(ValueError,
                                    'Expecting decimal info annotation.*'):
            conv = schema2sql.TableConverter(
                proto2schema.ConvertMessage(
                    schema_test_bad_pb2.BadDecimalMissing.DESCRIPTOR))
            conv.validate()
        with self.assertRaisesRegex(
                ValueError,
                'Decimal info annotation present for non decimal field'):
            conv = schema2sql.TableConverter(
                proto2schema.ConvertMessage(
                    schema_test_bad_pb2.BadDecimalType.DESCRIPTOR))
            conv.validate()
        with self.assertRaisesRegex(
                ValueError, 'Decimal precision for field .* is too large .*'):
            conv = schema2sql.TableConverter(
                proto2schema.ConvertMessage(
                    schema_test_bad_pb2.BadDecimalPrecision.DESCRIPTOR))
            conv.validate()
        with self.assertRaisesRegex(
                ValueError,
                'Decimal precision for field .* is less than the scale .*'):
            conv = schema2sql.TableConverter(
                proto2schema.ConvertMessage(
                    schema_test_bad_pb2.BadDecimalScale.DESCRIPTOR))
            conv.validate()
        with self.assertRaisesRegex(ValueError,
                                    'Timestamp precision for field .*'):
            conv = schema2sql.TableConverter(
                proto2schema.ConvertMessage(
                    schema_test_bad_pb2.BadTimestampPrecision.DESCRIPTOR))
            conv.validate()
        with self.assertRaisesRegex(
                ValueError,
                'Timestamp info annotation present for non timestamp .*'):
            conv = schema2sql.TableConverter(
                proto2schema.ConvertMessage(
                    schema_test_bad_pb2.BadTimestampType.DESCRIPTOR))
            conv.validate()
        with self.assertRaisesRegex(
                ValueError, 'Delta_compression cannot be used for type.*'):
            conv = schema2sql.TableConverter(
                proto2schema.ConvertMessage(
                    schema_test_bad_pb2.BadDeltaCompressionType.DESCRIPTOR))
            conv.validate()
        with self.assertRaisesRegex(ValueError,
                                    'Invalid delta_compression value.*'):
            conv = schema2sql.TableConverter(
                proto2schema.ConvertMessage(
                    schema_test_bad_pb2.BadDeltaCompressionValue.DESCRIPTOR))
            conv.validate()
        with self.assertRaisesRegex(ValueError,
                                    'Delta compression too large.*'):
            conv = schema2sql.TableConverter(
                proto2schema.ConvertMessage(
                    schema_test_bad_pb2.BadDeltaCompressionRange.DESCRIPTOR))
            conv.validate()
        with self.assertRaisesRegex(
                ValueError,
                'Compression level can be specified only for ZSTD.*'):
            conv = schema2sql.TableConverter(
                proto2schema.ConvertMessage(
                    schema_test_bad_pb2.BadDeltaCompressionLevel.DESCRIPTOR))
            conv.validate()
        with self.assertRaisesRegex(
                ValueError, 'Low cardinality fields allowed only for string.*'):
            conv = schema2sql.TableConverter(
                proto2schema.ConvertMessage(
                    schema_test_bad_pb2.BadLowCardinality.DESCRIPTOR))
            conv.validate()
        with self.assertRaisesRegex(
                ValueError, 'Cannot have ORDER BY with the provided Engine.*'):
            conv = schema2sql.TableConverter(
                proto2schema.ConvertMessage(
                    schema_test_bad_pb2.BadOrderByEngine.DESCRIPTOR))
            conv.validate()
        with self.assertRaisesRegex(
                ValueError,
                'Cannot have PARTITION BY with the provided Engine'):
            conv = schema2sql.TableConverter(
                proto2schema.ConvertMessage(
                    schema_test_bad_pb2.BadPartitionByEngine.DESCRIPTOR))
            conv.validate()
        with self.assertRaisesRegex(
                ValueError,
                'Cannot specify index granularity for the provided Engine'):
            conv = schema2sql.TableConverter(
                proto2schema.ConvertMessage(
                    schema_test_bad_pb2.BadIndexGranularityEngine.DESCRIPTOR))
            conv.validate()
        with self.assertRaisesRegex(
                ValueError, 'ORDER BY field allowed only for required one.*'):
            conv = schema2sql.TableConverter(
                proto2schema.ConvertMessage(
                    schema_test_bad_pb2.BadOrderByField.DESCRIPTOR))
            conv.validate()
        with self.assertRaisesRegex(
                ValueError,
                'PARTITION BY field allowed only for required ones'):
            conv = schema2sql.TableConverter(
                proto2schema.ConvertMessage(
                    schema_test_bad_pb2.BadPartitionByField.DESCRIPTOR))
            conv.validate()
        with self.assertRaisesRegex(
                ValueError, 'SAMPLE BY field allowed only for required ones'):
            conv = schema2sql.TableConverter(
                proto2schema.ConvertMessage(
                    schema_test_bad_pb2.BadSampleByField.DESCRIPTOR))
            conv.validate()
        with self.assertRaisesRegex(ValueError,
                                    'ORDER BY field `idd` not found in .*'):
            conv = schema2sql.TableConverter(
                proto2schema.ConvertMessage(
                    schema_test_bad_pb2.BadOrderByFieldName.DESCRIPTOR))
            conv.validate()

    def test_generate_example_proto(self):
        fc = schema2scala.FileConverter(_DEFAULT_ANNOTATIONS).from_proto_file(
            schema_example_pb2.DESCRIPTOR)
        exports = schema2scala.TypeExports()
        fc.fill_exports(exports)
        print(f'Example Scala:\n{fc.to_scala(schema2scala.TypeExports())}')
        fs = schema2sql.FileConverter().from_proto_file(
            schema_example_pb2.DESCRIPTOR)
        fs.validate()
        for s in fs.to_sql().values():
            print(f'Example Sql:\n{s}')

    def test_generate_example_dataclass(self):
        fc = schema2scala.FileConverter(_DEFAULT_ANNOTATIONS).from_module(
            schema_example)
        exports = schema2scala.TypeExports()
        fc.fill_exports(exports)
        print(f'Example Scala:\n{fc.to_scala(schema2scala.TypeExports())}')
        fs = schema2sql.FileConverter().from_module(schema_example)
        fs.validate()
        for s in fs.to_sql().values():
            print(f'Example Sql:\n{s}')


if __name__ == '__main__':
    unittest.main()
