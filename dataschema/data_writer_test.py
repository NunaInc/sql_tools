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
"""Tests the synthetic data writer."""

import dataclasses
import datetime
import decimal
import os
import pandas
import shutil
import sqlalchemy
import tempfile
import unittest

from dataschema.entity import Annotate
from dataschema import annotations
from dataschema import data_writer
from dataschema import entity
from dataschema import python2schema
from dataschema import schema_synth
from dataschema import schema2sqlalchemy

from typing import List, Optional


@dataclasses.dataclass
class A:
    a_id: Annotate(int, annotations.Id())
    name: str
    common_stuff: Optional[str]
    date: datetime.date
    attribute: int
    cost: Annotate(decimal.Decimal, annotations.Decimal(10, 2))


@dataclasses.dataclass
class B:
    b_id: Annotate(int, annotations.Id())
    a_id: int
    relation: Optional[int]
    attribute: List[float]


class SynthSchemaTest(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def generate(self,
                 output_writer,
                 output_type,
                 size,
                 dir_name,
                 ext='',
                 shards=None,
                 data_classes=None):
        builder = schema_synth.Builder()
        if data_classes is None:
            data_classes = [A, B]
        gens = builder.schema_generator(output_type=output_type,
                                        data_classes=data_classes)
        file_info = [
            schema_synth.FileGeneratorInfo(gen, size, gen.name(), ext, shards)
            for gen in gens
        ]
        return schema_synth.GenerateFiles(
            file_info, output_writer,
            os.path.join(self.test_dir, dir_name) if dir_name else '')

    def test_print(self):
        self.generate(data_writer.PrintWriter(),
                      schema_synth.OutputType.DATACLASS, 5, 'print')

    def check_a_dataframe(self, a, size):
        self.assertEqual(list(a.columns),
                         [f.name for f in dataclasses.fields(A)])
        self.assertTrue(all(a['a_id'] == range(1, size + 1)))

    def check_b_dataframe(self, b, size):
        self.assertEqual(list(b.columns),
                         [f.name for f in dataclasses.fields(B)])
        self.assertTrue(all(b['b_id'] == range(1, size + 1)))

    def test_csv(self):
        size = 10
        files = self.generate(data_writer.CsvWriter(),
                              schema_synth.OutputType.DATAFRAME, size, 'csv',
                              '.csv')
        a_data = pandas.read_csv(files['A'][0])
        b_data = pandas.read_csv(files['B'][0])
        self.check_a_dataframe(a_data, size)
        self.check_b_dataframe(b_data, size)

    def test_json(self):
        size = 10
        files = self.generate(data_writer.JsonWriter(),
                              schema_synth.OutputType.DICT, size, 'json',
                              '.json')
        a_data = pandas.io.json.read_json(files['A'][0])
        b_data = pandas.io.json.read_json(files['B'][0])
        self.check_a_dataframe(a_data, size)
        self.check_b_dataframe(b_data, size)

    def test_pickle(self):
        size = 10
        files = self.generate(data_writer.PickleWriter(),
                              schema_synth.OutputType.DICT, size, 'pickle',
                              '.pickle')
        a_data = pandas.DataFrame(pandas.read_pickle(files['A'][0]))
        b_data = pandas.DataFrame(pandas.read_pickle(files['B'][0]))
        self.check_a_dataframe(a_data, size)
        self.check_b_dataframe(b_data, size)

    def test_parquet(self):
        size = 10
        files = self.generate(data_writer.ParquetWriter(),
                              schema_synth.OutputType.DATAFRAME, size,
                              'parquet', '.parquet')
        a_data = pandas.read_parquet(files['A'][0])
        b_data = pandas.read_parquet(files['B'][0])
        self.check_a_dataframe(a_data, size)
        self.check_b_dataframe(b_data, size)

    def test_sql_alchemy(self):
        size = 10
        db_path = os.path.join(self.test_dir, 'sqltest.db')
        # Note: four slashes for absolute paths !
        db_url = f'sqlite:///{db_path}'
        engine = sqlalchemy.create_engine(db_url, echo=True)
        engine.connect()
        meta = sqlalchemy.MetaData()
        table_a = schema2sqlalchemy.ConvertTable(
            python2schema.ConvertDataclass(A), meta=meta)
        # Note: arrays not supported by sqlite - skipping B
        meta.create_all(engine)
        tables = self.generate(data_writer.SqlAlchemyWriter(engine),
                               schema_synth.OutputType.DATAFRAME,
                               size,
                               '',
                               data_classes=[A])
        print(f'SQL tables generated: {tables}')
        connection = engine.connect()
        query = sqlalchemy.select([table_a])
        results = connection.execute(query).fetchall()
        print(f'Sql data:\n{results}')
        self.assertEqual(len(results), 10)
        self.assertEqual([res[0] for res in results], list(range(1, size + 1)))
        self.assertEqual(len(results[0]), len(dataclasses.fields(A)))
        for res in results:
            for (value, field) in zip(res, dataclasses.fields(A)):
                if value is None:
                    self.assertEqual(field.name, 'common_stuff')
                else:
                    self.assertTrue(
                        isinstance(value, entity.GetOriginalType(field.type)),
                        f'field: {field} => {value}')


if __name__ == '__main__':
    unittest.main()
