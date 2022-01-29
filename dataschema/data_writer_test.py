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
import os
import pandas
import shutil
import tempfile
import unittest

from dataschema.entity import Annotate
from dataschema import annotations
from dataschema import data_writer
from dataschema import schema_synth

from typing import List, Optional


@dataclasses.dataclass
class A:
    a_id: Annotate(int, annotations.Id())
    name: str
    common_stuff: str
    date: datetime.date
    attribute: int


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
                 shards=None):
        builder = schema_synth.Builder()
        gens = builder.schema_generator(output_type=output_type,
                                        data_classes=[A, B])
        file_info = [
            schema_synth.FileGeneratorInfo(gen, size, gen.name(), ext, shards)
            for gen in gens
        ]
        return schema_synth.GenerateFiles(file_info, output_writer,
                                          os.path.join(self.test_dir, dir_name))

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


if __name__ == '__main__':
    unittest.main()
