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
"""Tests the synthetic schema generation."""

import json
import numpy.random
import unittest
from dataclasses import dataclass
from dataschema.entity import Annotate
from dataschema import Schema_pb2
from dataschema import annotations
from dataschema import python2schema
from dataschema import schema_synth
from dataschema import schema_test_data
from dataschema import synthgen
from typing import List, Optional

NONE_PROB = [
    1.0 - schema_synth.DEFAULT_NONE_PROBABILITY,
    schema_synth.DEFAULT_NONE_PROBABILITY
]
EXPECTED_TABLE_GEN = ('nested', {
    'id': schema_synth.DEFAULT_INT_ID,
    'fint32': schema_synth.DEFAULT_INT,
    'fsint32': schema_synth.DEFAULT_INT,
    'fint64': schema_synth.DEFAULT_INT,
    'fsint64': ('choice', ([schema_synth.DEFAULT_INT, None], NONE_PROB)),
    'fdouble': ('choice', ([schema_synth.DEFAULT_FLOAT, None], NONE_PROB)),
    'ffloat': ('choice', ([schema_synth.DEFAULT_FLOAT, None], NONE_PROB)),
    'fstring': ('choice', ([schema_synth.DEFAULT_STRING, None], NONE_PROB)),
    'fbytes': ('choice', ([schema_synth.DEFAULT_STRING, None], NONE_PROB)),
    'fdate': ('choice', ([schema_synth.DEFAULT_DATE, None], NONE_PROB)),
    'ftimestamp': ('choice', ([schema_synth.DEFAULT_DATETIME,
                               None], NONE_PROB)),
    'fdqannotated': ('choice', ([schema_synth.DEFAULT_STRING,
                                 None], NONE_PROB)),
    'frep_seq':
        ('array', (schema_synth.DEFAULT_STRING, schema_synth.DEFAULT_LENGTH)),
    'frep_array':
        ('array', (schema_synth.DEFAULT_STRING, schema_synth.DEFAULT_LENGTH)),
    'frep_set': ('set', ('array', (schema_synth.DEFAULT_STRING,
                                   schema_synth.DEFAULT_LENGTH))),
    'fdecimal_bigint': ('choice', ([schema_synth.DEFAULT_DECIMAL,
                                    None], NONE_PROB)),
    'fdecimal_default': ('choice', ([schema_synth.DEFAULT_DECIMAL,
                                     None], NONE_PROB)),
    'fdecimal_bigdecimal': ('choice', ([schema_synth.DEFAULT_DECIMAL,
                                        None], NONE_PROB)),
    'with__original__name': ('choice', ([schema_synth.DEFAULT_STRING,
                                         None], NONE_PROB)),
    'finitialized':
        ('array', (schema_synth.DEFAULT_STRING, schema_synth.DEFAULT_LENGTH)),
    'fwidth': ('choice', ([schema_synth.DEFAULT_STRING, None], NONE_PROB)),
    'flob': ('choice', ([schema_synth.DEFAULT_STRING, None], NONE_PROB)),
    'fcommented': ('choice', ([schema_synth.DEFAULT_STRING, None], NONE_PROB)),
    'fboolean': ('choice', ([schema_synth.DEFAULT_BOOL, None], NONE_PROB))
})


class SynthSchemaTest(unittest.TestCase):

    def test_all_types(self):
        builder = schema_synth.Builder()
        gen = builder.table_generator(data_class=schema_test_data.TestProto)
        self.assertEqual(gen, EXPECTED_TABLE_GEN)
        generator = synthgen.BuildGenerator(
            numpy.random.Generator(numpy.random.PCG64()), gen)
        print('--- Some generated classes:')
        for _ in range(10):
            print(generator.generate())

    def test_options(self):

        @dataclass
        class A:
            foo: int
            bar: Optional[str]
            baz: List[int]

        @dataclass
        class B:
            other: int
            other_list: List[Optional[int]]
            a_val: A

        builder = schema_synth.Builder(
            generators={
                'a_val.foo': 20,
                'other': 10,
                'other_list': 15
            },
            none_probability=0.1,
            length_generators={'a_val': {
                'baz': 3
            }},
            defaults={Schema_pb2.ColumnInfo.TYPE_STRING: 'haha'},
            default_length=4)
        gen = builder.table_generator(
            output_type=schema_synth.OutputType.DATAFRAME, data_class=B)
        expected_b_gen = (
            'dataframe',
            {
                'other':
                    10,
                'other_list': ('array', (15, 4)),
                'a_val': (
                    'nested',
                    {  # internally we have dicts
                        'foo': 20,
                        'bar': ('choice', ([('str', 8), None], [0.9, 0.1])),
                        'baz': ('array', (('int', (0, 10000)), 3))
                    })
            })
        self.assertEqual(gen, expected_b_gen)
        generator = synthgen.BuildGenerator(
            numpy.random.Generator(numpy.random.PCG64()), gen)
        print('--- Some dataframe generated classes:')
        for _ in range(10):
            print(generator.generate())
        gen = builder.table_generator(
            output_type=schema_synth.OutputType.DATACLASS, data_class=B)
        generator = synthgen.BuildGenerator(
            numpy.random.Generator(numpy.random.PCG64()), gen)
        print('--- Some dataclass generated classes:')
        for _ in range(10):
            print(generator.generate())

    def test_joint(self):

        @dataclass
        class A:
            a_id: Annotate(int, annotations.Id())
            name: str
            common_stuff: str

        @dataclass
        class B:
            b_id: Annotate(str, annotations.Id())
            common_stuff: str
            a: A

        @dataclass
        class C:
            c_id: Annotate(int, annotations.Id())
            common_stuff: str
            a_id: int
            b_id: str
            joiner: str

        @dataclass
        class D:
            foo: str
            c: C
            name: int

        builder = schema_synth.Builder()
        (builder_fields, builder_is_choice) = builder.find_joint_fields_by_name(
            [python2schema.ConvertDataclass(dc) for dc in [A, B, C, D]])
        expected_fields = {
            'A.a_id': 'A.a_id',
            'B.a.a_id': 'A.a_id',
            'C.a_id': 'A.a_id',
            'D.c.a_id': 'A.a_id',
            'A.name': 'name-0',
            'B.a.name': 'name-0',  # note D.name not here
            'A.common_stuff': 'common_stuff-1',
            'B.common_stuff': 'common_stuff-1',
            'B.a.common_stuff': 'common_stuff-1',
            'C.common_stuff': 'common_stuff-1',
            'D.c.common_stuff': 'common_stuff-1',
            'B.b_id': 'B.b_id',
            'C.b_id': 'B.b_id',
            'D.c.b_id': 'B.b_id',
            'C.c_id': 'C.c_id',
            'D.c.c_id': 'C.c_id',
            'C.joiner': 'joiner-2',
            'D.c.joiner': 'joiner-2'
        }
        self.assertEqual(builder_fields, expected_fields)
        self.assertEqual(builder_is_choice, {
            'C.a_id', 'D.c.a_id', 'B.a.a_id', 'C.b_id', 'D.c.b_id', 'D.c.c_id'
        })
        # Random order of dataclasses
        classes = [C, D, B, A]
        gens = builder.schema_generator(data_classes=classes)
        expected_a_id = ('joint', (('inc', 1),
                                   ['B.a.a_id', 'C.a_id', 'D.c.a_id']))
        expected_b_id = ('joint', (('concat', ([('val', 'b_id'),
                                                ('tostring', ('inc', 1))],
                                               '_')), ['C.b_id', 'D.c.b_id']))
        expected_c_id = ('joint', (('inc', 1), ['D.c.c_id']))
        expected_joiner = ('joint', (('str', (('val', 8),)), []))
        expected_name_str = expected_joiner
        expected_common = expected_joiner
        expected = [('nested', {
            'a_id': expected_a_id,
            'name': expected_name_str,
            'common_stuff': expected_common
        }),
                    ('nested', {
                        'b_id':
                            expected_b_id,
                        'common_stuff':
                            expected_common,
                        'a': ('nested', {
                            'a_id': expected_a_id,
                            'name': expected_name_str,
                            'common_stuff': expected_common
                        })
                    }),
                    ('nested', {
                        'c_id': expected_c_id,
                        'common_stuff': expected_common,
                        'a_id': expected_a_id,
                        'b_id': expected_b_id,
                        'joiner': expected_joiner
                    }),
                    ('nested', {
                        'foo': ('str', (('val', 8),)),
                        'c': ('nested', {
                            'c_id': expected_c_id,
                            'common_stuff': expected_common,
                            'a_id': expected_a_id,
                            'b_id': expected_b_id,
                            'joiner': expected_joiner
                        }),
                        'name': ('int', (0, 10000))
                    })]
        self.assertEqual([g.table.name() for g in gens], ['A', 'B', 'C', 'D'])
        self.assertEqual([g.generator.save() for g in gens], expected)
        # More we expect exactly the same objects (test just some):
        self.assertEqual(gens[0].generator.nested('a_id'),
                         gens[1].generator.nested('a').nested('a_id'))
        self.assertEqual(gens[0].generator.nested('a_id'),
                         gens[2].generator.nested('a_id'))
        self.assertEqual(gens[0].generator.nested('a_id'),
                         gens[3].generator.nested('c').nested('a_id'))
        self.assertEqual(gens[0].generator.nested('common_stuff'),
                         gens[1].generator.nested('common_stuff'))
        self.assertEqual(gens[0].generator.nested('common_stuff'),
                         gens[1].generator.nested('a').nested('common_stuff'))
        order = builder.find_generator_order([cls.__name__ for cls in classes])
        self.assertEqual([classes[i] for i in order], [A, B, C, D])

        # Print some generated data:
        for gen in gens:
            print(f'---> Synthetic data for: {gen.table.name()}')
            for _ in range(20):
                print(gen.generator.generate(key=gen.table.name()))

    def test_annotated(self):

        @dataclass
        class A:
            foo: Annotate(int, annotations.SynthGen(('inc', 1)))
            bar: Annotate(List[int], annotations.SynthGen(('int', (1, 10))))

        table = python2schema.ConvertDataclass(A)
        builder = schema_synth.Builder()
        gen = builder.table_generator(table=table)
        self.assertEqual(gen, ('nested', {
            'foo': ('inc', 1),
            'bar': ('array', (('int', (1, 10)), builder.default_length))
        }))
        self.assertEqual(table.columns[0].info.synthgen_spec, '["inc", 1]')
        gen_foo = synthgen.BuildGenerator(
            None, json.loads(table.columns[0].info.synthgen_spec))
        self.assertEqual(gen_foo.save(), ('inc', 1))
        self.assertEqual(table.columns[1].info.synthgen_spec,
                         '["int", [1, 10]]')
        gen_bar = synthgen.BuildGenerator(
            None, json.loads(table.columns[1].info.synthgen_spec))
        self.assertEqual(gen_bar.save(), ('int', (1, 10)))


if __name__ == '__main__':
    unittest.main()
