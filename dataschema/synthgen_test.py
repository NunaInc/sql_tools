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
"""Testing synthetic data generators."""

import dataclasses
import datetime
import decimal
import time
import unittest
from dataschema import synthgen
from numpy.random import Generator, PCG64
from typing import Any, Dict, List, Set


def rand():
    return Generator(PCG64())


@dataclasses.dataclass
class FooData:
    foo: int
    bar: List[int]


def add(x, y) -> int:
    return x + y


def add_by2(x, y) -> int:
    return 2 * x + y


class SynthGenTest(unittest.TestCase):

    def check_values(self, spec, out_type, expected):
        gen = synthgen.BuildGenerator(rand(), spec)
        self.assertTrue(gen.result_type(), out_type)
        self.assertEqual(gen.save(), spec)
        self.assertEqual(gen.generate(), expected)
        self.assertEqual(gen.generate(4), [expected] * 4)
        gen2 = synthgen.BuildGenerator(rand(), gen.save())
        self.assertEqual(gen2.save(), spec)

    def test_values(self):
        self.check_values(('val', 23), int, 23)
        self.check_values(('todate', ('val', '2020-10-03')), datetime.date,
                          datetime.date.fromisoformat('2020-10-03'))
        self.check_values(
            ('todatetime', ('val', '2020-10-03T14:52:08.577702')),
            datetime.datetime,
            datetime.datetime.fromisoformat('2020-10-03T14:52:08.577702'))
        self.check_values(('todecimal', ('val', '12345.654321')),
                          decimal.Decimal, decimal.Decimal('12345.654321'))
        self.check_values(('date', (('todate', ('val', '2020-10-03')),
                                    ('val', 5))), datetime.date,
                          datetime.date.fromisoformat('2020-10-03') +
                          datetime.timedelta(days=5))
        self.check_values(
            ('datetime', (('todatetime', ('val', '2020-10-03T14:52:08.577702')),
                          ('val', 4235438))), datetime.datetime,
            datetime.datetime.fromisoformat('2020-10-03T14:52:08.577702') +
            datetime.timedelta(seconds=4235438))
        self.check_values(('array', (('val', 1234), ('val', 3))), List[int],
                          [1234, 1234, 1234])
        self.check_values(('elem_array', (('val', 1234), ('val', 4))),
                          List[int], [1234, 1234, 1234, 1234])
        self.check_values(('set', ('array', (('val', 1234), ('val', 3)))),
                          Set[int], {1234})
        self.check_values(('seq', (('val', 5), ('val', 3))), List[int],
                          [5, 6, 7])
        self.check_values(('nested', {
            'foo': ('val', 5),
            'bar': ('val', 'zip'),
            'baz': ('val', 22.2)
        }), Dict[str, Any], {
            'foo': 5,
            'bar': 'zip',
            'baz': 22.2
        })
        self.check_values(('tobool', ('val', 22)), bool, True)
        self.check_values(('tobool', ('val', 0)), bool, False)
        self.check_values(('tostring', ('val', 234)), str, '234')
        self.check_values(('concat', ([('val', 'x'), ('val', 'y'),
                                       ('val', 'z')], '.')), str, 'x.y.z')
        self.check_values(('format', ([('val', 'x'), ('val', 'y'),
                                       ('val', 'z')], 'get {} on {} to {}')),
                          str, 'get x on y to z')

    def check_generator(self, spec, out_type, check):
        gen = synthgen.BuildGenerator(rand(), spec)
        self.assertTrue(gen.result_type(), out_type)
        self.assertEqual(gen.save(), spec)
        self.assertTrue(check(gen.generate()))
        self.assertTrue(all(check(x) for x in gen.generate(4)))
        gen2 = synthgen.BuildGenerator(rand(), gen.save())
        self.assertEqual(gen2.save(), spec)

    def test_randoms(self):
        self.check_generator(('uniform', (5, 10)), float, lambda x: 5 <= x < 10)
        self.check_generator(('normal', (100, 20)), float,
                             lambda x: isinstance(x, float))
        self.check_generator(('int', (5, 10)), int, lambda x: 5 <= x < 10)
        self.check_generator(('exp', 3), float, lambda x: x > 0)
        self.check_generator(('geom', 0.2), float,
                             lambda x: x > 0 and float(x) == int(x))
        self.check_generator(('pareto', 10), float, lambda x: x > 0)
        self.check_generator(('choice', ([('val', 'foo'), ('val', 'bar'),
                                          ('val', 'baz')], [0.8, 0.15, 0.05])),
                             str, lambda x: x in {'foo', 'bar', 'baz'})
        self.check_generator(
            ('str', (('val', 3),)), str,
            lambda s: len(s) == 3 and all(c in synthgen.ALPHABET for c in s))

    def test_inc(self):
        spec = ('inc', 3)
        gen = synthgen.BuildGenerator(rand(), spec)
        self.assertTrue(gen.result_type(), int)
        self.assertEqual(gen.save(), spec)
        self.assertEqual(gen.generate(), 3)
        self.assertEqual(gen.generate(4), [4, 5, 6, 7])
        gen2 = synthgen.BuildGenerator(rand(), gen.save())
        self.assertEqual(gen2.save(), spec)

    def test_inc_limit(self):
        spec = ('inc', (-1, -1, 4))
        gen = synthgen.BuildGenerator(rand(), spec)
        self.assertEqual(gen.generate(10), [-1, 2, 1, 0, -1, 2, 1, 0, -1, 2])
        spec = ('inc', (-1, 1, 4))
        gen = synthgen.BuildGenerator(rand(), spec)
        self.assertEqual(gen.generate(10), [-1, 0, 1, 2, -1, 0, 1, 2, -1, 0])

    def test_dict(self):
        spec = ('dict', (('inc', 1), ('inc', 10), ('val', 2)))
        gen = synthgen.BuildGenerator(rand(), spec)
        self.assertTrue(gen.result_type(), Dict[int, int])
        self.assertEqual(gen.save(), spec)
        self.assertEqual(gen.generate(), {1: 10, 2: 11})
        self.assertEqual(gen.generate(3), [{
            3: 12,
            4: 13
        }, {
            5: 14,
            6: 15
        }, {
            7: 16,
            8: 17
        }])
        gen2 = synthgen.BuildGenerator(rand(), gen.save())
        self.assertEqual(gen2.save(), spec)

    def test_dataframe(self):
        spec = ('dataframe', {
            'foo': ('inc', 1),
            'bar': ('seq', (('inc', 30), ('val', 3)))
        })
        gen = synthgen.BuildGenerator(rand(), spec)
        self.assertTrue(gen.result_type(), Dict[str, Any])
        self.assertEqual(gen.save(), spec)
        self.assertEqual(gen.generate(), {'foo': [1], 'bar': [[30, 31, 32]]})
        self.assertEqual(gen.generate(3), {
            'foo': [2, 3, 4],
            'bar': [[31, 32, 33], [32, 33, 34], [33, 34, 35]]
        })
        gen2 = synthgen.BuildGenerator(rand(), gen.save())
        self.assertEqual(gen2.save(), spec)

    def test_dataclass(self):
        spec = ('dataclass', ({
            'foo': ('inc', 1),
            'bar': ('seq', (('inc', 30), ('val', 3)))
        }, '__main__', 'FooData'))
        gen = synthgen.BuildGenerator(rand(), spec)
        self.assertTrue(gen.result_type(), Dict[str, Any])
        self.assertEqual(gen.save(), spec)
        self.assertEqual(gen.generate(), FooData(foo=1, bar=[30, 31, 32]))
        self.assertEqual(gen.generate(3), [
            FooData(foo=2, bar=[31, 32, 33]),
            FooData(foo=3, bar=[32, 33, 34]),
            FooData(foo=4, bar=[33, 34, 35]),
        ])
        gen2 = synthgen.BuildGenerator(rand(), gen.save())
        self.assertEqual(gen2.save(), spec)

    def test_str_benchmark(self):
        gen = synthgen.BuildGenerator(rand(), ('str', 20))
        start_time = time.time()
        _ = gen.generate(100000)
        end_time = time.time()
        print(f'Generated 100000 column strings in: {end_time - start_time}')
        for _ in range(10000):
            gen.generate()
        print(
            f'Generated 10000 individual strings in: {time.time() - end_time}')

    def test_faker(self):
        if synthgen.faker is None:
            print('Faker not installed - skipping test')
        self.check_generator(('faker', ('name', None)), str,
                             lambda x: print(x) or True)
        self.check_generator(('faker', ('latitude', None)), decimal.Decimal,
                             lambda x: print(x) or True)

    def test_joint(self):
        spec = ('joint', (('int', (0, 100)), []))
        gen = synthgen.BuildGenerator(rand(), spec)
        self.assertEqual(spec, gen.save())
        history = [gen.generate() for _ in range(50)]
        self.assertEqual([gen.generate(key='foo') for _ in range(50)], history)
        self.assertEqual([gen.generate(key='bar') for _ in range(50)], history)
        next_history = [gen.generate(key='foo') for _ in range(20)]
        self.assertEqual([gen.generate(key='bar') for _ in range(20)],
                         next_history)

    def test_joint_create(self):
        joint_fields = {'A.foo': 'foo', 'B.foo': 'foo'}
        builder = synthgen.Builder(rand(), joint_fields=joint_fields)
        spec = ('dataclass', ({
            'foo': ('inc', 1),
            'bar': ('seq', (('inc', 30), ('val', 3)))
        }, '__main__', 'FooData'))
        gen_a = builder.set_prefix('A').build(spec)
        gen_b = builder.set_prefix('B').build(spec)
        spec[1][0]['foo'] = ('joint', (('inc', 1), []))
        self.assertEqual(gen_a.save(), spec)
        spec[1][0]['foo'] = ('joint', (('inc', 1), []))
        self.assertEqual(gen_b.save(), spec)
        values = [gen_a.generate(key='A').foo for _ in range(20)]
        self.assertEqual([gen_b.generate(key='B').foo for _ in range(20)],
                         values)

    def test_field(self):
        spec = ('nested', {
            'a': ('inc', 1),
            'b': ('field', ('a', ('val', None), ('inc', (0, 1, 4))))
        })
        gen = synthgen.Builder(rand()).build(spec).with_context()
        self.assertEqual(gen.save(), spec)
        self.assertEqual(gen.generate(5), [{
            'a': 1,
            'b': 1
        }, {
            'a': 2,
            'b': 1
        }, {
            'a': 3,
            'b': 1
        }, {
            'a': 4,
            'b': 1
        }, {
            'a': 5,
            'b': 5
        }])

    def test_sub_field(self):
        spec = ('nested', {
            'x': ('nested', {
                'a': ('inc', 1),
                'b': ('field', ('x.a', ('val', None), ('inc', (0, 1, 4))))
            }),
            'y': ('field', ('x.a', ('val', None), ('val', 0)))
        })
        gen = synthgen.Builder(rand()).build(spec).with_context()
        self.assertEqual(gen.save(), spec)
        self.assertEqual(gen.generate(5), [{
            'x': {
                'a': 1,
                'b': 1
            },
            'y': 1
        }, {
            'x': {
                'a': 2,
                'b': 1
            },
            'y': 2
        }, {
            'x': {
                'a': 3,
                'b': 1
            },
            'y': 3
        }, {
            'x': {
                'a': 4,
                'b': 1
            },
            'y': 4
        }, {
            'x': {
                'a': 5,
                'b': 5
            },
            'y': 5
        }])

    def test_simple_apply(self):
        spec = ('apply', (add_by2, [('inc', 1), ('inc', 10)], int))
        gen = synthgen.Builder(rand()).build(spec)
        saved = gen.save()
        self.assertEqual(len(saved), 2)
        self.assertEqual(saved[0], 'apply')
        self.assertEqual(len(saved[1]), 3)
        self.assertTrue(isinstance(saved[1][0], bytes))
        self.assertEqual(saved[1][1], spec[1][1])
        self.assertTrue(isinstance(saved[1][2], bytes))
        expected = [12, 15, 18, 21, 24, 27, 30, 33, 36, 39]
        self.assertEqual(gen.generate(10), expected)
        gen2 = synthgen.Builder(rand()).build(saved)
        self.assertEqual(gen2.save(), saved)
        self.assertEqual(gen2.generate(10), expected)

    def test_apply(self):
        spec = ('nested', {
            'x': ('nested', {
                'a': ('inc', 1),
                'b': ('field', ('x.a', ('val', None), ('inc', (0, 1, 4))))
            }),
            'y': ('field', ('x.a', ('val', None), ('val', 0))),
            'z': ('apply', (add, [('field', 'x.b'), ('field', 'x.a')]))
        })
        gen = synthgen.Builder(rand()).build(spec).with_context()
        self.assertEqual(gen.generate(10), [{
            'x': {
                'a': 1,
                'b': 1
            },
            'y': 1,
            'z': 2
        }, {
            'x': {
                'a': 2,
                'b': 1
            },
            'y': 2,
            'z': 3
        }, {
            'x': {
                'a': 3,
                'b': 1
            },
            'y': 3,
            'z': 4
        }, {
            'x': {
                'a': 4,
                'b': 1
            },
            'y': 4,
            'z': 5
        }, {
            'x': {
                'a': 5,
                'b': 5
            },
            'y': 5,
            'z': 10
        }, {
            'x': {
                'a': 6,
                'b': 5
            },
            'y': 6,
            'z': 11
        }, {
            'x': {
                'a': 7,
                'b': 5
            },
            'y': 7,
            'z': 12
        }, {
            'x': {
                'a': 8,
                'b': 5
            },
            'y': 8,
            'z': 13
        }, {
            'x': {
                'a': 9,
                'b': 9
            },
            'y': 9,
            'z': 18
        }, {
            'x': {
                'a': 10,
                'b': 9
            },
            'y': 10,
            'z': 19
        }])

    def test_extra_nested(self):
        spec = ('nested', {
            'top': ('nested', {
                'x': ('nested', {
                    'a': ('inc', 1),
                    'b': ('field', ('top.x.a', ('val', None), ('inc', (0, 1,
                                                                       4))))
                })
            }),
            'y': ('field', ('top.x.a', ('val', None), ('val', 0)))
        })
        gen = synthgen.Builder(rand()).build(spec).with_context()
        self.assertEqual(gen.save(), spec)
        self.assertEqual(gen.generate(5), [{
            'top': {
                'x': {
                    'a': 1,
                    'b': 1
                }
            },
            'y': 1
        }, {
            'top': {
                'x': {
                    'a': 2,
                    'b': 1
                }
            },
            'y': 2
        }, {
            'top': {
                'x': {
                    'a': 3,
                    'b': 1
                }
            },
            'y': 3
        }, {
            'top': {
                'x': {
                    'a': 4,
                    'b': 1
                }
            },
            'y': 4
        }, {
            'top': {
                'x': {
                    'a': 5,
                    'b': 5
                }
            },
            'y': 5
        }])

    def test_df_sub_field(self):
        spec = ('dataframe', {
            'x': ('nested', {
                'a': ('inc', 1),
                'b': ('field', ('x.a', ('val', None), ('inc', (0, 1, 4))))
            }),
            'y': ('field', ('x.a', ('val', None), ('val', 0)))
        })
        gen = synthgen.Builder(rand()).build(spec).with_context()
        self.assertEqual(gen.save(), spec)
        self.assertEqual(
            gen.generate(5), {
                'x': [{
                    'a': 1,
                    'b': 1
                }, {
                    'a': 2,
                    'b': 1
                }, {
                    'a': 3,
                    'b': 1
                }, {
                    'a': 4,
                    'b': 1
                }, {
                    'a': 5,
                    'b': 5
                }],
                'y': [1, 2, 3, 4, 5]
            })

    def test_df_extra_nested(self):
        spec = ('dataframe', {
            'top': ('nested', {
                'x': ('nested', {
                    'a': ('inc', 1),
                    'b': ('field', ('top.x.a', ('val', None), ('inc', (0, 1,
                                                                       4))))
                })
            }),
            'y': ('field', ('top.x.a', ('val', None), ('val', 0)))
        })
        gen = synthgen.Builder(rand()).build(spec).with_context()
        self.assertEqual(gen.save(), spec)
        self.assertEqual(
            gen.generate(5), {
                'top': [{
                    'x': {
                        'a': 1,
                        'b': 1
                    }
                }, {
                    'x': {
                        'a': 2,
                        'b': 1
                    }
                }, {
                    'x': {
                        'a': 3,
                        'b': 1
                    }
                }, {
                    'x': {
                        'a': 4,
                        'b': 1
                    }
                }, {
                    'x': {
                        'a': 5,
                        'b': 5
                    }
                }],
                'y': [1, 2, 3, 4, 5]
            })

    def test_df_self_ref(self):
        spec = ('dataframe', {
            'a': ('field', ('a', ('inc', 1), ('inc', (0, 1, 3))))
        })
        gen = synthgen.Builder(rand()).build(spec).with_context()
        self.assertEqual(gen.generate(10),
                         {'a': [1, 1, 1, 2, 2, 2, 3, 3, 3, 4]})

    def test_self_ref(self):
        spec = ('nested', {
            'a': ('field', ('a', ('inc', 1), ('inc', (0, 1, 3))))
        })
        gen = synthgen.Builder(rand()).build(spec).with_context()
        data = gen.generate(10)
        self.assertEqual([x['a'] for x in data], [1, 1, 1, 2, 2, 2, 3, 3, 3, 4])


if __name__ == '__main__':
    unittest.main()
