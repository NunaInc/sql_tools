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
"""Module with generators for synthetic data. Check the doc for BuildGenerator."""

import decimal
import datetime
try:
    import faker
except ModuleNotFoundError:
    # The faker is unavailable
    faker = None

try:
    import dill
    encoder_module = dill
except ModuleNotFoundError:
    # Dill not found - using pickle for functions
    import pickle
    encoder_module = pickle

import numpy.random
import sys
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union


def _IsOptional(cls: type) -> bool:
    """If the cls looks like an Optional[Any] type."""
    return (hasattr(cls, '__origin__')
            # pylint: disable=comparison-with-callable
            and cls.__origin__ == Union and len(cls.__args__) == 2
            and cls.__args__[1] == type(None))


def _IsOptionalType(cls: type, base: type) -> bool:
    """If the cls looks like a base or Optional[base] type."""
    if issubclass(cls, base):
        return True
    return (hasattr(cls, '__origin__')
            # pylint: disable=comparison-with-callable
            and cls.__origin__ == Union and len(cls.__args__) == 2
            and issubclass(cls.__args__[0], base)
            and cls.__args__[1] == type(None))


def _GetStructuredTypeName(cls: type) -> str:
    """Returns the structure type name for a type, behind annotation."""
    if not hasattr(cls, '__origin__'):
        return None
    if cls.__origin__ is dict:
        return 'dict'
    elif cls.__origin__ is list:
        return 'list'
    elif cls.__origin__ is set:
        return 'set'
    return None


### Names for our generators - used for named construction.
class Names:
    """Names for all the generators in this files."""
    VALUE = 'val'
    INC = 'inc'
    SEQ = 'seq'
    # Random number generators
    UNIFORM = 'uniform'
    NORMAL = 'normal'
    EXP = 'exp'
    GEOM = 'geom'
    PARETO = 'pareto'
    INT = 'int'
    # Conversion generators
    TOINT = 'toint'
    TODATE = 'todate'
    TODATETIME = 'todatetime'
    TODECIMAL = 'todecimal'
    TOBOOL = 'tobool'
    TOSTRING = 'tostring'
    TOBYTES = 'tobytes'
    # Choice based generators
    CHOICE = 'choice'
    STR = 'str'
    # Structure generators
    ARRAY = 'array'
    ELEM_ARRAY = 'elem_array'
    SET = 'set'
    DICT = 'dict'
    NESTED = 'nested'
    DATAFRAME = 'dataframe'
    DATACLASS = 'dataclass'
    # Combiners
    DATE = 'date'
    DATETIME = 'datetime'
    CONCAT = 'concat'
    FORMAT = 'format'
    APPLY = 'apply'
    # Faker based generator
    FAKER = 'faker'
    # Reference based
    JOINT = 'joint'
    FIELD = 'field'


class ContextField:
    """Class for obtaining a field/subfield specified as `a.b.c` from a dict/record."""

    def __init__(self, full_name: str):
        self._name = full_name.split('.')
        self._column_name = self._name[0]
        self._field_name = self._name[-1]
        self._is_deep = len(self._name) > 1
        self._front_name = self._name[:-1]
        self._columnar_field = (ContextField('.'.join(self._name[1:]))
                                if self._is_deep else None)

    def get(self, record: Dict[str, Any]) -> Any:
        if not self._is_deep:
            return record.get(self._field_name, None)
        return self.record_get(record, self._name)

    def get_columnar(self, record: Dict[str, Any], index: int = 0) -> Any:
        column = record.get(self._column_name, None)
        if column is None or index < 0 or index >= len(column):
            return None
        if not self._is_deep:
            return column[index]
        return self._columnar_field.get(column[index])

    def record_get(self, record: Any, names: List[str]) -> Any:
        for name in names:
            if isinstance(record, dict):
                record = record.get(name, None)
            else:
                record = getattr(record, name, None)
            if record is None:
                return None
        return record

    def set(self, top_record: Dict[str, Any], record: Dict[str, Any]):
        if not self._is_deep:
            top_record[self._field_name] = record
        else:
            sub_record = self.record_get(top_record, self._front_name)
            if sub_record is not None:
                sub_record[self._field_name] = record

    def set_columnar(self, top_record: Dict[str, Any], record: Dict[str, Any]):
        column = top_record.get(self._column_name, None)
        if column is None:
            return
        if not self._is_deep:
            column.append(record)
        else:
            self._columnar_field.set(column[-1], record)


class ContextBase:
    """Base of the context passed to generate.
    Contains the history of generated records."""

    def __init__(self):
        self.field_setters = {}

    def get(self, index: int, field: ContextField):
        raise NotImplementedError('Abstract context')

    def set(self, key: str, record: Dict[str, Any]):
        pass

    def field(self, key: str):
        if key in self.field_setters:
            return self.field_setters[key]
        field = ContextField(key)
        self.field_setters[key] = field
        return field


class FlatContext(ContextBase):
    """A context of flat records - generated for dicts or classes."""

    def __init__(self):
        super().__init__()
        self.records = []

    def append(self, record: Dict[str, Any]):
        self.records.append(record)

    def get(self, index: int, field: ContextField):
        if index < 0 or index >= len(self.records):
            return None
        return field.get(self.records[index])

    def set(self, key: str, record: Dict[str, Any]):
        self.field(key).set(self.records[-1], record)


class ColumnarContext(ContextBase):
    """A context of columnar records - dataframe."""

    def __init__(self, record: Any):
        super().__init__()
        self.record = record

    def get(self, index: int, field: ContextField):
        return field.get_columnar(self.record, index)

    def set(self, key: str, record: Dict[str, Any]):
        self.field(key).set_columnar(self.record, record)


class Builder:
    """Structure used for building generators. Allows joint fields w/ the map options."""

    def __init__(self,
                 rand: Optional[numpy.random.Generator],
                 joint_fields: Optional[Dict[str, str]] = None,
                 joint_is_choice: Optional[Set[str]] = None,
                 name_prefix: Optional[str] = '',
                 joint_map: Optional[Dict[str, 'Generator']] = None,
                 record_structure: Optional[Dict[str, 'Generator']] = None):
        self.rand = rand
        if self.rand is None:
            self.rand = numpy.random.Generator(numpy.random.PCG64())
        self.name_prefix = name_prefix
        self.joint_map = joint_map if joint_map is not None else {}
        self.joint_fields = joint_fields if joint_fields is not None else {}
        self.joint_is_choice = joint_is_choice if joint_is_choice is not None else {}
        self.record_structure = record_structure if record_structure is not None else {}

    def build(self, spec):
        """Builds the generator according to spec."""
        return BuildGenerator(self, spec)

    def set_prefix(self, name_prefix):
        """Changes the name prefix for this builder to start another table."""
        self.name_prefix = name_prefix
        return self

    def add_generator(self, key, generator):
        """Adds a generator for the specified key under current name prefix."""
        self.record_structure[self.key_name(key)] = generator

    def key_name(self, key):
        """Concatenation of passed key to existing prefix for a full_name."""
        if self.name_prefix:
            return self.name_prefix + '.' + key
        return key

    def key_builder(self, key):
        """Builder that can be used for a nested key."""
        return Builder(self.rand,
                       joint_fields=self.joint_fields,
                       joint_is_choice=self.joint_is_choice,
                       name_prefix=self.key_name(key),
                       joint_map=self.joint_map,
                       record_structure=self.record_structure)

    def build_for_key(self, key, spec):
        """Builds or reuses a builder for an existing field key."""
        crt_name = self.key_name(key)
        if (crt_name in self.joint_fields and
                self.joint_fields[crt_name] in self.joint_map):
            generator = self.joint_map[self.joint_fields[crt_name]]
            if crt_name in self.joint_is_choice:
                generator.add_choice(crt_name)
            return generator
        generator = self.key_builder(key).build(spec)
        if crt_name not in self.joint_fields:
            return generator
        generator = JointGenerator(self.rand, generator)
        if crt_name in self.joint_is_choice:
            generator.add_choice(crt_name)
        self.joint_map[self.joint_fields[crt_name]] = generator
        return generator


class Generator:
    """Base class for the generator of a random value."""

    def __init__(self, rand: numpy.random.Generator):
        self._rand = rand

    def generate(self,
                 size: Optional[int] = None,
                 key: Optional[str] = None,
                 context: Optional[ContextBase] = None) -> Any:
        """Generates the next value.

        Args:
          size: number of generated sample.
        """
        raise NotImplementedError('Abstract generator')

    def result_type(self) -> type:
        """Returns the type of the generated value."""
        return None

    def save(self) -> Any:
        """Produces a structure that can be passed to BuildGenerater."""
        raise NotImplementedError('Abstract generator')

    def __repr__(self) -> str:
        return f'{type(self)}{self.save()}'

    def with_context(self):
        """Turns on context for generated values, based on records generated by this."""
        raise NotImplementedError(
            f'Generator of type {type(self)} does not support context')


class ValueGenerator(Generator):
    """Generator of the same value all the time."""

    def __init__(self, rand: numpy.random.Generator, value: Any):
        super().__init__(rand)
        self._value = value

    @classmethod
    def build(cls, builder: Builder, spec):
        return cls(builder.rand, spec)

    def generate(self,
                 size: Optional[int] = None,
                 key: Optional[str] = None,
                 context: Optional[ContextBase] = None) -> Any:
        if size is None:
            return self._value
        return [self._value] * size

    def result_type(self) -> type:
        return type(self._value)

    def save(self) -> Any:
        return (Names.VALUE, self._value)

    def __repr__(self) -> str:
        return f'{type(self)}({self._value})'


class NoneGenerator(ValueGenerator):
    """Generator of None - all the time."""

    def __init__(self, rand: numpy.random.Generator):
        super().__init__(rand, None)

    @classmethod
    def build(cls, builder: Builder, spec):
        return cls(builder.rand)


class IncGenerator(ValueGenerator):
    """Generates an incrementing value of numbers."""

    def __init__(self,
                 rand: numpy.random.Generator,
                 value: Any,
                 increment: int = 1,
                 limit: Optional[int] = None):
        super().__init__(rand, value)
        self._inc = 0
        self._increment = increment
        self._limit = limit

    def generate(self,
                 size: Optional[int] = None,
                 key: Optional[str] = None,
                 context: Optional[ContextBase] = None) -> Any:
        start = self._inc
        if size is None:
            self._inc += self._increment
            return self._value + self.incval(start)
        self._inc += self._increment * size
        return [
            self._value + self.incval((start + i * self._increment))
            for i in range(size)
        ]

    def incval(self, val):
        if self._limit is None:
            return val
        return val % self._limit

    def save(self) -> Any:
        if (self._increment == 1 and self._limit is None):
            return (Names.INC, self._value)
        return (Names.INC, (self._value, self._increment, self._limit))

    @classmethod
    def build(cls, builder: Builder, spec):
        if isinstance(spec, (tuple, list)):
            if len(spec) not in (1, 2, 3):
                raise ValueError(f'{cls} expects one to three args only.')
            return cls(builder.rand, spec[0], spec[1] if len(spec) > 1 else 1,
                       spec[2] if len(spec) > 2 else None)
        else:
            return cls(builder.rand, spec)


class FloatGenerator(Generator):
    """Base for default distribution-based numpy float random generators."""

    def result_type(self) -> type:
        return float


class TwoArgsRandomGenerator(FloatGenerator):
    """Base class for two argument random float distribution generator."""

    def __init__(self, rand: numpy.random.Generator, arg1: float, arg2: float,
                 name: str, generator: Callable):
        super().__init__(rand)
        self._arg1 = arg1
        self._arg2 = arg2
        self._name = name
        self._generator = generator

    @classmethod
    def build(cls, builder: Builder, spec):
        # pylint: disable=no-value-for-parameter
        return cls(builder.rand, spec[0], spec[1])

    def generate(self,
                 size: Optional[int] = None,
                 key: Optional[str] = None,
                 context: Optional[ContextBase] = None) -> float:
        return self._generator(self._rand, self._arg1, self._arg2, size)

    def save(self) -> Any:
        return (self._name, (self._arg1, self._arg2))


class UniformGenerator(TwoArgsRandomGenerator):
    """Generates a uniformly distributed float between `min_val` and `max_val`."""

    def __init__(self, rand: numpy.random.Generator, min_val: float,
                 max_val: float):
        super().__init__(
            rand, min_val, max_val, Names.UNIFORM,
            lambda rand, min_val, max_val, size: rand.uniform(
                min_val, max_val, size))


class NormalGenerator(TwoArgsRandomGenerator):
    """Generates a normal distributed float with provided mean and standard deviation."""

    def __init__(self, rand: numpy.random.Generator, mean: float,
                 std_dev: float):
        super().__init__(
            rand, mean, std_dev, Names.NORMAL,
            lambda rand, mean, std_dev, size: rand.normal(mean, std_dev, size))


class OneArgRandomGenerator(FloatGenerator):
    """Base class for one argument random float distribution generator."""

    def __init__(self, rand: numpy.random.Generator, arg: float, name: str,
                 generator: Callable):
        super().__init__(rand)
        self._arg = arg
        self._name = name
        self._generator = generator

    @classmethod
    def build(cls, builder: Builder, spec):
        # pylint: disable=no-value-for-parameter
        return cls(builder.rand, spec)

    def generate(self,
                 size: Optional[int] = None,
                 key: Optional[str] = None,
                 context: Optional[ContextBase] = None) -> float:
        return self._generator(self._rand, self._arg, size)

    def save(self) -> Any:
        return (self._name, self._arg)


class ExpGenerator(OneArgRandomGenerator):
    """Generates an exponentially distributed float with `rate`."""

    def __init__(self, rand: numpy.random.Generator, rate: float):
        super().__init__(rand, rate, Names.EXP,
                         lambda rand, rate, size: rand.exponential(rate, size))


class GeomGenerator(OneArgRandomGenerator):
    """Generates a geometrically distributed float with `p` in (0, 1]."""

    def __init__(self, rand: numpy.random.Generator, p: float):
        if p <= 0 or p > 1:
            raise ValueError(f'GeomGenerator: Invalid argument p: {p} '
                             'needs to be in (0, 1]')
        super().__init__(rand, p, Names.GEOM,
                         lambda rand, p, size: rand.geometric(p, size))


class ParetoGenerator(OneArgRandomGenerator):
    """Generates a pareto distributed float with provided shape."""

    def __init__(self, rand: numpy.random.Generator, shape: float):
        if shape <= 0:
            raise ValueError(
                f'ParetoGenerator: Invalid argument shape: {shape} '
                '(needs to be in > 0)')
        super().__init__(rand, shape, Names.PARETO,
                         lambda rand, shape, size: rand.pareto(shape, size))


class UniformIntGenerator(Generator):
    """Generates a uniform integer between `low` (inclusive) and `high` (exclusive)."""

    def __init__(self, rand: numpy.random.Generator, low: int, high: int):
        super().__init__(rand)
        self._low = low
        self._high = high

    @classmethod
    def build(cls, builder: Builder, spec):
        if not isinstance(spec, (tuple, list)) or len(spec) != 2:
            raise ValueError(f'{cls} expects two underlying arguments.')
        return cls(builder.rand, int(spec[0]), int(spec[1]))

    def generate(self,
                 size: Optional[int] = None,
                 key: Optional[str] = None,
                 context: Optional[ContextBase] = None) -> int:
        return self._rand.integers(self._low, self._high, size)

    def result_type(self) -> type:
        return int

    def save(self) -> Any:
        return (Names.INT, (self._low, self._high))


class SequenceGenerator(Generator):
    """Generates a sequence of numbers."""

    def __init__(self, rand: numpy.random.Generator,
                 start_generator: numpy.random.Generator,
                 len_generator: numpy.random.Generator):
        super().__init__(rand)
        if not _IsOptionalType(len_generator.result_type(), int):
            raise ValueError(f'{type(self)} needs an integer length generator.'
                             f'Got: {len_generator}')
        self._start_generator = start_generator
        self._len_generator = len_generator

    @classmethod
    def build(cls, builder: Builder, spec):
        if not isinstance(spec, (tuple, list)) or len(spec) != 2:
            raise ValueError(f'{cls} expects two underlying arguments')
        return cls(builder.rand, BuildGenerator(builder, spec[0]),
                   BuildGenerator(builder, spec[1]))

    def _generate_value(self, start: Any, length: Optional[int]):
        if length is None:
            return None
        return [start + i for i in range(length)]

    def generate(self,
                 size: Optional[int] = None,
                 key: Optional[str] = None,
                 context: Optional[ContextBase] = None) -> int:
        start = self._start_generator.generate(size, key, context)
        length = self._len_generator.generate(size, key, context)
        if size is None:
            return self._generate_value(start, length)
        return [self._generate_value(s, l) for (s, l) in zip(start, length)]

    def result_type(self) -> type:
        return List[self._start_generator.result_type()]

    def save(self) -> Any:
        return (Names.SEQ, (self._start_generator.save(),
                            self._len_generator.save()))


class ConvertGenerator(Generator):
    """Converts a underlying number generated by `child` to a type via a function."""

    def __init__(self, rand: numpy.random.Generator, child: Generator,
                 name: str, converter: Callable[[Any], Any],
                 in_types: List[type], out_type: type):
        super().__init__(rand)
        if (in_types is not None and
                not any(issubclass(child.result_type(), t) for t in in_types)):
            raise ValueError(
                f'{type(self)} can be built only on top generators of one '
                f'of these types: {in_types}')
        self._name = name
        self._child = child
        self._converter = converter
        self._out_type = out_type

    def generate(self,
                 size: Optional[int] = None,
                 key: Optional[str] = None,
                 context: Optional[ContextBase] = None) -> int:
        if size is None:
            return self._converter(self._child.generate(size, key, context))
        return [
            self._converter(v)
            for v in self._child.generate(size, key, context)
        ]

    def result_type(self) -> type:
        return self._out_type

    @classmethod
    def build(cls, builder: Builder, spec):
        # pylint: disable=no-value-for-parameter
        return cls(builder.rand, BuildGenerator(builder, spec))

    def save(self):
        return (self._name, self._child.save())


class ToIntGenerator(ConvertGenerator):
    """Converts a underlying number generated by `child` to int."""

    def __init__(self, rand: numpy.random.Generator, child: Generator):
        super().__init__(rand, child, Names.TOINT, int, [int, float], int)


class ToDateGenerator(ConvertGenerator):
    """Converts a underlying iso string generated by `child` to date."""

    def __init__(self, rand: numpy.random.Generator, child: Generator):
        super().__init__(rand, child, Names.TODATE, datetime.date.fromisoformat,
                         [str], datetime.date)


class ToDateTimeGenerator(ConvertGenerator):
    """Converts a underlying iso string generated by `child` to date."""

    def __init__(self, rand: numpy.random.Generator, child: Generator):
        super().__init__(rand, child, Names.TODATETIME,
                         datetime.datetime.fromisoformat, [str],
                         datetime.datetime)


class ToDecimalGenerator(ConvertGenerator):
    """Converts a underlying decimal string generated by `child` to decimal."""

    def __init__(self, rand: numpy.random.Generator, child: Generator):
        super().__init__(rand, child, Names.TODECIMAL, decimal.Decimal, [str],
                         decimal.Decimal)


class ToBoolGenerator(ConvertGenerator):
    """Converts a underlying number generated by `child` to boolean."""

    def __init__(self, rand: numpy.random.Generator, child: Generator):
        super().__init__(rand, child, Names.TOBOOL, lambda x: x != 0,
                         [int, float], bool)


class ToStringGenerator(ConvertGenerator):
    """Converts an underlying object to string."""

    def __init__(self, rand: numpy.random.Generator, child: Generator):
        super().__init__(rand, child, Names.TOSTRING, repr, None, str)


class ToBytesGenerator(ConvertGenerator):
    """Converts an underlying string to bytes - utf8 encoding."""

    def __init__(self, rand: numpy.random.Generator, child: Generator):
        super().__init__(rand, child, Names.TOBYTES,
                         lambda s: bytes(s, 'utf-8'), [str], bytes)


class ChoiceGenerator(Generator):
    """Generator that makes a choice between the underlying generators by `children`."""

    def __init__(self,
                 rand: numpy.random.Generator,
                 children: List[Generator],
                 probabilities: List[float] = None):
        super().__init__(rand)
        if not children:
            raise ValueError(
                'ChoiceGenerator cannot be built with empty choices.')
        child_types = {c.result_type() for c in children}
        has_none = type(None) in child_types
        if has_none:
            child_types.remove(type(None))
        if len(child_types) > 1:
            raise ValueError('ChoiceGenerator cannot choose from multiple '
                             f'underlying types: {child_types}')
        self._result_type = child_types.pop()
        if has_none:
            self._result_type = Optional[self._result_type]
        self._children = children
        self._probabilities = probabilities

    @classmethod
    def build(cls, builder: Builder, spec):
        if not isinstance(spec, (tuple, list)) or len(spec) not in [1, 2]:
            raise ValueError(f'{cls} expects list of one or two arguments')
        if not isinstance(spec[0], (tuple, list)):
            raise ValueError(f'{cls} expects first argument to be a list')
        choices = [BuildGenerator(builder, s) for s in spec[0]]
        probabilities = None
        if len(spec) > 1:
            probabilities = spec[1]
        return cls(builder.rand, choices, probabilities)

    def generate(self,
                 size: Optional[int] = None,
                 key: Optional[str] = None,
                 context: Optional[ContextBase] = None) -> Any:
        choice = self._rand.choice(len(self._children),
                                   size=size,
                                   p=self._probabilities)
        if size is None:
            return self._children[choice].generate(None, key, context)
        return [self._children[c].generate(None, key, context) for c in choice]

    def result_type(self) -> type:
        return self._result_type

    def save(self) -> Any:
        return (Names.CHOICE, ([child.save() for child in self._children],
                               self._probabilities))


ALPHABET = ' abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
ALPHABET_FREQ = [
    # <sp> abcdefg
    0.12512899999999982,  # so the sum is 1
    0.054814,
    0.010027,
    0.018717,
    0.028744,
    0.086900,
    0.014706,
    0.013369,
    # hijklmno
    0.040776,
    0.046792,
    0.001003,
    0.005147,
    0.026738,
    0.016043,
    0.044787,
    0.050134,
    # pqrstuvw
    0.012701,
    0.000635,
    0.040107,
    0.042113,
    0.060830,
    0.018717,
    0.006551,
    0.016043,
    # xyzABCDE
    0.001003,
    0.013369,
    0.000468,
    0.010963,
    0.002005,
    0.003743,
    0.005749,
    0.017380,
    # FGHIJKLM
    0.002941,
    0.002674,
    0.008155,
    0.009358,
    0.000201,
    0.001029,
    0.005348,
    0.003209,
    # NOPQRSTU
    0.008957,
    0.010027,
    0.002540,
    0.000127,
    0.008021,
    0.008423,
    0.012166,
    0.003743,
    # VWXYZ012
    0.001310,
    0.003209,
    0.000201,
    0.002674,
    0.000094,
    0.006939,
    0.006939,
    0.006939,
    # 3456789
    0.006939,
    0.006939,
    0.006939,
    0.006939,
    0.006939,
    0.006939,
    0.006939
]


class StringGenerator(Generator):
    """Generates a string of generated length, from an alphabet w/ some frequencies."""

    def __init__(self,
                 rand: numpy.random.Generator,
                 len_generator: Generator,
                 alphabet: Optional[str] = None,
                 probabilities: Optional[List[float]] = None):
        super().__init__(rand)
        if not _IsOptionalType(len_generator.result_type(), int):
            raise ValueError(
                'StringGenerator needs an integer length generator. '
                f'Got: {len_generator} of: {len_generator.result_type()}')
        self._len_generator = len_generator
        if probabilities is not None and alphabet is None:
            raise ValueError(
                'StringGenerator needs `alphabet` when `probabilities` '
                'is specified')
        if probabilities is not None and len(alphabet) != len(probabilities):
            raise ValueError(
                'StringGenerator needs `probabilities` to be the same size as '
                f'the size of the alphabet: {len(probabilities)} vs {len(alphabet)}'
            )
        self._using_defaults = False
        if alphabet is None:
            self._alphabet_list = ALPHABET
            probabilities = ALPHABET_FREQ
            self._using_defaults = True
        else:
            self._alphabet_list = alphabet
        if probabilities:
            probabilities[0] += (1.0 - sum(probabilities))
        self._alphabet = list(self._alphabet_list)
        self._probabilities = probabilities

    @classmethod
    def build(cls, builder: Builder, spec):
        alphabet = None
        probabilities = None
        if isinstance(spec, (tuple, list)):
            len_spec = spec[0]
            if len(spec) > 1:
                alphabet = spec[1]
            if len(spec) > 2:
                probabilities = spec[1]
        else:
            len_spec = spec
        return cls(builder.rand, BuildGenerator(builder, len_spec), alphabet,
                   probabilities)

    def generate(self,
                 size: Optional[int] = None,
                 key: Optional[str] = None,
                 context: Optional[ContextBase] = None) -> Dict[Any, Any]:
        str_len = self._len_generator.generate(None, key, context)
        if size is None:
            return ''.join(
                self._rand.choice(self._alphabet,
                                  str_len,
                                  p=self._probabilities))
        else:
            letters = self._rand.choice(self._alphabet, [size, str_len],
                                        p=self._probabilities)
            return [''.join(s) for s in letters]

    def result_type(self) -> type:
        return str

    def save(self) -> Any:
        if self._using_defaults:
            return ('str', (self._len_generator.save(),))
        return (Names.STR, (self._len_generator.save(), self._alphabet_list,
                            self._probabilities))


class ArrayGenerator(Generator):
    """Generator for list of values of value_generator, with a length."""

    def __init__(self, rand: numpy.random.Generator, value_generator: Generator,
                 len_generator: Generator):
        """Initializes an array generator.

        Args:
          value_generator: generates the underlying values in the array.
          len_generator: generates the length of the output array.
             On None generated by this, our output is None.
        """
        super().__init__(rand)
        if not _IsOptionalType(len_generator.result_type(), int):
            raise ValueError(
                'ArrayGenerator needs an integer length generator. '
                f'Got: {len_generator.result_type()}')
        self._value_generator = value_generator
        self._len_generator = len_generator

    @classmethod
    def build(cls, builder: Builder, spec):
        if not isinstance(spec, (tuple, list)) or len(spec) != 2:
            raise ValueError(f'{cls} expects two underlying arguments')
        return cls(builder.rand, BuildGenerator(builder, spec[0]),
                   BuildGenerator(builder, spec[1]))

    def generate(self,
                 size: Optional[int] = None,
                 key: Optional[str] = None,
                 context: Optional[ContextBase] = None) -> List[Any]:
        array_len = self._len_generator.generate(size, key, context)
        if array_len is None:
            return None
        if size is None:
            return self._value_generator.generate(array_len, key, context)
        else:
            return [
                self._value_generator.generate(sz, key, context)
                for sz in array_len
            ]

    def _list_type(self) -> type:
        """Underlying list type."""
        return List[self._value_generator.result_type()]

    def result_type(self) -> type:
        if _IsOptional(self._len_generator.result_type(),):
            return Optional[self._list_type()]
        return self._list_type()

    def save(self) -> Any:
        return (Names.ARRAY, (self._value_generator.save(),
                              self._len_generator.save()))


class PerElementArrayGenerator(ArrayGenerator):
    """Similar with ArrayGenerator, but the generation is done for every element.

    This is more expensive, but allows a choice to be made at each element, as opposed
    to one choice per each generated array. On the downside, when generating an
    array of arrays with this, by specifying a size parameter, that size is fixed."""

    def generate(self,
                 size: Optional[int] = None,
                 key: Optional[str] = None,
                 context: Optional[ContextBase] = None) -> List[Any]:
        array_len = self._len_generator.generate(None, key, context)
        if array_len is None:
            return None
        return [
            self._value_generator.generate(size, key, context)
            for _ in range(array_len)
        ]

    def save(self) -> Any:
        return (Names.ELEM_ARRAY, (self._value_generator.save(),
                                   self._len_generator.save()))


class MakeSetGenerator(Generator):
    """Converts the underlying generated list to a set."""

    def __init__(self, rand: numpy.random.Generator, list_generator: Generator):
        """Makes a set out of a generated list."""
        super().__init__(rand)
        if _GetStructuredTypeName(list_generator.result_type()) != 'list':
            raise ValueError(
                'MakeSetGenerator expecting an underlying generator of '
                f'a list. Got: {list_generator.result_type()}')
        self._list_generator = list_generator

    @classmethod
    def build(cls, builder: Builder, spec):
        return cls(builder.rand, BuildGenerator(builder, spec))

    def generate(self,
                 size: Optional[int] = None,
                 key: Optional[str] = None,
                 context: Optional[ContextBase] = None) -> Set[Any]:
        result = self._list_generator.generate(size, key, context)
        if result is None:
            return result
        elif size is None:
            return set(result)
        else:
            return [set(sub_list) for sub_list in result]

    def result_type(self) -> type:
        return Set[self._list_generator.result_type().__args__[0]]

    def save(self) -> Any:
        return (Names.SET, self._list_generator.save())


class DictGenerator(Generator):
    """Generates random dictionaries with len, keys and values as chosen."""

    def __init__(self, rand: numpy.random.Generator, key_generator: Generator,
                 value_generator: Generator, len_generator: Generator):
        """Generates a dictionary of length determined by len_generator, """
        super().__init__(rand)
        if not _IsOptionalType(len_generator.result_type(), int):
            raise ValueError('DictGenerator needs an integer length generator. '
                             f'Got: {len_generator}')
        self._key_generator = key_generator
        self._value_generator = value_generator
        self._len_generator = len_generator

    @classmethod
    def build(cls, builder: Builder, spec):
        if not isinstance(spec, (tuple, list)) or len(spec) != 3:
            raise ValueError(f'{cls} expects two underlying arguments')
        return cls(builder.rand, BuildGenerator(builder, spec[0]),
                   BuildGenerator(builder, spec[1]),
                   BuildGenerator(builder, spec[2]))

    def generate(self,
                 size: Optional[int] = None,
                 key: Optional[str] = None,
                 context: Optional[ContextBase] = None) -> Dict[Any, Any]:
        if size is None:
            return self._generate_dict(key, context)
        return [self._generate_dict(key, context) for _ in range(size)]

    def _generate_dict(self, key, context) -> Dict[Any, Any]:
        dict_len = self._len_generator.generate(None, key, context)
        if dict_len is None:
            return None
        return dict(
            zip(self._key_generator.generate(dict_len, key, context),
                self._value_generator.generate(dict_len, key, context)))

    def _dict_type(self) -> type:
        """Underlying returned dictionary type."""
        return Dict[self._key_generator.result_type(),
                    self._value_generator.result_type()]

    def result_type(self) -> type:
        """Top level returned type, maybe optional."""
        if _IsOptional(self._len_generator.result_type()):
            return Optional[self._dict_type()]
        return self._dict_type()

    def save(self) -> Any:
        return (Names.DICT, (self._key_generator.save(),
                             self._value_generator.save(),
                             self._len_generator.save()))


class CombineGenerator(Generator):
    """Combines two values from two underlying generators."""

    def __init__(self, rand: numpy.random.Generator, first: Generator,
                 second: Generator, name: str,
                 combiner: Callable[[Any, Any], Any], first_types: List[type],
                 second_types: List[type], out_type: type):
        super().__init__(rand)
        if not any(issubclass(first.result_type(), t) for t in first_types):
            raise ValueError(
                f'{type(self)} can be built with first argument generating '
                f'{first_types}. Found: {first.result_type()}')
        if not any(issubclass(second.result_type(), t) for t in second_types):
            raise ValueError(
                f'{type(self)} can be built with second argument generating '
                f'{second_types}. Found: {second.result_type()}')
        self._first = first
        self._second = second
        self._combiner = combiner
        self._name = name
        self._out_type = out_type

    def generate(self,
                 size: Optional[int] = None,
                 key: Optional[str] = None,
                 context: Optional[ContextBase] = None) -> Dict[Any, Any]:
        if size is None:
            return self._combiner(self._first.generate(None, key, context),
                                  self._second.generate(None, key, context))
        return [
            self._combiner(f, s)
            for (f, s) in zip(self._first.generate(size, key, context),
                              self._second.generate(size, key, context))
        ]

    def result_type(self) -> type:
        return self._out_type

    def save(self) -> Any:
        return (self._name, (self._first.save(), self._second.save()))

    @classmethod
    def build(cls, builder: Builder, spec):
        if not isinstance(spec, (tuple, list)) or len(spec) != 2:
            raise ValueError(f'{cls} expects two underlying arguments')
        # pylint: disable=no-value-for-parameter
        return cls(builder.rand, BuildGenerator(builder, spec[0]),
                   BuildGenerator(builder, spec[1]))


class DateGenerator(CombineGenerator):
    """Generates dates with a start date, and a delta."""

    def __init__(self, rand: numpy.random.Generator, start_date: Generator,
                 delta_days: Generator):
        super().__init__(
            rand, start_date, delta_days, Names.DATE,
            lambda start, delta: start + datetime.timedelta(days=int(delta)),
            [datetime.date], [int], datetime.date)


class DatetimeGenerator(CombineGenerator):
    """Generates datetimes with a start timestamp, and a delta in seconds."""

    def __init__(self, rand: numpy.random.Generator, start_datetime: Generator,
                 delta_seconds: Generator):
        super().__init__(
            rand, start_datetime, delta_seconds, Names.DATETIME,
            lambda start, delta: start + datetime.timedelta(seconds=int(delta)),
            [datetime.datetime], [int], datetime.datetime)


class StrOpGenerator(Generator):
    """Combines strings generated by the children."""

    def __init__(self, rand: numpy.random.Generator, children: List[Generator],
                 op: str, name: str, in_types: List[type],
                 combiner: Callable[[str, List[any]], str]):
        super().__init__(rand)
        if (in_types is not None and any(
                not any(issubclass(child.result_type(), t)
                        for t in in_types)
                for child in children)):
            raise ValueError(
                f'{type(self)} expecting {in_types} input types. '
                f'Got {[child.result_type() for child in children]}')
        if not isinstance(op, str):
            raise ValueError(
                f'{type(self)} expecting string operand. Got `{op}`')
        self._children = children
        self._op = op
        self._name = name
        self._combiner = combiner

    def result_type(self) -> type:
        return str

    def save(self) -> Any:
        return (self._name, ([child.save() for child in self._children],
                             self._op))

    @classmethod
    def build(cls, builder: Builder, spec):
        if not isinstance(spec, (tuple, list)) or len(spec) != 2:
            raise ValueError(f'{cls} expects two underlying arguments')
        # pylint: disable=no-value-for-parameter
        return cls(builder.rand, [BuildGenerator(builder, s) for s in spec[0]],
                   spec[1])

    def generate(self,
                 size: Optional[int] = None,
                 key: Optional[str] = None,
                 context: Optional[ContextBase] = None) -> str:
        if size is None:
            return self._combiner(self._op, [
                child.generate(None, key, context) for child in self._children
            ])
        return [
            self._combiner(self._op, elems) for elems in zip(*[
                child.generate(size, key, context) for child in self._children
            ])
        ]


class ConcatGenerator(StrOpGenerator):

    def __init__(self, rand: numpy.random.Generator, children: List[Generator],
                 concat: str):
        super().__init__(rand, children, concat, Names.CONCAT, [str],
                         lambda op, elem: op.join(elem))


class FormatGenerator(StrOpGenerator):

    def __init__(self, rand: numpy.random.Generator, children: List[Generator],
                 formatter: str):
        super().__init__(rand, children, formatter, Names.FORMAT, None,
                         lambda op, elem: op.format(*elem))


def _GenKey(key: str, base: Optional[str]):
    if base:
        return base + '.' + key
    return key


class RecordGenerator(Generator):
    """Base class for generating records-like objects: dicts, classes, dataframes."""

    def __init__(self, rand: numpy.random.Generator,
                 generators: Dict[str, Generator], name: str):
        super().__init__(rand)
        self._generators = generators
        self._name = name
        self._with_context = False

    def with_context(self):
        """Turns on context for generated values, based on records generated by this."""
        self._with_context = True
        return self

    def nested(self, key: str):
        """Access the internal generator for provided key."""
        if key in self._generators:
            return self._generators[key]
        return None

    @classmethod
    def build(cls, builder: Builder, spec):
        if not isinstance(spec, dict):
            raise ValueError(
                f'{cls} expecting a dict specification. Got: {type(spec)}')
        generators = {}
        for k, v in spec.items():
            if not isinstance(k, str):
                raise ValueError(f'{cls} expecting string keys. Got `{k}`')
            generator = builder.build_for_key(k, v)
            builder.add_generator(k, generator)
            generators[k] = generator
        # pylint: disable=no-value-for-parameter
        return cls(builder.rand, generators)

    def save(self) -> Any:
        return (self._name, {k: v.save() for k, v in self._generators.items()})

    def generate(self,
                 size: Optional[int] = None,
                 key: Optional[str] = None,
                 context: Optional[ContextBase] = None) -> Any:
        if self._with_context:
            context = FlatContext()
        if size is None:
            return self.generate_record(key, context)
        return [self.generate_record(key, context) for _ in range(size)]

    def generate_record(self, key: Optional[str], context: ContextBase) -> Any:
        record = {}
        if self._with_context:
            context.append(record)
        elif key is not None and context:
            context.set(key, record)
        for k, g in self._generators.items():
            record[k] = g.generate(None, _GenKey(k, key), context)
        return record

    def result_type(self) -> type:
        return Dict[str, Any]


class NestedGenerator(RecordGenerator):
    """Generates "nested" dicts - for specific set of keys, specific values."""

    def __init__(self, rand: numpy.random.Generator,
                 generators: Dict[str, Generator]):
        super().__init__(rand, generators, Names.NESTED)


class DataframeGenerator(RecordGenerator):
    """Similar w/ NestedGenerator, but generates keys to sequences."""

    def __init__(self, rand: numpy.random.Generator,
                 generators: Dict[str, Generator]):
        super().__init__(rand, generators, Names.DATAFRAME)

    def generate(self,
                 size: Optional[int] = None,
                 key: Optional[str] = None,
                 context: Optional[ContextBase] = None) -> Dict[Any, Any]:
        record = {}
        if self._with_context:
            context = ColumnarContext(record)
        for k, g in self._generators.items():
            # this is mainly to provide storage during generation
            # for nested subkeys.
            record[k] = []
            record[k] = g.generate(1 if size is None else size, _GenKey(k, key),
                                   context)
        return record


class DataclassGenerator(RecordGenerator):
    """Similar w/ NestedGenerator, but generates classes and sets members."""

    def __init__(
            self,
            rand: numpy.random.Generator,
            generators: Dict[str, Generator],
            # Specify the class:
            build_class: Optional[type] = None,
            # Or the module and class
            module_name: Optional[str] = None,
            class_name: Optional[str] = None):
        super().__init__(rand, generators, Names.DATACLASS)
        self.set_dataclass(build_class, module_name, class_name)
        self._cls = None

    def set_dataclass(self,
                      build_class: Optional[type] = None,
                      module_name: Optional[str] = None,
                      class_name: Optional[str] = None):
        if build_class:
            self._cls = build_class
        elif module_name and class_name:
            self._cls = getattr(sys.modules[module_name], class_name)

    @classmethod
    def build(cls, builder: Builder, spec):
        if not isinstance(spec, (tuple, list)) or len(spec) not in [2, 3]:
            raise ValueError(f'{cls} expects two or three underlying '
                             f'arguments. Got {spec}')
        instance = super().build(builder, spec[0])
        if len(spec) == 2:
            if not isinstance(spec[1], type):
                raise ValueError(
                    f'{cls} expects a class when providing two arguments')
            instance.set_dataclass(build_class=spec[1])
        else:
            instance.set_dataclass(module_name=spec[1], class_name=spec[2])
        return instance

    def generate_record(self, key: Optional[str], context: ContextBase) -> Any:
        record = super().generate_record(key, context)
        return self._cls(**record)

    def result_type(self) -> type:
        return self._cls

    def save(self) -> Any:
        return (Names.DATACLASS,
                ({k: v.save() for k, v in self._generators.items()},
                 self._cls.__module__, self._cls.__qualname__))


class FakerGenerator(Generator):
    """Generates values using the faker library."""

    def __init__(self,
                 rand: numpy.random.Generator,
                 fake_generator: str,
                 locale: Optional[Union[str, List[str]]] = None):
        super().__init__(rand)
        if faker is None:
            raise NotImplementedError('faker module not available')
        self._faker = faker.Faker(locale)
        self._fake_generator = fake_generator
        self._locale = locale
        self._generator = getattr(self._faker, fake_generator)
        if not callable(self._generator):
            raise ValueError(
                f'The faker generator `{fake_generator}` is not callable.')
        self._result_type = type(self._generator())

    @classmethod
    def build(cls, builder: Builder, spec):
        if isinstance(spec, (tuple, list)):
            if len(spec) != 2:
                raise ValueError(f'{cls} expects two underlying arguments for '
                                 'parametrize creation')
            return cls(builder.rand, spec[0], spec[1])
        return cls(builder.rand, spec)

    def result_type(self) -> type:
        return self._result_type

    def save(self) -> Any:
        return (Names.FAKER, (self._fake_generator, self._locale))

    def generate(self,
                 size: Optional[int] = None,
                 key: Optional[str] = None,
                 context: Optional[ContextBase] = None) -> Any:
        if size is None:
            return self._generator()
        return [self._generator() for _ in range(size)]


class JointGenerator(Generator):
    """Generates same values across various calls, from different destination."""

    class IdData:
        """Helper that holds information about per key index."""

        def __init__(self, index, is_choice):
            self.index = index
            self.is_choice = is_choice

        def next_index(self):
            self.index += 1
            return self.index - 1

    def __init__(self,
                 rand: numpy.random.Generator,
                 child: Generator,
                 choice_keys: Optional[Set[str]] = None):
        super().__init__(rand)
        self._child = child
        self._ids = {}
        self._id_data = []  # List[IdData]
        self._values = []
        if isinstance(choice_keys, list):
            self._choice_keys = set(choice_keys)
        elif isinstance(choice_keys, set):
            self._choice_keys = choice_keys
        elif choice_keys is None:
            self._choice_keys = set()
        else:
            raise ValueError(
                f'Invalid `choice_keys` argument provided: {choice_keys}')

    def add_choice(self, key):
        self._choice_keys.add(key)

    def generate(self,
                 size: Optional[int] = None,
                 key: Optional[str] = None,
                 context: Optional[ContextBase] = None) -> Any:
        if key not in self._ids:
            self._ids[key] = len(self._id_data)
            id_data = JointGenerator.IdData(0, key in self._choice_keys)
            self._id_data.append(id_data)
        else:
            id_data = self._id_data[self._ids[key]]
        next_id = id_data.next_index()
        while len(self._values) <= next_id:
            self._values.append(self._child.generate(size, key, context))
        if id_data.is_choice:
            return self._values[self._rand.choice(len(self._values))]
        return self._values[next_id]

    def result_type(self) -> type:
        return self._child.result_type()

    @classmethod
    def build(cls, builder: Builder, spec):
        if not isinstance(spec, (tuple, list)) or len(spec) != 2:
            raise ValueError(f'Expecting a tuple argument to {cls}. Got {spec}')
        return cls(builder.rand, BuildGenerator(builder, spec[0]), spec[1])

    def save(self) -> Any:
        # Save choices as a sorted list - easier to test and serialize.
        choices = list(self._choice_keys)
        choices.sort()
        return (Names.JOINT, (self._child.save(), choices))


class FieldGenerator(Builder):
    """Generates a field from the currently generated record."""

    def __init__(self, rand: numpy.random.Generator, field_name: str,
                 default: Generator, delta: Generator, result_type: type):
        super().__init__(rand)
        if (default.result_type() != type(None) and
                default.result_type() != result_type):
            raise ValueError(
                f'{type(self)} has default value of a different type than the '
                f'one discovered for field {field_name}: '
                f'`{default.result_type()}` vs. `{result_type}`')
        if delta.result_type() != int:
            raise ValueError(f'{type(self)} expects int from delta generator. '
                             f'found: {delta.result_type()}')

        self._field_name = field_name
        self._field = ContextField(field_name)
        self._default = default
        self._delta = delta
        self._result_type = result_type
        self._last_context = None

    def generate(self,
                 size: Optional[int] = None,
                 key: Optional[str] = None,
                 context: Optional[ContextBase] = None) -> Any:
        if not context:
            return self._default.generate(size, key, context)
        if context != self._last_context:
            self._last_context = context
            self._index = 0
        if not size:
            return self._generate_value(self, context)
        elif key is not None and context and key == self._field_name:
            # Self reference - generate and update the context value:
            record = []
            for _ in range(size):
                value = self._generate_value(self, context)
                context.set(key, value)
                record.append(value)
            return record
        else:
            return [self._generate_value(self, context) for _ in range(size)]

    def _generate_value(self, key: Optional[str], context: ContextBase):
        delta = self._delta.generate(None, key, context)
        value = None
        if delta is not None and delta >= 0:
            value = context.get(self._index - delta, self._field)

        self._index += 1
        if value is None:
            return self._default.generate(None, key, context)
        return value

    @classmethod
    def build(cls, builder: Builder, spec):
        default = ValueGenerator(builder.rand, None)
        delta = ValueGenerator(builder.rand, 0)
        if not isinstance(spec, (tuple, list)):
            field_name = spec
        elif len(spec) not in (1, 2, 3):
            raise ValueError(
                f'{cls} expects one to three arguments got `{spec}`.')
        else:
            field_name = spec[0]
            if len(spec) > 1:
                default = spec[1]
            if len(spec) > 2:
                delta = spec[2]
        default_gen = BuildGenerator(builder, default)
        if field_name not in builder.record_structure:
            if default_gen is None:
                raise ValueError(
                    f'Cannot reference field `{field_name}` under existing generators. '
                    'Make sure that this generator is created after the one '
                    'for the referenced field. This may mean to rearrange the '
                    'fields in the dataclass, or to shuffle the order of keys in '
                    'the dictionary passed to the top record generator.')
            result_type = default_gen.result_type()
        else:
            result_type = builder.record_structure[field_name].result_type()
            if (default_gen.result_type() != type(None) and
                    result_type != default_gen.result_type()):
                raise ValueError(
                    f'The field `{field_name}` under existing generator '
                    'produces a field of a different type than the '
                    f'default generator: `{result_type}` vs. '
                    f'`{default_gen.result_type()}`')

        return cls(builder.rand, field_name, default_gen,
                   BuildGenerator(builder, delta), result_type)

    def save(self) -> Any:
        return (Names.FIELD, (self._field_name, self._default.save(),
                              self._delta.save()))

    def result_type(self) -> type:
        return self._result_type


class ApplyGenerator(Generator):
    """Combines input using actual code from the combiner function"""

    def __init__(self,
                 rand: numpy.random.Generator,
                 combiner: Callable,
                 children: List[Generator],
                 result_type: Optional[type] = None):
        super().__init__(rand)
        self._combiner = combiner
        self._children = children
        if result_type is None:
            # Try to find the result type from function annotation.
            if 'return' not in combiner.__annotations__:
                raise ValueError(
                    f'{type(self)} needs a result type, either annotate '
                    f'your function or pass a `result_type` parameter')
            self._result_type = combiner.__annotations__['return']
        else:
            self._result_type = result_type

    def result_type(self) -> type:
        return str

    def save(self) -> Any:
        return (Names.APPLY, (encoder_module.dumps(self._combiner),
                              [child.save() for child in self._children],
                              encoder_module.dumps(self._result_type)))

    @classmethod
    def build(cls, builder: Builder, spec):
        if not isinstance(spec, (tuple, list)) or len(spec) not in (2, 3):
            raise ValueError(f'{cls} expects two underlying arguments')
        if isinstance(spec[0], bytes):
            combiner = encoder_module.loads(spec[0])
        else:
            combiner = spec[0]
        result_type = None
        if len(spec) > 2:
            if isinstance(spec[2], bytes):
                result_type = encoder_module.loads(spec[2])
            else:
                result_type = spec[2]
        # pylint: disable=no-value-for-parameter
        return cls(builder.rand, combiner,
                   [BuildGenerator(builder, s) for s in spec[1]], result_type)

    def generate(self,
                 size: Optional[int] = None,
                 key: Optional[str] = None,
                 context: Optional[ContextBase] = None) -> str:
        if size is None:
            return self._combiner(*[
                child.generate(None, key, context) for child in self._children
            ])
        return [
            self._combiner(*elems) for elems in zip(*[
                child.generate(size, key, context) for child in self._children
            ])
        ]


_CLASS_MAP = {
    Names.VALUE: ValueGenerator,
    Names.INC: IncGenerator,
    Names.SEQ: SequenceGenerator,
    Names.UNIFORM: UniformGenerator,
    Names.NORMAL: NormalGenerator,
    Names.EXP: ExpGenerator,
    Names.GEOM: GeomGenerator,
    Names.PARETO: ParetoGenerator,
    Names.INT: UniformIntGenerator,
    Names.TOINT: ToIntGenerator,
    Names.TODATE: ToDateGenerator,
    Names.TODATETIME: ToDateTimeGenerator,
    Names.TODECIMAL: ToDecimalGenerator,
    Names.TOBOOL: ToBoolGenerator,
    Names.TOBYTES: ToBytesGenerator,
    Names.TOSTRING: ToStringGenerator,
    Names.CHOICE: ChoiceGenerator,
    Names.STR: StringGenerator,
    Names.ARRAY: ArrayGenerator,
    Names.ELEM_ARRAY: PerElementArrayGenerator,
    Names.SET: MakeSetGenerator,
    Names.DICT: DictGenerator,
    Names.NESTED: NestedGenerator,
    Names.DATACLASS: DataclassGenerator,
    Names.DATAFRAME: DataframeGenerator,
    Names.DATE: DateGenerator,
    Names.DATETIME: DatetimeGenerator,
    Names.CONCAT: ConcatGenerator,
    Names.FORMAT: FormatGenerator,
    Names.FAKER: FakerGenerator,
    Names.JOINT: JointGenerator,
    Names.FIELD: FieldGenerator,
    Names.APPLY: ApplyGenerator,
}

GeneratorSpec = Union[Tuple[str, Any], Generator, Any]


def BuildGenerator(builder: Union[Builder, numpy.random.Generator],
                   spec: GeneratorSpec) -> Generator:
    """Builds a generator based on a tuple that includes the name and arguments.

    Args:
      spec: specification of the generator, normally a tuple
      Specific Values:
          ('val', <value>)
              - a deterministic generator that always returns value.
              e.g. ('val', None) - return always None.
          ('inc', <start_value>) or
          ('inc', (<start_value>[, <increment> [, <limit>]]))
              - a deterministic generator that generates incremental numbers
              from the `start_value` integer. The `increment` can be changed,
              and a `limit` can also be changed, to reset the counter to `start_value`
              when the limit is reached (exclusive).
              ('inc', 3) - generates in order: 3, 4, 5, ..
              ('inc', (10, 10, 40)) - generates: 10, 20, 30, 10, 20, 30 ...
          ('seq', (<start>, <length>)) - generates sequences, as arrays, of
              provided start generator, of length as specified by length.
              None in length result in None outputs.
              E.g. ('seq', ('int', (2, 5), ('int', (1, 4)))) generates
                sequences of length 1 to 4 (exclusive), starting with values
                from 2 to 5. Something like [3, 4, 5], [4], [2, 3] may be sequences
                generated as above.
      Random Number Generators:
          ('uniform', (<min_val>, <max_val>))
              - a uniformly distributed float value generator.
              e.g. ('uniform', (1.2, 9.8)) - generates floats uniformly
              between 1.2 and 9.8
          ('normal', (<mean>, <std_dev>))
              - a normally distributed float with provided mean and standard deviation.
          ('pareto', <shape>)
              - a Pareto distributed float with provided shape.
          ('int', (<min_val>, <max_val>))
              - a uniformly distributed int value generator between
              `min_val` (inclusive) and `max_val` (exclusive).
              e.g. ('int', (1, 10)) - generates ints uniformly between 1 and 9.
          ('exp', <rate(lambda)>)
              - an exponentially distributed float with provided rate
              (ie. mean 1/lambda)
          ('geom', <p>)
              - a geometrically distributed float that takes only discrete
              values.
      Converters:
          ('toint', <value>)
              - converts the value returned by generator <value> to an integer.
          ('todate', <value>)
              - converts the ISO date string returned by generator <value> to a
              `datetime.date`.
          ('todatetime', <value>)
              - converts the ISO datetime string returned by generator <value> to a
              `datetime.datetime`.
          ('todecimal', <value>)
              - converts the decimal string returned by generator <value> to a
              `decimal.Decimal`.
          ('tobool', <value>)
              - converts the input numeric value to boolean by comparing to 0.
          ('tostring', <value>)
              - converts the input value of any type to a string through `repr`.
      Choice based generators:
          ('choice', (<choices> [, <prob>]))
              - the value is chosen between the generators from `choices`
              (which need to return the same type of value), with optional
              `prob` probabilities.
              If `prob` is not specified, we choose uniformly.
              e.g. ('choice', ([('val', 'foo'), ('val', 'bar'), ('val', 'baz')],
                               [0.8, 0.15, 0.05])) will geometrically choose
                               between 'foo' (80%), 'bar' (15%) and 'baz' (5%).
                   ('choice', ([('val', None), ('int', (1, 20))])) chooses
                      equally between a uniform number between 1 and 20 or None.
          ('str', <length> | (<length> [, <alphabet>[, <probabilities>]]))
               - generates a string of a length determined by the <length> generator.
               Alternately one can specify an alphabet and associated letter
               probabilities. For example:
                 ('str', ('int', (5, 10))) - generates a string with uniformly
                     distributed lengths between 5 and 10.
                 ('str', (('int', (5, 10)), 'abc', [0.5, 0.3, 0.2]))
                     - generates string with letters 'a' 'b' or 'c' chosen
                    with probabilities .5, .3 and .2 respectively.
      Combiner generators:
          ('date', (<start_day>, <delta_days>))
               - generates a day from the `start_day`, adding `delta_days`
               e.g: ('date', ('todate', '2021-09-10'), ('int', (10, 20)))
                 generates dates 10 to 20 days in the future, starting from
                 2021/09/10.
          ('datetime', (<start_datetime>, <delta_seconds>))
               - same as above, but with a timestamp and a delta in seconds.
          ('concat', ([<children>], <concat_str>))
               - concatenates the strings generated by the `children` string
                 generator, with `concat_str` joiner:
                 ('concat', ([('str', 4), ('int', (0, 100)), ('str', 3)], '-'))
                 would generate string like 'foof-23-bar'
          ('format', ([<children>], <format_str>))
               - formats the provided strings generated by `children` using
                 the `format_str`:
                 ('format', ([('str', 4), ('int', (0, 100)), ('str', 3)],
                             'from: {} via {} to {}'))
                 would generate string like 'from: foof via 23 to bar'

      Structure generators:
          ('array', (<value>, <length>))
               - generates arrays of elements returned by <value> generator, of
               a length determined by `length` generator.
               e.g. ('array', ('exp', 3), ('int', (50, 100)))
                  - we generate arrays between 50 and 100 elements, of exponentially
                  chosen floats w/ rate 3.
          ('elem_array', (<value>, <length>))
               - same as array, but this is different how we treat the internal
               generation when size is specified. Specifically:
               `array.generate(size)` would generate `size` random lengths and
               generate `size` number of arrays of random size and elements,
               while `elem_array.generate(size)` will generate a random number
               of arrays of length `size`. E.g.
               array.generate(3) generates 3 arrays of random sizes
               elem_array.generate(3) generates a random number of arrays of size 3
          ('set', <elements>)
               - builds a set from the elements generated by the underlying
               `elements` generator.
          ('dict', (<key>, <value>, <size>))
               - generates a random dictionary with keys and values determined
               by <key> and <value> generators, with number of keys determined
               by the <size> generator,
               ('dict', ('uniform', 1, 10), ('str', ('int', 5, 10)), 3)
                - builds dictionaries with 3 keys chosen uniformly between 1 and 10,
                mapping to string of sizes between 5 and 10.7
          ('nested', {<names>: <values>})
               - builds dictionaries with keys as in <names> and associated
               values generated by <values> generators>. E.g.
               ('nested', {'foo': (str, 5), 'bar': (int, (5, 100))})
                builds dictionaries with keys 'foo' and 'bar', with values for 'foo'
                being string of length 5, and for 'bar' integers between 5 and 100.
          ('dataframe', {<names>: <values>})
                - same as 'nested', but the generated dict is usable as a dataframe
                initializer, when size is specified.
                E.g. for ('nested', {'foo': (str, 5), 'bar': (int, (5, 100))}),
                when generate(3) is called, you may get something like:
                [{'foo': 'abcde', 'bar': 5},
                 {'foo': 'fghij', 'bar': 7},
                 {'foo': 'xyzxy', 'bar': 6}]
                but when using 'dataframe' instead of nested, you get:
                {'foo': ['abcde', 'fghij', 'xyzxy'], 'bar': [5, 6, 7]}
          ('dataclass', ({<names>: <values>}, [cls | module_name, class_name])
                - same as 'nested', but it generates a class of the provided type.
                The names in the dict must be members of the class.
                E.g
                @dataclass
                class MyData:
                   foo: str
                   bar: int
                ('dataclass', ({'foo': (str, 3), 'bar': (int, (5, 10))}, MyData))
                generates MyData instances, with members foo and bar set according
                to the corresponding generators.

        Faker:
          ('faker', ('faker_name'[, 'locale']))
                uses the Faker library to generate synthetic data according to
                the specification.
                E.g. ('faker', 'name') generates US names,
                     ('faker', ('name', 'fr_FR')) generates French Names,
                     ('faker', 'latitude') generates latitudes etc.
                For details: https://pypi.org/project/Faker/
        Joint:
          ('joint', (<child>, <choice_keys>))
                - subsequent calls to generate, with different keys, generate
                the same values, or choice of the same values (if key is in the
                choice_keys set). This is useful for generating values from the
                same set for joining tables on a particular column.

        Field:
           ('field', <field_name>) or
           ('field', (<field_name>, <default_value>[, <delta>]))
                 - returns the value of an already defined field from the current
                 record. This needs to be specified under a NESTED, DATACLASS or
                 DATAFRAME generator, tha generator with_context() called.
                 The referenced field needs to be generated before the place
                 where this generator is used.
                 `field_name can be composed as <field>.<sub_field> to reference
                 nested elements.
                 `delta` can specify a previous record in the current generation
                 round to reference the field from (0 - current record, 1 - previous
                 record etc)
                 `default_value` can be specified the value to return when the
                 provided field is None or non existent (e.g. delta is too high).
                   ('nested', { 'a': ('int', 5, 20), 'b': ('field', 'a') })
                   will generate a record with 'a' and 'b' fields of equal value.
                   ('nested', {
                       'a': ('int', (0, 100)),
                       'b': ('field', ('a', None, ('inc', (0, 1, 3))))
                   })
                   will generate a random from 'a', but 'b' will repeat the value of
                   'a' for 4 records in a row. Eg. .generate(10) may result in:
                   [{'a': 13, 'b': 13}, {'a': 68, 'b': 13}, {'a': 94, 'b': 13},
                    {'a': 3, 'b': 3}, {'a': 58, 'b': 3}, {'a': 83, 'b': 3},
                    {'a': 91, 'b': 91}, {'a': 50, 'b': 91}, {'a': 93, 'b': 91},
                    {'a': 78, 'b': 78}]

        Apply a Function:
            ('apply', (<function>, [<children>][, <return_type>]))
                   - allows you to apply any code to a set of input (<children>),
                   to generate your output value. We recommend to have a top level
                   defined function (i.e. not a lambda / local) for this one,
                   and annotate the return type. When not annotated, add a return
                   type to the generator specification.
                   Example:
                     def add(x: int, y: int) -> int:
                         return x + y
                     Then:
                     ('apply', (add, [('inc', 0), ('inc', 10)]))
                     Would return 1+10=11, 2+11=13, 3+12=15 etc

        When spec is not a tuple, it is equivalent w/ a contant value generator:
        ('val', spec)
        Alternately one can specify an generator itself.
    """
    if not isinstance(builder, Builder):
        builder = Builder(builder)
    if issubclass(type(spec), Generator):
        return spec
    if not isinstance(spec, (tuple, list)):
        return ValueGenerator(builder.rand, spec)
    if len(spec) != 2:
        raise ValueError(
            f'Expecting exactly two elements in the generator spec: {spec}')
    if spec[0] not in _CLASS_MAP:
        raise ValueError(f'Cannot find any generator under name `{spec[0]}`')
    return _CLASS_MAP[spec[0]].build(builder, spec[1])
