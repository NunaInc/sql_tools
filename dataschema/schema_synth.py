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
"""Module for generating synthetic data for a schema."""

import dataclasses
import numpy.random
import re
import os

from dataschema import Schema, Schema_pb2, data_writer, entity, python2schema, synthgen
from enum import IntEnum
from typing import Any, Dict, List, Optional, Set, Tuple, Union

DEFAULT_BOOL = ('tobool', ('int', (0, 2)))
# Would include negatives in the default generator, but that would actually
# be less useful, as people usually mean 'positive values' in their integer
# columns, and they can override these values to produce something more specific.
DEFAULT_INT_8 = ('int', (0, 127))
DEFAULT_UINT_8 = ('int', (0, 255))
DEFAULT_INT = ('int', (0, 32767))
DEFAULT_UINT = ('int', (0, 65535))
DEFAULT_STRING = ('str', 8)
DEFAULT_BYTES = ('tobytes', ('str', 8))
DEFAULT_FLOAT = ('uniform', (0, 100))
DEFAULT_DATE = ('date', (('todate', '2010-01-01'), ('int', (0, 365 * 10 + 2))))
DEFAULT_DATETIME = ('datetime', (('todatetime', '2010-01-01'),
                                 ('int', (0, (365 * 10 + 2) * 24 * 3600))))
DEFAULT_DECIMAL = ('todecimal', ('format', ([('int', (0, 10000)),
                                             ('int', (0, 100))], '{}.{:02d}')))

DEFAULT_BY_TYPE = {
    Schema_pb2.ColumnInfo.TYPE_STRING: DEFAULT_STRING,
    Schema_pb2.ColumnInfo.TYPE_BYTES: DEFAULT_BYTES,
    Schema_pb2.ColumnInfo.TYPE_BOOLEAN: DEFAULT_BOOL,
    Schema_pb2.ColumnInfo.TYPE_INT_8: DEFAULT_INT_8,
    Schema_pb2.ColumnInfo.TYPE_INT_16: DEFAULT_INT,
    Schema_pb2.ColumnInfo.TYPE_INT_32: DEFAULT_INT,
    Schema_pb2.ColumnInfo.TYPE_INT_64: DEFAULT_INT,
    Schema_pb2.ColumnInfo.TYPE_UINT_8: DEFAULT_UINT_8,
    Schema_pb2.ColumnInfo.TYPE_UINT_16: DEFAULT_UINT,
    Schema_pb2.ColumnInfo.TYPE_UINT_32: DEFAULT_UINT,
    Schema_pb2.ColumnInfo.TYPE_UINT_64: DEFAULT_UINT,
    Schema_pb2.ColumnInfo.TYPE_DECIMAL: DEFAULT_DECIMAL,
    Schema_pb2.ColumnInfo.TYPE_FLOAT_32: DEFAULT_FLOAT,
    Schema_pb2.ColumnInfo.TYPE_FLOAT_64: DEFAULT_FLOAT,
    Schema_pb2.ColumnInfo.TYPE_DATE: DEFAULT_DATE,
    Schema_pb2.ColumnInfo.TYPE_DATETIME_64: DEFAULT_DATETIME,
}

DEFAULT_INT_ID = ('inc', 1)
DEFAULT_DATE_ID = ('date', (('todate', '2010-01-01'), ('inc', 1)))
DEFAULT_DATETIME_ID = ('datetime', (('todatetime', '2010-01-01'), ('inc', 1)))
_POSSIBLE_ID_TYPES = {
    Schema_pb2.ColumnInfo.TYPE_STRING,
    Schema_pb2.ColumnInfo.TYPE_BYTES,
    Schema_pb2.ColumnInfo.TYPE_INT_32,
    Schema_pb2.ColumnInfo.TYPE_INT_64,
    Schema_pb2.ColumnInfo.TYPE_UINT_32,
    Schema_pb2.ColumnInfo.TYPE_UINT_64,
    Schema_pb2.ColumnInfo.TYPE_DATE,
    Schema_pb2.ColumnInfo.TYPE_DATETIME_64,
}


class OutputType(IntEnum):
    DICT = data_writer.InputType.DICT
    DATAFRAME = data_writer.InputType.DATAFRAME
    DATACLASS = data_writer.InputType.DATACLASS


DEFAULT_LENGTH = ('int', (2, 6))
DEFAULT_NONE_PROBABILITY = 0.05


class GeneratorInfo:
    """Wraps information about a synthetic table generator."""

    def __init__(self, table: Schema.Table, data_class: type,
                 generator: synthgen.Generator, output_type: OutputType):
        self.table = table
        self.data_class = data_class
        self.generator = generator
        self.output_type = output_type

    def name(self):
        return self.table.name()


class Builder:
    """Class for building synthetic data generators for columns, tables, schemas."""

    def __init__(self,
                 generators: Optional[Dict[str, synthgen.GeneratorSpec]] = None,
                 re_generators: Optional[Tuple[Union[str, re.Pattern],
                                               synthgen.GeneratorSpec]] = None,
                 none_probability: float = DEFAULT_NONE_PROBABILITY,
                 length_generators: Dict[str, synthgen.GeneratorSpec] = None,
                 defaults: Dict[int, synthgen.GeneratorSpec] = None,
                 default_length: synthgen.GeneratorSpec = DEFAULT_LENGTH,
                 joint_fields: Optional[Dict[str, str]] = None,
                 joint_is_choice: Optional[Set[str]] = None):
        """Builds a synthesizer of synthetic data generators for schemas.

        We resolve a column generator in this order:
          1/ we look in the `generators` for the composed column.name()
          2/ we use the `column.synthgen` if not None
          3/ we look in re_generator for the first match on column.name()
          4/ we build a default generator based on the type of column
        At the end, we make sure that the chosen generator matches the structure
        of the field. i.e. if the field is repeated / array and the generator is
        not, we wrap it in an array generator with the length determined through
        `length_generators` or `default_length`

        Args:
            generators: per column generators - maps from column name to
               generator or generator specification, possibly nested.
               This means that if a nested field `foo` has a sub-field `bar`, you
               can specify the generator for bar under the foo key:
               { ... 'foo': { ... 'bar': <generator for bar under foo> ... } ... }
            re_generators: generators that we choose for field names, if they
               match any of the patterns in the list, tested in order.
               E.g. [('.*_name', ('faker', 'name')),
                     ('*_id', ('str', ('0123456789ABCDEF', 12)))]
            none_probability: the probability of generating None values for
               optional fields.
            length_generators: generators of length values for arrays, repeated,
               dicts. It maps from field name to a integer generator
               specification. Same structure as generators for nested fields.
            defaults: default per-type generators - for all fields that don't have
               a specific generator defined in `generators` - this is where we
               will get the generator for its specific type.
            default_length: default generator for lengths of arrays, repeated, dicts.
            joint_fields: an optional map from full field name to an ID that is used
               to join the generation of fields for multiple tables. For example,
               if you have a column that is joint cross structures:
                  class Foo:
                    foo_id: int
                    foo_value: str

                  class Bar:
                    bar_id: int
                    foo_id: int
                    bar_value: str
                  you may want to pass a joint_fields as:
                  {'Foo.foo_id': 'foo_id', 'Bar.foo_id': 'foo_id'}

            joint_is_choice: full field names that are choices of already generated
                  values of joint_fields. In the example above, if Bar.foo_id is
                  not necessary an id for Bar, and if you generate more Bars than
                  Foos, you may want to pass {'Bar.foo_id'} as a joint_is_choice.
        """
        self.generators = generators.copy() if generators else {}
        self.re_generators = []
        if re_generators:
            for s, gen in re_generators:
                if isinstance(s, str):
                    self.re_generators.append((re.compile(s), gen))
                elif isinstance(s, re.Pattern):
                    self.re_generators.append((s, gen))
                else:
                    raise ValueError('Expecting a string or re.Pattern for '
                                     f're_generators. Got `{s}`')
        self.none_probability = none_probability
        self.length_generators = length_generators if length_generators is not None else {}
        if defaults is None:
            self.defaults = DEFAULT_BY_TYPE.copy()
        else:
            self.defaults = defaults
            self.defaults.update(DEFAULT_BY_TYPE)
        self.default_length = default_length
        self.joint_fields = joint_fields.copy() if joint_fields else {}
        self.joint_is_choice = joint_is_choice.copy(
        ) if joint_is_choice else set()

    def find_by_name(self, full_name: str, source: Dict[str, Any]) -> Any:
        """Finds the element for `full_name` by looking recursively in source."""
        if full_name in source:
            return source[full_name]
        names = full_name.split('.')
        crt_gen = source
        for name in names:
            if name not in crt_gen:
                return None
            crt_gen = crt_gen[name]
        return crt_gen

    def find_generator_by_name(self, full_name: str,
                               column: Schema.Column) -> synthgen.GeneratorSpec:
        """Finds a generator by name: either looks in generators or re_generators."""
        spec_gen = self.find_by_name(full_name, self.generators)
        if spec_gen:
            return spec_gen
        if column and column.synthgen:
            return column.synthgen
        for pattern, gen in self.re_generators:
            if pattern.fullmatch(full_name):
                return gen
        return None

    def find_field_type(self, data_class: type, name: str) -> type:
        """Looks into data_class, finds the field `name`, and returns its type."""
        if not data_class:
            return None
        for field in dataclasses.fields(data_class):
            if field.name == name:
                sub_type = entity.GetOriginalType(field.type)
                if entity.GetStructuredTypeName(sub_type):
                    sub_type = sub_type.__args__[0]
                return sub_type
        raise ValueError(
            f'Cannot find sub-class for field `{name}` in class `{data_class}`')

    def build_type_generator(self, column_name: str, column_type: int,
                             is_id: int) -> synthgen.GeneratorSpec:
        """Builds the default generator for provided column type."""
        if column_type not in self.defaults:
            return ('val', None)
        if not is_id:
            return self.defaults[column_type]
        if column_type not in _POSSIBLE_ID_TYPES:
            raise ValueError(
                f'Invalid type found for Id-like column {column_name}: '
                f'type: {Schema_pb2.ColumnInfo.ColumnType.Name(column_type)}')
        if column_type not in (Schema_pb2.ColumnInfo.TYPE_STRING,
                               Schema_pb2.ColumnInfo.TYPE_BYTES):
            if column_type == Schema_pb2.ColumnInfo.TYPE_DATE:
                return DEFAULT_DATE_ID
            if column_type == Schema_pb2.ColumnInfo.TYPE_DATETIME_64:
                return DEFAULT_DATETIME_ID

            return DEFAULT_INT_ID
        gen = ('concat', ([('val', column_name),
                           ('tostring', DEFAULT_INT_ID)], '_'))
        if column_type == Schema_pb2.ColumnInfo.TYPE_BYTES:
            return ('tobytes', gen)
        return gen

    def build_length_generator(self, full_name: str) -> synthgen.GeneratorSpec:
        """Returns the generator of length for the full field name (foo.bar.baz)."""
        generator = self.find_by_name(full_name, self.length_generators)
        if not generator:
            return self.default_length
        return generator

    def build_nested_generator(
            self, fields: List[Schema.Column], prefix: str,
            output_type: OutputType,
            data_class: Optional[type]) -> synthgen.GeneratorSpec:
        """Builds the generator for the fields of a nested column."""
        sub_gen = {}
        id_count = sum(field.is_id() for field in fields)
        sub_output_type = output_type
        if sub_output_type == OutputType.DATAFRAME:
            sub_output_type = OutputType.DICT
        for field in fields:
            sub_name = Schema.full_name(prefix, field.name())
            sub_data_class = self.find_field_type(data_class, field.name())
            crt_gen = self.column_generator(field, id_count < 2 and
                                            field.is_id(), sub_output_type,
                                            sub_name, sub_data_class)
            sub_gen[field.name()] = crt_gen
        if output_type == OutputType.DATACLASS:
            if not data_class:
                raise ValueError('Please provide a `data_class` argument when '
                                 'outputting data class')
            return ('dataclass', (sub_gen, data_class))
        elif output_type == OutputType.DATAFRAME:
            return ('dataframe', sub_gen)
        else:
            return ('nested', sub_gen)

    def build_default_generator(
            self,
            column: Schema.Column,
            is_id: bool,
            full_name: str,
            output_type: OutputType,
            data_class: Optional[type] = None,
            disable_optional: Optional[bool] = False) -> synthgen.GeneratorSpec:
        """Builds the default generator for a column.

        Args:
           column - the column we build the generator for
           full_name - full name of the column (for nested includes parent columns)
           data_class - the python representation of the column (if needed)
           disable_optional - disable the generation of optional None values for
              optional columns.
        """
        ctype = column.info.column_type
        generator = None
        disable_repeated = False
        if ctype in (Schema_pb2.ColumnInfo.TYPE_ARRAY,
                     Schema_pb2.ColumnInfo.TYPE_SET):
            generator = ('array', (self.build_default_generator(
                column.fields[0], is_id, full_name, output_type,
                data_class), self.build_length_generator(full_name)))
            disable_repeated = True
            if ctype == Schema_pb2.ColumnInfo.TYPE_SET:
                generator = ('set', generator)
        elif ctype == Schema_pb2.ColumnInfo.TYPE_MAP:
            generator = ('dict',
                         (self.build_default_generator(column.fields[0], False,
                                                       full_name, output_type,
                                                       data_class),
                          self.build_default_generator(column.fields[1], False,
                                                       full_name, output_type,
                                                       data_class, True),
                          self.build_length_generator(full_name)))
            disable_repeated = True
        elif ctype == Schema_pb2.ColumnInfo.TYPE_NESTED:
            generator = self.build_nested_generator(column.fields, full_name,
                                                    output_type, data_class)
        else:
            generator = self.build_type_generator(column.name(), ctype, is_id)
        if column.is_optional() and (not disable_optional and
                                     self.none_probability and
                                     full_name not in self.joint_fields):
            generator = ('choice',
                         ([generator, None],
                          [1 - self.none_probability, self.none_probability]))
        if column.is_repeated() and not disable_repeated:
            generator = ('array', (generator,
                                   self.build_length_generator(full_name)))
        return generator

    def _ensure_structure(
            self, column: Schema.Column, full_name: str,
            generator: synthgen.GeneratorSpec) -> synthgen.GeneratorSpec:
        """Ensures `generator` is generating the right structure for `column`."""
        ctype = column.info.column_type
        if (not column.is_repeated() and
                ctype != Schema_pb2.ColumnInfo.TYPE_ARRAY and
                ctype != Schema_pb2.ColumnInfo.TYPE_SET):
            return generator

        gen = synthgen.BuildGenerator(numpy.random.PCG64(), generator)
        struct_name = entity.GetStructuredTypeName(
            entity.GetOriginalType(gen.result_type()))
        if struct_name == Schema_pb2.ColumnInfo.TYPE_SET:
            if struct_name == 'set':
                return generator
            if struct_name == 'list':
                return ('set', generator)
        generator = ('array', (generator,
                               self.build_length_generator(full_name)))
        if struct_name == Schema_pb2.ColumnInfo.TYPE_SET:
            generator = ('set', generator)
        return generator

    def column_generator(
            self,
            column: Schema.Column,
            is_id: bool,
            output_type: OutputType,
            full_name: Optional[str] = None,
            data_class: Optional[type] = None) -> synthgen.GeneratorSpec:
        """Builds a data generator for specific column."""
        if not full_name:
            full_name = column.name()
        spec_gen = self.find_generator_by_name(full_name, column)
        if column.info.column_type == Schema_pb2.ColumnInfo.TYPE_NESTED:
            if spec_gen is dict or spec_gen is None:
                return self._ensure_structure(
                    column, full_name,
                    self.build_nested_generator(column.fields, full_name,
                                                output_type, data_class))
        if spec_gen:
            return self._ensure_structure(column, full_name, spec_gen)
        return self.build_default_generator(column, is_id, full_name,
                                            output_type, data_class)

    def table_generator(self,
                        output_type: Optional[OutputType] = OutputType.DICT,
                        table: Optional[Schema.Table] = None,
                        data_class: Optional[type] = None,
                        name_prefix: str = '') -> synthgen.GeneratorSpec:
        """Produces a generator for a table.

        Args:
           output_type - what kind of data the generators output.
           table - schema representation of the table - specify this or data_class;
           data_class - original dataclass - needed to build a
               generator that produces dataclasses of this instance;
           generate_dataframe - when true, we create a dataframe generator.
           name_prefix - prefix when looking up names for the fields of this table.
        Returns:
           a synthetic data generator specification.
        """
        if table is None:
            if not data_class:
                raise ValueError('No table and no data_class provided.')
            table = python2schema.ConvertDataclass(data_class)
        if output_type == OutputType.DATACLASS and not data_class:
            raise ValueError('Please provide a `data_class` argument when '
                             'outputting data class')
        return self.build_nested_generator(table.columns, name_prefix,
                                           output_type, data_class)

    def _extract_fields(
            self, prefix: str, columns: List[Schema.Column]
    ) -> List[Tuple[str, Schema.Column, bool]]:
        """Recursively produce a list of full field names, and if they are ids."""
        sub_fields = []
        id_count = sum(col.is_id() for col in columns)
        for col in columns:
            full_name = Schema.full_name(prefix, col.name())
            sub_fields.append((full_name, col, id_count < 2 and col.is_id()))
            if col.info.column_type == Schema_pb2.ColumnInfo.TYPE_NESTED:
                sub_fields.extend(self._extract_fields(full_name, col.fields))
        return sub_fields

    def create_joint_structure(
        self, sub_fields: List[Tuple[str, Schema.Column, bool]]
    ) -> Tuple[Dict[str, str], Set[str]]:
        """Generates a joint field map and choice map from a list of full field
        names and if they are identifiers."""
        id_fields = {}
        joint_map = {}
        for field in sub_fields:
            if field[2]:
                id_fields[field[0]] = field[1]
            last_name = field[0].split('.')[-1]
            key = (last_name, field[1].info.column_type)
            if key in joint_map:
                joint_map[key].append(field[0])
            else:
                joint_map[key] = [field[0]]
        result_joint = {}
        result_choice = set()
        joint_id = 0
        for key, v in joint_map.items():
            if len(v) < 2:
                continue
            id_field = None
            key_generators = [
                field_name for field_name in v if field_name in self.generators
            ]
            if not key_generators:
                key_generators = [
                    field_name for field_name in v if field_name in id_fields
                ]
            if key_generators:
                field_name = key_generators[0]
                joint_name = field_name
                if field_name in id_fields:
                    id_field = field_name
                    if joint_name not in self.generators:
                        field = id_fields[field_name]
                        self.generators[field_name] = self.build_type_generator(
                            field.name(), field.info.column_type, True)
            else:
                joint_name = f'{key[0]}-{joint_id}'
                joint_id += 1
            for field_name in v:
                result_joint[field_name] = joint_name
                if (id_field and field_name != id_field and
                    (field_name not in id_fields or
                     len(field_name.split('.')) > 2)):
                    result_choice.add(field_name)
        return (result_joint, result_choice)

    def find_joint_fields_by_name(
            self,
            tables: List[Schema.Table]) -> Tuple[Dict[str, str], Set[str]]:
        """From a list of tables detect the joint fields and chice options."""
        joint_fields = []
        for t in tables:
            joint_fields.extend(self._extract_fields(t.name(), t.columns))
        return self.create_joint_structure(joint_fields)

    def schema_generator(
            self,
            output_type: Optional[OutputType] = OutputType.DICT,
            tables: Optional[List[Schema.Table]] = None,
            data_classes: Optional[List[type]] = None,
            autodetect_joins: bool = True,
            rand: Optional[numpy.random.Generator] = None
    ) -> List[GeneratorInfo]:
        """Produces a list of generators for data presented through tables.

        Note: this function changes the internal member according to joint fields
        we detect. We also expect the generators to contain full field names,
        including the class name.

        Args:
           output_type - the type of output the generators produce;
           tables - schema tables for which we produce generators; provide this
               or the `data_classes`
           data_classes - python data classes that represent the schema. Must be
               provided if output_type requests DATACLASSES. Must be provided if
               not tables is provided.
           autodetect_joins - if no joint_fields / joint_is_choice is provided,
               we try to detect these by considering that fields and columns
               with the same name need to contain same data (joinable)
           rand - a random number generator - optional, we create one if not
               provided.

        Return:
           a list of GeneratorInfo, which includes a synthetic data Generators,
           for the tables / data classes. They may come in a different order,
           determined byt the autodetected join ordering.
        """
        if tables is None:
            if not data_classes:
                raise ValueError('No tables or data_classes provided.')
            tables = [python2schema.ConvertDataclass(dc) for dc in data_classes]
        if data_classes and tables and len(tables) != len(data_classes):
            raise ValueError('Different lengths of tables and data_classes.')
        if output_type == OutputType.DATACLASS and not data_classes:
            raise ValueError('Please provide a `data_classes` argument when '
                             'outputting data class')
        if autodetect_joins:
            (builder_fields,
             builder_is_choice) = self.find_joint_fields_by_name(tables)
            self.joint_fields.update(builder_fields)
            self.joint_is_choice.update(builder_is_choice)
        if rand is None:
            rand = numpy.random.Generator(numpy.random.PCG64())
        builder = synthgen.Builder(rand, self.joint_fields,
                                   self.joint_is_choice)
        generators = []
        order = self.find_generator_order([table.name() for table in tables])
        for index in range(len(tables)):
            table = tables[order[index]]
            data_class = data_classes[order[index]] if data_classes else None
            gen_spec = self.table_generator(output_type, table, data_class,
                                            table.name())
            gen = builder.set_prefix(table.name()).build(gen_spec)
            generators.append(GeneratorInfo(table, data_class, gen,
                                            output_type))
        return generators

    def find_generator_order(self, names: List[str]) -> List[int]:
        """After running `schema_generator` and autodetecting joins, run this
        to obtain the order in which the tables should be generated for proper
        key reusability (e.g. if key field a_id in A is a field in table B, then
        A should be generated first, because a_id is a choice of already generated
        keys in a_id). E.g. if you call `schema_generator(my_tables)` use
        `find_generator_order([table.name() for table in my_tables])` to obtain
        the indices from my_table in which order you should generate."""

        class _TopoSorter:
            """A simple and stupid topological sorer."""

            def __init__(self, size):
                self._size = size
                self._links_to = {}

            def add(self, src: int, dest: int):
                if dest in self._links_to:
                    self._links_to[dest].add(src)
                else:
                    self._links_to[dest] = {src}

            def sort(self):
                visited = [False] * self._size
                order = []
                for i in range(self._size):
                    if not visited[i]:
                        self._visit(i, visited, order)
                return order

            def _visit(self, i, visited, order):
                visited[i] = True
                if i in self._links_to:
                    for j in self._links_to[i]:
                        if not visited[j]:
                            self._visit(j, visited, order)
                order.append(i)

        indices = dict(
            zip([name.split('.')[0] for name in names], range(len(names))))
        sorter = _TopoSorter(len(names))
        for k, v in self.joint_fields.items():
            name_k = k.split('.')[0]
            name_v = v.split('.')[0]
            if name_k != name_v and name_k in indices and name_v in indices:
                sorter.add(indices[name_v], indices[name_k])
        return sorter.sort()


class FileGeneratorInfo:
    """Helper class to generate data files."""

    def __init__(self,
                 generator: GeneratorInfo,
                 count: int,
                 file_basename: Optional[str] = None,
                 extension: Optional[str] = None,
                 num_shards: int = None):
        self.generator = generator
        self.count = count
        self.file_basename = file_basename
        self.num_shards = num_shards
        self.extension = extension

    def name(self):
        return self.generator.table.name()

    def check_writer(self, writer: data_writer.BaseWriter):
        if int(self.generator.output_type) not in [
                int(t) for t in writer.data_types()
        ]:
            raise ValueError(
                f'Cannot output data type: {self.generator.output_type} '
                f'with writer {writer}')

    def basename(self):
        if self.file_basename:
            return self.file_basename
        return self.name()

    def filename(self, dirname: str, shard: Optional[int] = None):
        if shard is None or self.num_shards is None:
            return os.path.join(dirname, self.basename())
        else:
            fname = f'{self.basename()}_{shard:05d}-of-{self.num_shards:05d}'
            return os.path.join(dirname, fname)

    def generate(self, writer: data_writer.BaseWriter, dirname: str):
        self.check_writer(writer)
        num_shards = self.num_shards if self.num_shards is not None else 1
        extension = self.extension if self.extension is not None else writer.extension(
        )
        files = []
        for shard in range(num_shards):
            print(f' - Generating {self.name()} - '
                  f'shard {shard} or {num_shards}')
            data = self.generator.generator.generate(size=self.count,
                                                     key=self.name())
            file_name = self.filename(dirname, shard) + extension
            print(f'   - Writing data to: {file_name}')
            file_object = writer.open(file_name)
            writer.write(self.generator.table, data, file_object)
            writer.close(file_object)
            del data
            files.append(file_name)
            print(' ... DONE')
        return files


def GenerateFiles(generators: List[FileGeneratorInfo],
                  writer: data_writer.BaseWriter, output_dir: str):
    for gen in generators:
        gen.check_writer(writer)
    file_names = {}
    for gen in generators:
        file_names[gen.name()] = gen.generate(writer, output_dir)
    return file_names
