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
"""Base module for creating util binaries for generating data."""

import json
import sys

from absl import flags

from dataschema import data_writer, schema_synth
from typing import List, Optional

FLAGS = flags.FLAGS

WRITER_NAMES = {
    'print': data_writer.PrintWriter,
    'csv': data_writer.CsvWriter,
    'json': data_writer.JsonWriter,
    'pickle': data_writer.PickleWriter,
    'parquet': data_writer.ParquetWriter
}

flags.DEFINE_string('output_dir', None,
                    'Output the generated data under this directory.')
flags.DEFINE_string(
    'output_type', 'print',
    f'Output type of the data. Can be one of these: {WRITER_NAMES.keys()}')
flags.DEFINE_string(
    'writer_args', '',
    'Json string including specific arguments for creating the '
    'data writer. Depends on the writer. Consult data_writer.py for '
    'which arguments are possible. '
    'For default work, just leave empty')
flags.DEFINE_string('counts', '',
                    'Map from class name to record count of that type.')
flags.DEFINE_integer('default_count', 1000,
                     'Default number of records to generate for each class.')
flags.DEFINE_string(
    'shards', '',
    'Map from class name to number of shards to generate for each '
    'class. This will result in shards file x count record.')
flags.DEFINE_integer('default_shards', 0,
                     'Default number of records to generate for each class.')
flags.DEFINE_string(
    'filenames', '',
    'Map from class name to output file name. If not specified '
    'we default to the class name as the output file name.')
flags.DEFINE_string(
    'extension', None,
    'Extensions for the file we generate. If not specified we '
    'default to the normal extension depending on output_type. '
    'Some outputs may employ compression depending on extension.')
flags.mark_flag_as_required('output_dir')


def _build_writer():
    if FLAGS.output_type not in WRITER_NAMES:
        sys.exit(f'Unknown writer type `{FLAGS.output_type}` choose '
                 f'from: {", ".join(WRITER_NAMES.keys())}')
    args = {}
    if FLAGS.writer_args:
        args = json.loads(FLAGS.writer_args)
    return WRITER_NAMES[FLAGS.output_type](**args)


def _get_flag_map(flag, name, converter):
    counts = {}
    if not flag:
        return counts
    for s in flag.split(','):
        comp = s.split(':')
        if len(comp) != 2:
            sys.exit(f'Invalid flag value --{name}: `{flag}`')
        counts[comp[0].strip()] = converter(comp[1].strip())
    return counts


def generate(data_classes: List[type],
             builder: Optional[schema_synth.Builder] = None):
    if builder is None:
        builder = schema_synth.Builder()
    writer = _build_writer()
    gens = builder.schema_generator(output_type=writer.data_types()[0],
                                    data_classes=data_classes)
    counts = _get_flag_map(FLAGS.counts, 'counts', int)
    filenames = _get_flag_map(FLAGS.filenames, 'filenames', str)
    shards = _get_flag_map(FLAGS.shards, 'shards', int)
    default_shards = FLAGS.default_shards if FLAGS.default_shards > 0 else None
    file_info = [
        schema_synth.FileGeneratorInfo(
            gen, counts.get(gen.name(), FLAGS.default_count),
            filenames.get(gen.name(), None), FLAGS.extension,
            shards.get(gen.name(), default_shards)) for gen in gens
    ]
    schema_synth.GenerateFiles(file_info, writer, FLAGS.output_dir)
