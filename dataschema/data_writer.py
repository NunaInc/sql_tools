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
"""Contains various writers for writing python in-memory data to files."""
import json
import decimal
import datetime
import numpy
import pickle
import pyarrow
import os
import urllib.parse

from dataschema import Schema, schema2parquet, schema2pandas
from enum import IntEnum
from pyarrow import fs, parquet
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union


class InputType(IntEnum):
    """The acceptable type of data input for a writer."""
    DICT = 0
    DATAFRAME = 1
    DATACLASS = 2


class WriterType(IntEnum):
    """Type of outputs that a writer can create."""
    PRINT = 1
    CSV = 2
    PARQUET = 3
    JSON = 4
    PICKLE = 5
    SQLALCHEMY = 6


def _is_decimal_column(column):
    for value in column:
        if value is not None:
            return isinstance(value, decimal.Decimal)
    return False


def _decimals_to_str(dataframe):
    for column in dataframe.columns:
        if _is_decimal_column(dataframe[column]):
            dataframe[column] = [str(value) for value in dataframe[column]]


class BaseWriter:
    """Base class for python data writer to file."""

    def __init__(self):
        pass

    def open(self, file_name: str) -> Any:
        """Opens a file for writing - returns writer specific type."""
        return file_name

    def write(self, table: Schema.Table, data: Any, file_object: Any):
        """Writes `data`, which is in the provided `table` schema to a file."""
        raise NotImplementedError('Abstract writer')

    def close(self, file_object: Any):
        """Closes a previously opened file."""
        pass

    def writer_type(self) -> WriterType:
        """Returns the output type of this writer."""
        raise NotImplementedError('Abstract writer')

    def data_types(self) -> List[InputType]:
        """Returns the acceptable input data type for this writer."""
        raise NotImplementedError('Abstract writer')

    def extension(self) -> str:
        """Default file extension for files of this type."""
        return ''


class PrintWriter(BaseWriter):
    """Writer for printing data to stdout."""

    def write(self, table: Schema.Table, data: Any, file_object: Any):
        """Prints `data` to stdout."""
        if not isinstance(data, Dict) and isinstance(data, Iterable):
            for ob in data:
                print(ob)
        else:
            print(data)

    def writer_type(self) -> WriterType:
        return WriterType.PRINT

    def data_types(self) -> List[InputType]:
        return (InputType.DICT, InputType.DATAFRAME, InputType.DATACLASS)


class CsvWriter(BaseWriter):
    """Writer for csv file, corresponds to pandas.DataFrame.to_csv.

    For details pls. consult:
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html
    """

    def __init__(self,
                 convert_decimals: bool = False,
                 sep: str = ',',
                 na_rep: str = '',
                 float_format: Optional[str] = None,
                 columns: Optional[List[str]] = None,
                 header: bool = True,
                 index: bool = False,
                 index_label: Optional[str] = None,
                 encoding: Optional[str] = None,
                 compression: str = 'infer',
                 quoting: Optional[int] = None,
                 quotechar: str = '"',
                 line_terminator: Optional[str] = None,
                 date_format: Optional[str] = None,
                 doublequote: bool = True,
                 storage_options: Optional[Dict[Any, Any]] = None):
        super().__init__()
        self.convert_decimals = convert_decimals
        self.sep = sep
        self.na_rep = na_rep
        self.float_format = float_format
        self.columns = columns
        self.header = header
        self.index = index
        self.index_label = index_label
        self.encoding = encoding
        self.compression = compression
        self.quoting = quoting
        self.quotechar = quotechar
        self.line_terminator = line_terminator
        self.date_format = date_format
        self.doublequote = doublequote
        self.storage_options = storage_options

    def write(self, table: Schema.Table, data: Dict[Any, Any],
              file_object: str):
        """Writes `data`, which is in the provided `table` schema to a file."""
        df = schema2pandas.ToDataFrame(data, table)
        if self.convert_decimals:
            _decimals_to_str(df)
        df.to_csv(file_object,
                  sep=self.sep,
                  na_rep=self.na_rep,
                  float_format=self.float_format,
                  columns=self.columns,
                  header=self.header,
                  index=self.index,
                  index_label=self.index_label,
                  encoding=self.encoding,
                  compression=self.compression,
                  quoting=self.quoting,
                  quotechar=self.quotechar,
                  line_terminator=self.line_terminator,
                  date_format=self.date_format,
                  doublequote=self.doublequote,
                  storage_options=self.storage_options)

    def open(self, file_name):
        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        return file_name

    def writer_type(self) -> WriterType:
        return WriterType.CSV

    def data_types(self) -> List[InputType]:
        return (InputType.DATAFRAME,)

    def extension(self) -> str:
        return '.csv'


class _WriteEncoder(json.JSONEncoder):
    """Used for json encoding to convert types that we may generate."""

    def default(self, o):
        if type(o) in (str, int, float, bool):
            return o
        if isinstance(o, numpy.bool_):
            return bool(o)
        if isinstance(o, numpy.int64):
            return int(o)
        if isinstance(o, numpy.float):
            return float(o)
        if isinstance(o, numpy.ndarray):
            return list(o)
        if isinstance(o, (datetime.date, datetime.datetime)):
            return o.isoformat()
        if isinstance(o, decimal.Decimal):
            return str(o)
        return json.JSONEncoder.default(self, o)


class JsonWriter(BaseWriter):
    """Writer for json files. The members correspond to json.dump arguments."""

    def __init__(self,
                 encoding: str = 'utf-8',
                 skipkeys: bool = False,
                 ensure_ascii: bool = True,
                 check_circular: bool = True,
                 allow_nan: bool = True,
                 indent: Optional[Union[int, str]] = None,
                 separators: Optional[Tuple[str]] = None,
                 default: Optional[Callable[[Any], str]] = None,
                 sort_keys: bool = False):
        super().__init__()
        self.encoding = encoding
        self.skipkeys = skipkeys
        self.ensure_ascii = ensure_ascii
        self.check_circular = check_circular
        self.allow_nan = allow_nan
        self.indent = indent
        self.separators = separators
        self.default = default
        self.sort_keys = sort_keys

    def write(self, table: Schema.Table, data: Dict[Any, Any], file_object):
        """Writes `data`, which is in the provided `table` schema to a file."""
        json.dump(data,
                  file_object,
                  skipkeys=self.skipkeys,
                  ensure_ascii=self.ensure_ascii,
                  check_circular=self.check_circular,
                  allow_nan=self.allow_nan,
                  cls=_WriteEncoder,
                  indent=self.indent,
                  separators=self.separators,
                  default=self.default,
                  sort_keys=self.sort_keys)

    def open(self, file_name):
        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        return open(file_name, mode='w', encoding=self.encoding)

    def close(self, file_object):
        file_object.close()

    def writer_type(self) -> WriterType:
        return WriterType.JSON

    def data_types(self) -> List[InputType]:
        return (InputType.DICT, InputType.DATACLASS)

    def extension(self) -> str:
        return '.json'


class PickleWriter(BaseWriter):
    """Writer for writing pickled files.

    The members correspond to pickle.dump()
    """

    def __init__(self,
                 protocol: Optional[int] = None,
                 fix_imports: bool = True):
        super().__init__()
        self.protocol = protocol
        self.fix_imports = fix_imports

    def write(self, table: Schema.Table, data: Any, file_object):
        """Pickles `data`, which is in the provided `table` schema to a file."""
        pickle.dump(data,
                    file_object,
                    protocol=self.protocol,
                    fix_imports=self.fix_imports)

    def open(self, file_name):
        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        return open(file_name, mode='wb', encoding=None)

    def close(self, file_object):
        file_object.close()

    def writer_type(self) -> WriterType:
        return WriterType.PICKLE

    def data_types(self) -> List[InputType]:
        return (
            InputType.DICT,
            InputType.DATACLASS,
            InputType.DATAFRAME,
        )

    def extension(self) -> str:
        return '.pickle'


class ParquetWriter(BaseWriter):
    """Writes generated data to a parquet file, according to options.

    If you write a file intended for PySpark, use no_uint - as PySpark
    does not support uint types.
    """

    def __init__(
            self,
            no_uint: bool = False,
            # Options per pyarrow.parquet.ParquetWriter. Details:
            # https://arrow.apache.org/docs/python/generated
            #        /pyarrow.parquet.ParquetWriter.html
            version: str = '1.0',
            use_dictionary: bool = True,
            compression: str = 'snappy',
            write_statistics: Union[bool, List[str]] = True,
            flavor: Optional[str] = None,
            compression_level: Optional[int] = None,
            use_byte_stream_split: Union[bool, List[str]] = False,
            data_page_version: str = '1.0',
            use_compliant_nested_type: bool = True,
            # Write table rows in groups of this size:
            row_group_size: Optional[int] = None,
            # Options per FileSystem.open_output_stream:
            # https://arrow.apache.org/docs/python/generated
            # /pyarrow.fs.FileSystem.html
            # #pyarrow.fs.FileSystem.open_output_stream
            file_compression: str = 'detect',
            file_buffer_size: Optional[int] = None,
            file_metadata: Optional[Dict[str, str]] = None,
            # Options for external filesystem connection: S3 and HDFS
            s3_access_key: Optional[str] = None,
            s3_secret_key: Optional[str] = None,
            s3_session_token: Optional[str] = None,
            hdfs_host: Optional[str] = None,
            hdfs_port: Optional[int] = None):
        super().__init__()
        self.no_uint = no_uint
        self.version = version
        self.use_dictionary = use_dictionary
        self.compression = compression
        self.write_statistics = write_statistics
        self.flavor = flavor
        self.compression_level = compression_level
        self.use_byte_stream_split = use_byte_stream_split
        self.data_page_version = data_page_version
        self.use_compliant_nested_type = use_compliant_nested_type
        self.row_group_size = row_group_size
        self.file_compression = file_compression
        self.file_buffer_size = file_buffer_size
        self.file_metadata = file_metadata
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self.s3_session_token = s3_session_token
        self.hdfs_host = hdfs_host
        self.hdfs_port = hdfs_port

    def write(self, table: Schema.Table, data: Any, file_object):
        """Writes `data`, which is in the provided `table` schema to a parquet."""
        fsys = self.filesystem(file_object)
        path = fsys.normalize_path(self.path(file_object))
        fsys.create_dir(os.path.dirname(path))
        output_stream = fsys.open_output_stream(
            path,
            compression=self.file_compression,
            buffer_size=self.file_buffer_size,
            metadata=self.file_metadata)
        df = schema2pandas.ToDataFrame(data, table)
        schema = schema2parquet.ConvertTable(table, self.no_uint)
        arrow_table = pyarrow.Table.from_pandas(df, schema=schema)
        writer = parquet.ParquetWriter(
            output_stream,
            schema,
            flavor=self.flavor,
            version=self.version,
            use_dictionary=self.use_dictionary,
            compression=self.compression,
            write_statistics=self.write_statistics,
            compression_level=self.compression_level,
            use_byte_stream_split=self.use_byte_stream_split,
            data_page_version=self.data_page_version,
            use_compliant_nested_type=self.use_compliant_nested_type)
        writer.write_table(arrow_table, self.row_group_size)
        writer.close()

    def path(self, file_url: str) -> str:
        """Returns the path part of a url."""
        info = urllib.parse.urlparse(file_url)
        return info.path

    def filesystem(self, file_url: str) -> fs.FileSystem:
        """Opens a parquet file from an url/path. Use `s3` or `hdfs` as schemes."""
        info = urllib.parse.urlparse(file_url)
        if not info.scheme or info.scheme == 'file':
            return parquet.LocalFileSystem()
        if info.scheme == 's3':
            query_params = urllib.parse.parse_qs(info.query)
            return fs.S3FileSystem(
                access_key=query_params.get('access_key', self.s3_access_key),
                secret_key=query_params.get('secret_key', self.s3_secret_key),
                session_token=query_params.get('session_token',
                                               self.s3_session_token))
        if info.scheme == 'hdfs':
            host = info.host if info.host is not None else self.hdfs_host
            port = info.port if info.port is not None else self.hdfs_port
            return fs.HadoopFileSystem(host=host, port=port)
        raise ValueError(f'Unknown url scheme `{info.scheme}` for parquet file')

    def data_types(self) -> List[InputType]:
        return (InputType.DATAFRAME,)

    def extension(self) -> str:
        return '.parquet'


class SqlAlchemyWriter(BaseWriter):
    """Writes data to a sql alchemy table, via pandas.Dataframe.to_sql"""

    def __init__(self,
                 conn: Any,
                 schema: Optional[str] = None,
                 if_exists: Optional[str] = 'append',
                 index: bool = False,
                 index_label: Optional[str] = None,
                 chunksize: Optional[int] = None):
        super().__init__()
        self.conn = conn
        self.schema = schema
        self.if_exists = if_exists
        self.index = index
        self.index_label = index_label
        self.chunksize = chunksize

    def writer_type(self):
        return WriterType.SQLALCHEMY

    def data_types(self):
        return (InputType.DATAFRAME,)

    def write(self, table: Schema.Table, data: Dict[Any, Any],
              file_object: str):
        df = schema2pandas.ToDataFrame(data, table)
        _decimals_to_str(df)
        df.to_sql(file_object,
                  self.conn,
                  schema=self.schema,
                  if_exists=self.if_exists,
                  index=self.index,
                  index_label=self.index_label,
                  chunksize=self.chunksize)
