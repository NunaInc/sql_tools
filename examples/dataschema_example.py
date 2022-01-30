"""Examples for dataschema module."""
import dataclasses
import datetime
import tempfile
import typing

from dataschema import (
    data_writer,
    parquet2schema,
    proto2schema,
    python2schema,
    schema2dbml,
    schema2pandas,
    schema2parquet,
    schema2python,
    schema2scala,
    schema2sql,
    schema2sqlalchemy,
    schema_synth,
    schema_types,
)
from examples import example_pb2
from sqlalchemy.schema import CreateTable
from sql_analyze.grammars.ClickHouse import parse_sql_lib as parse_sql_ch


@dataclasses.dataclass
class CustomerInfo:
    customer_id: schema_types.Id(str)  # Annotated as ID column
    order_count: int
    start_date: datetime.date
    end_date: typing.Optional[datetime.date]
    last_payments: schema_types.DecimalList(10, 2)


def convert_from_schema():
    """Converts from a Schema table to various other formats."""
    table = python2schema.ConvertDataclass(CustomerInfo)
    print('=' * 80)
    print(f'Table proto representation:\n{table.to_proto()}')

    # Now, this table schema structure can be converted to various
    # other supported data schema types:

    print('=' * 80)
    sql = schema2sql.ConvertTable(table, table_name='customers')
    print(f'ClickHose SQL:\n{sql}')

    print('=' * 80)
    scala = schema2scala.ConvertTable(table, java_package='com.nuna.example')
    print(f'Scala Case Class:\n{scala}')

    print('=' * 80)
    parquet_schema = schema2parquet.ConvertTable(table)
    print(f'Parquet Schema:\n{parquet_schema}\n')

    print('=' * 80)
    alchemy_table = schema2sqlalchemy.ConvertTable(table)
    print(f'Sql Alchemy Table:{str(CreateTable(alchemy_table))}')

    print('=' * 80)
    dbml = schema2dbml.ConvertTable(table)
    print(f'DBML Table:\n{dbml}')

    print('=' * 80)
    pandas = schema2pandas.ConvertTable(table)
    print(f'Pandas Schema:\n{pandas}')

    print('=' * 80)
    python = schema2python.ConvertTable(table)
    print(f'Python Code:\n{python}')


def convert_proto_to_schema():
    """Converts a proto message descriptor to schema Table."""
    table = proto2schema.ConvertMessage(example_pb2.CustomerInfo.DESCRIPTOR)

    # Same as above, this schema can be converted to whatever we want.
    # We do it for python here:
    print('=' * 80)
    python = schema2python.ConvertTable(table)
    print(f'Proto Schema to Python Code:\n{python}')


def convert_sql_to_schema():
    """Converts a ClickHouse SQL create statement to schema Table.
    We use the sql_analyze library for this operation.
    """
    stmt = parse_sql_ch.parse_clickhouse_sql_create("""
CREATE TABLE CustomerInfo (
  customer_id String,
  order_count Int64,
  start_date Date,
  end_date Nullable(Date),
  last_payments Array(Decimal64(2))
)
""")
    table = stmt.schema
    # Same as above, this schema can be converted to whatever we want.
    # We do it for python here:
    print('=' * 80)
    python = schema2python.ConvertTable(table)
    print(f'SQL Schema to Python Code:\n{python}')

    # Schema comparison example:
    py_table = python2schema.ConvertDataclass(CustomerInfo)

    # This comparison tries to check if data in the SQL schema can be
    # read into the python dataschema:
    differences = py_table.compare(table)
    print('Differences found:')
    for diff in differences:
        print(diff)


def synthetic_generation():
    table = python2schema.ConvertDataclass(CustomerInfo)
    gen_dir = tempfile.mkdtemp()

    # Prepare synthetic data generator(s).
    # Generally we want to generate for multiple tables if join references
    # exist between them, so the keys are properly linked.
    builder = schema_synth.Builder()
    generators = builder.schema_generator(
        output_type=schema_synth.OutputType.DATAFRAME, tables=[table])

    num_records = 20
    file_info = [
        schema_synth.FileGeneratorInfo(gen, num_records) for gen in generators
    ]

    # Print Some generated data:
    schema_synth.GenerateFiles(file_info, data_writer.PrintWriter(), gen_dir)

    # Generate a CSV file:
    csv_file_names = schema_synth.GenerateFiles(file_info,
                                                data_writer.CsvWriter(),
                                                gen_dir)
    print(f'CSV file(s) generated: {csv_file_names}')

    # Generate a Parquet file:
    parquet_file_names = schema_synth.GenerateFiles(file_info,
                                                    data_writer.ParquetWriter(),
                                                    gen_dir)
    # Pick the first (and only) file name:
    parquet_file_name = list(parquet_file_names.values())[0][0]
    print(f'Parquet file(s) generated: {parquet_file_name}')

    # Read the schema from the parquet file:
    parquet_table = parquet2schema.ConvertParquetSchema(
        parquet2schema.OpenParquetFile(parquet_file_name))
    # Same as above, this schema can be converted to whatever we want.
    # We do it for python here:
    print('=' * 80)
    python = schema2python.ConvertTable(parquet_table)
    print(f'Parquet Schema to Python Code:\n{python}')


def main():
    convert_from_schema()
    convert_proto_to_schema()
    convert_sql_to_schema()
    synthetic_generation()


if __name__ == '__main__':
    main()
