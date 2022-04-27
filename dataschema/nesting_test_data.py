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
"""A test module containing dataclasses for testing nested columns."""
from dataclasses import dataclass
from dataschema import annotations
from dataschema.entity import Annotate
from dataschema.schema_types import NamedTuple, OptNamedTuple, RepeatedNested
from typing import List, Optional


@dataclass
class InnerClass:
    """Class to nest inside another class."""
    field_b: str


@dataclass
class OuterClass:
    """Example class for testing nested fields."""
    field_a: str
    inner: InnerClass
    inner_tuple: Annotate(InnerClass, [
        annotations.ClickhouseNestedType("Tuple")
    ])
    optional_inner: Optional[InnerClass]
    optional_inner_tuple: Annotate(Optional[InnerClass], [
        annotations.ClickhouseNestedType("Tuple")
    ])
    inner_tuple_alias: NamedTuple(InnerClass)
    optional_inner_tuple_alias: OptNamedTuple(InnerClass)


@dataclass
class NestedBad:
    """Example class with invalid annotation."""
    non_nested_field: Annotate(str, [
        annotations.ClickhouseNestedType("Tuple")
    ])


@dataclass
class DoubleNested:
    """Example class to test two levels of nesting."""
    field_nested: InnerClass
    field_tuple: NamedTuple(InnerClass)


@annotations.default_compression(value="ZSTD")
@dataclass
class NestedCompression:
    """Example class to test compression in nested types."""
    field_a: str
    field_nested: InnerClass
    field_tuple: NamedTuple(InnerClass)
    double_nested: DoubleNested


@dataclass
class DoubleRepeatedNested:
    inner: InnerClass


@dataclass
class NestedWithArray:
    array: List[str]

@annotations.default_compression(value="ZSTD")
@dataclass
class OuterClassWithRepeatedNestedColumn:
    """Example class to test repeated nested columns."""
    field_a: str
    repeated_nested_from_default: List[InnerClass]
    repeated_nested_from_annotation: RepeatedNested(InnerClass)
    array_of_repeated_nested: List[List[InnerClass]]
    double_repeated_nested: List[DoubleRepeatedNested]
    repeated_nested_with_array: List[NestedWithArray]
    array_of_repeated_nested_with_array: List[List[NestedWithArray]]
