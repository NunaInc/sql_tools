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
from dataschema.schema_types import NamedTuple, OptNamedTuple
from typing import Optional


@dataclass
class InnerClass:
    field_b: str


@dataclass
class OuterClass:
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
    non_nested_field: Annotate(str, [
        annotations.ClickhouseNestedType("Tuple")
    ])
