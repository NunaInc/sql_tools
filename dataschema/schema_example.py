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
"""An example of table dataclass equivalent to example.proto."""

import datetime
import decimal
from dataclasses import dataclass
from dataschema.entity import Annotate
from dataschema import annotations
from typing import Optional


@annotations.order_by(values='member_id')
@annotations.clickhouse_engine(engine='MERGE_TREE')
@annotations.index_granularity(value=8192)
@dataclass
class Example:
    """Simple example of dataclass definition."""
    member_id: str
    num_claims: int
    rx_num_claims: Annotate(Optional[int], annotations.Compression('ZSTD'))
    start_date: Optional[datetime.date]
    total_paid: Annotate(Optional[decimal.Decimal], annotations.Decimal(12, 2))
