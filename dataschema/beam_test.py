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
"""Tests the conversion to Beam schema structures."""

import unittest
from dataclasses import dataclass

from dataschema import (python2schema, schema2beam, schema_example,
                        schema_test_data)
from typing import Dict, List, Optional, Set


@dataclass
class Element:
    foo: str
    bar: Optional[int]


@dataclass
class SuperElement:
    baz: int
    element: Element
    optelement: Optional[Element]


@dataclass
class TestStruct:
    element: Element
    olist: List[Optional[int]]
    elist: List[Element]
    eset: Set[Element]
    emap: Dict[int, Optional[Element]]
    super_element: Optional[SuperElement]


BEAM_SCHEMA = """fields {
  name: "id"
  type {
    atomic_type: INT64
  }
}
fields {
  name: "fint32"
  type {
    atomic_type: INT32
  }
}
fields {
  name: "fsint32"
  type {
    atomic_type: INT32
  }
}
fields {
  name: "fint64"
  type {
    atomic_type: INT64
  }
}
fields {
  name: "fsint64"
  type {
    nullable: true
    atomic_type: INT64
  }
}
fields {
  name: "fdouble"
  type {
    nullable: true
    atomic_type: DOUBLE
  }
}
fields {
  name: "ffloat"
  type {
    nullable: true
    atomic_type: FLOAT
  }
}
fields {
  name: "fstring"
  type {
    nullable: true
    atomic_type: STRING
  }
}
fields {
  name: "fbytes"
  type {
    nullable: true
    atomic_type: BYTES
  }
}
fields {
  name: "fdate"
  type {
    nullable: true
    logical_type {
      urn: "dataschema:logical_type:date:v1"
      representation {
        atomic_type: STRING
      }
    }
  }
}
fields {
  name: "ftimestamp"
  type {
    nullable: true
    logical_type {
      urn: "dataschema:logical_type:datetime:v1"
      representation {
        atomic_type: STRING
      }
    }
  }
}
fields {
  name: "fdqannotated"
  type {
    nullable: true
    atomic_type: STRING
  }
}
fields {
  name: "frep_seq"
  type {
    iterable_type {
      element_type {
        nullable: true
        atomic_type: STRING
      }
    }
  }
}
fields {
  name: "frep_array"
  type {
    iterable_type {
      element_type {
        nullable: true
        atomic_type: STRING
      }
    }
  }
}
fields {
  name: "frep_set"
  type {
    nullable: true
    array_type {
      element_type {
        atomic_type: STRING
      }
    }
  }
}
fields {
  name: "fdecimal_bigint"
  type {
    nullable: true
    logical_type {
      urn: "beam:logical_type:decimal:v1"
      representation {
        atomic_type: BYTES
      }
    }
  }
}
fields {
  name: "fdecimal_default"
  type {
    nullable: true
    logical_type {
      urn: "beam:logical_type:decimal:v1"
      representation {
        atomic_type: BYTES
      }
    }
  }
}
fields {
  name: "fdecimal_bigdecimal"
  type {
    nullable: true
    logical_type {
      urn: "beam:logical_type:decimal:v1"
      representation {
        atomic_type: BYTES
      }
    }
  }
}
fields {
  name: "with__original__name"
  type {
    nullable: true
    atomic_type: STRING
  }
}
fields {
  name: "finitialized"
  type {
    iterable_type {
      element_type {
        nullable: true
        atomic_type: STRING
      }
    }
  }
}
fields {
  name: "fwidth"
  type {
    nullable: true
    atomic_type: STRING
  }
}
fields {
  name: "flob"
  type {
    nullable: true
    atomic_type: STRING
  }
}
fields {
  name: "fcommented"
  description: "Some comment"
  type {
    nullable: true
    atomic_type: STRING
  }
}
fields {
  name: "fboolean"
  type {
    nullable: true
    atomic_type: BOOLEAN
  }
}
id: "5161e6fd-521d-b587-d818-11acadbf55c7"
"""

ELEMENT_BEAM_SCHEMA = """fields {
  name: "element"
  type {
    row_type {
      schema {
        fields {
          name: "foo"
          type {
            atomic_type: STRING
          }
        }
        fields {
          name: "bar"
          type {
            nullable: true
            atomic_type: INT64
          }
        }
        id: "3a3836ee-b2ef-8809-04b9-02d3d90fb0b7"
      }
    }
  }
}
fields {
  name: "olist"
  type {
    nullable: true
    array_type {
      element_type {
        nullable: true
        atomic_type: INT64
      }
    }
  }
}
fields {
  name: "elist"
  type {
    iterable_type {
      element_type {
        nullable: true
        row_type {
          schema {
            fields {
              name: "foo"
              type {
                atomic_type: STRING
              }
            }
            fields {
              name: "bar"
              type {
                nullable: true
                atomic_type: INT64
              }
            }
            id: "4f3cb811-3b98-9b64-722a-b2960718e48d"
          }
        }
      }
    }
  }
}
fields {
  name: "eset"
  type {
    nullable: true
    array_type {
      element_type {
        row_type {
          schema {
            fields {
              name: "foo"
              type {
                atomic_type: STRING
              }
            }
            fields {
              name: "bar"
              type {
                nullable: true
                atomic_type: INT64
              }
            }
            id: "3a3836ee-b2ef-8809-04b9-02d3d90fb0b7"
          }
        }
      }
    }
  }
}
fields {
  name: "emap"
  type {
    nullable: true
    map_type {
      key_type {
        atomic_type: INT64
      }
      value_type {
        nullable: true
        row_type {
          schema {
            fields {
              name: "foo"
              type {
                atomic_type: STRING
              }
            }
            fields {
              name: "bar"
              type {
                nullable: true
                atomic_type: INT64
              }
            }
            id: "20c65db9-23c5-a9d2-16ba-ca6656d9c9e9"
          }
        }
      }
    }
  }
}
fields {
  name: "super_element"
  type {
    nullable: true
    row_type {
      schema {
        fields {
          name: "baz"
          type {
            atomic_type: INT64
          }
        }
        fields {
          name: "element"
          type {
            row_type {
              schema {
                fields {
                  name: "foo"
                  type {
                    atomic_type: STRING
                  }
                }
                fields {
                  name: "bar"
                  type {
                    nullable: true
                    atomic_type: INT64
                  }
                }
                id: "23ef164f-cc5c-0800-44c8-00df6794cc68"
              }
            }
          }
        }
        fields {
          name: "optelement"
          type {
            nullable: true
            row_type {
              schema {
                fields {
                  name: "foo"
                  type {
                    atomic_type: STRING
                  }
                }
                fields {
                  name: "bar"
                  type {
                    nullable: true
                    atomic_type: INT64
                  }
                }
                id: "513fff5f-dbd5-d531-9a6b-6df0d5dbdd05"
              }
            }
          }
        }
        id: "7afffa7d-358e-4d02-4b1c-d0db507ef00f"
      }
    }
  }
}
id: "0b278d9a-3a85-e667-1677-6f978cb4c7e5"
"""


class BeamTest(unittest.TestCase):

    def test_schema2beam(self):
        table = python2schema.ConvertDataclass(schema_test_data.TestProto)
        struct_type = schema2beam.ConvertTable(table)
        print(f'Beam schema: {struct_type}')
        self.assertEqual(f'{struct_type}', BEAM_SCHEMA)

    def test_example(self):
        table = python2schema.ConvertDataclass(schema_example.Example)
        struct_type = schema2beam.ConvertTable(table)
        print(f'Example:\n{struct_type}')

    def test_nested(self):
        table = python2schema.ConvertDataclass(TestStruct)
        struct_type = schema2beam.ConvertTable(table)
        # print(f'TestStruct:\n{struct_type}')
        self.assertEqual(f'{struct_type}', ELEMENT_BEAM_SCHEMA)


if __name__ == '__main__':
    unittest.main()
