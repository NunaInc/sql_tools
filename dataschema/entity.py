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
"""Utilityes for checking and."""
import dataclasses
import datetime
import decimal
from types import ModuleType
from typing import NewType, Union

# In your data declaration python modules define a JAVA_PACKAGE
# variable at top level to specify the corresponding Java package of generated
# classes.
JAVA_PACKAGE = 'JAVA_PACKAGE'


def GetJavaPackage(module: ModuleType) -> str:
    if hasattr(module, JAVA_PACKAGE):
        return getattr(module, JAVA_PACKAGE)
    else:
        return module.__name__


_SCHEMA_ANNOTATIONS = '__schema_annotations__'
_EXPECTED_DICT_KEYS = set([
    '__module__', '__annotations__', '__doc__', '__dict__', '__weakref__',
    '__dataclass_params__', '__dataclass_fields__', _SCHEMA_ANNOTATIONS
])
_EXPECTED_FUNCTIONS = ['__init__', '__repr__', '__eq__', '__hash__']
_BASE_TYPES = set([
    int, bytes, str, float, bool, datetime.date, datetime.datetime,
    decimal.Decimal
])

_SCHEMA_ANNOTATIONS = '__schema_annotations__'
_CLASS_ID = 0


def _Annotate(cls=None, annotation=None):
    """Annotates a class or a type. `annotation` should from annotation.py"""

    def Wrap(cls):
        schema_annotations = []
        if hasattr(cls, _SCHEMA_ANNOTATIONS):
            schema_annotations.extend(getattr(cls, _SCHEMA_ANNOTATIONS))
        if isinstance(annotation, list):
            schema_annotations.extend(annotation)
        else:
            schema_annotations.append(annotation)
        global _CLASS_ID
        _CLASS_ID += 1
        supertype = cls
        if hasattr(cls, '__supertype__'):
            supertype = cls.__supertype__
        annotated_type = NewType(f'Annotated_{_CLASS_ID}', supertype)
        setattr(annotated_type, _SCHEMA_ANNOTATIONS, schema_annotations)
        return annotated_type

    if cls is None:
        return Wrap
    return Wrap(cls)


def Annotate(cls, annotation):
    """Annotates a field type with the provided annotation."""
    return _Annotate(cls, annotation=annotation)


def IsAnnotatedType(field_cls: type):
    """If provided field_cls is an annotated type."""
    return hasattr(field_cls, _SCHEMA_ANNOTATIONS)


def GetAnnotatedType(field_cls: type):
    """Returns the original type behind the annotation (if any)."""
    if IsAnnotatedType(field_cls) and hasattr(field_cls, '__supertype__'):
        return field_cls.__supertype__
    return field_cls


def IsOptionalType(field_cls: type):
    """If the field_cls looks like an Optional[...] type."""
    return (hasattr(field_cls, '__origin__')
            # pylint: disable=comparison-with-callable
            and field_cls.__origin__ == Union and len(field_cls.__args__) == 2
            and field_cls.__args__[1] == type(None))


def GetOptionalType(field_cls: type):
    """Returns the type of optional & annotation or None if not optional."""
    field_cls = GetAnnotatedType(field_cls)
    if IsOptionalType(field_cls):
        return field_cls.__args__[0]
    return None


def GetOriginalType(field_cls: type):
    """Returns the type of field_cls, behind annotations and Optional."""
    field_cls = GetAnnotatedType(field_cls)
    if IsOptionalType(field_cls):
        return field_cls.__args__[0]
    return field_cls


def GetStructuredTypeName(field_cls: type):
    """Returns the structure type name for a type, behind annotation."""
    field_cls = GetAnnotatedType(field_cls)
    if not hasattr(field_cls, '__origin__'):
        return None
    if field_cls.__origin__ is dict:
        return 'dict'
    elif field_cls.__origin__ is list:
        return 'list'
    elif field_cls.__origin__ is set:
        return 'set'
    return None


def IsBasicType(field_cls: type):
    """If the type field_cls looks like one of the basic field types."""
    if GetAnnotatedType(field_cls) in _BASE_TYPES:
        return True


_MAX_DEPTH = 30


class FieldTypeChecker:
    """Checks the type of a fields in a dataclass."""

    def __init__(self, field_name, field_cls):
        self.field_name = field_name
        self.field_cls = field_cls
        self.checked = set()

    def _check(self, field_cls, depth):
        """Check if the type of a field is acceptable."""
        if field_cls in self.checked:
            return True
        if depth > _MAX_DEPTH:
            raise ValueError(f'Recursive field type found at {field_cls} '
                             f'for field `{self.field_name}`')
        field_cls = GetAnnotatedType(field_cls)
        if IsBasicType(field_cls):
            return True
        if hasattr(field_cls, '__origin__'):
            if field_cls.__origin__ is dict:
                self._check(field_cls.__args__[0], depth)
                self._check(field_cls.__args__[1], depth)
            elif field_cls.__origin__ is list:
                self._check(field_cls.__args__[0], depth)
            elif field_cls.__origin__ is set:
                self._check(field_cls.__args__[0], depth)
            elif (  # pylint: disable=comparison-with-callable
                    field_cls.__origin__ == Union and
                    len(field_cls.__args__) == 2 and
                    field_cls.__args__[1] == type(None)):
                if GetStructuredTypeName(field_cls) is not None:
                    raise ValueError('Cannot have Optional structured fields.'
                                     '(e.g. Optional[List or Set or Dict])')
                # Optional[...]
                self._check(field_cls.__args__[0], depth)
            else:
                raise ValueError(f'Invalid origin class for {field_cls}: '
                                 f'`{field_cls.__origin__}`')
        else:
            checker = DataclassChecker(field_cls)
            if checker.check_is_dataclass() is not None:
                raise ValueError(
                    f'Invalid type surfaced for field `{self.field_name}`: '
                    f'`{self.field_cls}` - {field_cls} is not acceptable')
            err = checker.check()
            if err:
                errors = '; '.join(err)
                raise ValueError(
                    f'Subfield entity class of field `{self.field_name}` '
                    f'({field_cls}) has type errors: {errors}')
        self.checked.add(field_cls)
        return True

    def check(self):
        return self._check(self.field_cls, 0)


class DataclassChecker:
    """Checks if a python type and its structure conforms to Dataclass specs."""

    def __init__(self, cls: type):
        self.cls = cls
        self.nested = []

    def _err_class(self):
        return f'dataclass class `{self.cls}` in module `{self.cls.__module__}`'

    def _err_field(self, field: str):
        return (f'field `{field}` of dataclass class `{self.cls.__name__}` '
                f'in module `{self.cls.__module__}`')

    def check_is_dataclass(self):
        if not dataclasses.is_dataclass(self.cls):
            return f'{self._err_class()} is not a dataclass'
        return None

    def _check_type(self, field_name, field_cls):
        try:
            FieldTypeChecker(field_name, field_cls).check()
            return None
        except ValueError as e:
            return f'{e.args[0]} for {self._err_field(field_name)}'

    def _check_field_type(self, field_name, field_cls):
        return self._check_type(GetOriginalType(field_name), field_cls)

    def _check_dataclass_members(self):
        err = []
        for key in self.cls.__dict__:
            # pylint: disable=comparison-with-callable,unidiomatic-typecheck
            if type(self.cls.__dict__[key]) == type:
                self.nested.append(
                    (key, DataclassChecker(self.cls.__dict__[key])))
            elif callable(
                    self.cls.__dict__[key]) and key not in _EXPECTED_FUNCTIONS:
                err.append(f'{self._err_class()} has unexpected function '
                           f'member `{key}`')
            elif (key not in _EXPECTED_DICT_KEYS and
                  key not in _EXPECTED_FUNCTIONS and
                  key not in self.cls.__annotations__):
                err.append(f'{self._err_class()} has unexpected / non annotated'
                           f' member `{key}`: {self.cls.__dict__[key]}')
        for field in dataclasses.fields(self.cls):
            field_err = self._check_field_type(field.name, field.type)
            if field_err is not None:
                err.append(field_err)
        for nested in self.nested:
            for nested_err in nested[1].check():
                err.append(f'{nested_err}; for nested sub-class '
                           f'{nested[0]} of {self._err_class()}')
        return err

    def check(self):
        err_dataclass = self.check_is_dataclass()
        if err_dataclass is not None:
            return [err_dataclass]
        return self._check_dataclass_members()


def SchemaAnnotations(cls: type):
    """Returns the schema annotations of a type."""
    annotations = []
    if hasattr(cls, _SCHEMA_ANNOTATIONS):
        annotations.extend(cls.__schema_annotations__)
    return annotations
