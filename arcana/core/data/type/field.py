import attrs
from .base import Field


@attrs.define
class Str(Field):

    value: str = attrs.field(converter=str, default=None)


@attrs.define
class Bool(Field):

    value: bool = attrs.field(converter=bool, default=None)


@attrs.define
class Int(Field):

    value: int = attrs.field(converter=int, default=None)


@attrs.define
class Float(Field):

    value: float = attrs.field(converter=float, default=None)
