import typing as ty
from pydra.utils.hash import (
    register_serializer,
    Cache,
    bytes_repr,
    bytes_repr_sequence_contents,
)


# FIXME: Remove once this implementation has been merged into the Pydra release
@register_serializer(ty._GenericAlias)
@register_serializer(ty._SpecialForm)
@register_serializer(type)
def bytes_repr_type(klass: type, cache: Cache) -> ty.Iterator[bytes]:
    try:
        yield f"type:({klass.__module__}.{klass.__name__}".encode()
    except AttributeError:
        yield f"type:(typing.{klass._name}:(".encode()  # type: ignore
    args = ty.get_args(klass)
    if args:

        def sort_key(a):
            try:
                return a.__name__
            except AttributeError:
                return a._name

        yield b"["
        yield from bytes_repr_sequence_contents(sorted(args, key=sort_key), cache)
        yield b"]"
    yield b")"
