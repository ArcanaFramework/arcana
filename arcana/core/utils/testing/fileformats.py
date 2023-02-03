from __future__ import annotations
from pathlib import Path
from pydra import mark
from fileformats.generic import File
from fileformats.core.mixin import WithSideCar
from fileformats.text import Plain as Text
from fileformats.core.mark import converter


class Y(File):
    ext = "..y"


class Xyz(WithSideCar):

    ext = ".x"
    side_car_type = Y


class MyFormat(File):

    ext = ".my"


class MyFormatGz(MyFormat):

    ext = ".my.gz"


class MyFormatX(WithSideCar, MyFormat):

    side_car_exts = ("json",)


class YourFormat(File):

    ext = ".yr"


class ImageWithHeader(WithSideCar, File):

    ext = ".img"
    side_car_exts = ("hdr",)


class MyFormatGzX(MyFormatX, MyFormatGz):

    pass


class EncodedText(File):
    """A text file where the characters ASCII codes are shifted on conversion
    from text
    """

    ext = ".enc"


# @converter(Text)
# def encode(cls, fspath: ty.Union[str, Path], shift: int = 0):
#     shift = int(shift)
#     node = encoder_task(in_file=fspath, shift=shift)
#     return node, node.lzout.out


# @converter(EncodedText)
# def decode(cls, fspath: Path, shift: int = 0):
#     shift = int(shift)
#     node = encoder_task(
#         in_file=fspath, shift=-shift, out_file="out_file.txt"
#     )  # Just shift it backwards by the same amount
#     return node, node.lzout.out


@converter(source_format=EncodedText, target_format=Text, out_file="out_file.txt")
@converter(source_format=Text, target_format=EncodedText, out_file="out_file.enc")
@mark.task
def encoder_task(
    in_file: File,
    out_file: str,
    shift: int = 0,
) -> File:
    with open(in_file) as f:
        contents = f.read()
    encoded = encode_text(contents, shift)
    with open(out_file, "w") as f:
        f.write(encoded)
    return Path(out_file).absolute()


def encode_text(text: str, shift: int) -> str:
    encoded = []
    for c in text:
        encoded.append(chr(ord(c) + shift))
    return "".join(encoded)
