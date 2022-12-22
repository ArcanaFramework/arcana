from __future__ import annotations
import typing as ty
from pathlib import Path
from pydra import mark
from arcana.core.data.type.file import WithSideCars, BaseFile
from fileformats.common import Text
from arcana.core.mark import converter


class Xyz(WithSideCars):

    ext = "x"
    side_car_exts = ("y", "z")


class Nifti(BaseFile):

    ext = "nii"


class NiftiGz(Nifti):

    ext = "nii.gz"


class NiftiX(WithSideCars, Nifti):

    side_car_exts = ("json",)


class MrtrixImage(BaseFile):

    ext = "mif"


class Analyze(WithSideCars, BaseFile):

    ext = "img"
    side_car_exts = ("hdr",)


class NiftiGzX(NiftiX, NiftiGz):

    pass


class EncodedText(BaseFile):
    """A text file where the characters ASCII codes are shifted on conversion
    from text
    """

    ext = "enc"

    @classmethod
    @converter(Text)
    def encode(cls, fs_path: ty.Union[str, Path], shift: int = 0):
        shift = int(shift)
        node = encoder_task(in_file=fs_path, shift=shift)
        return node, node.lzout.out


class DecodedText(Text):
    @classmethod
    @converter(EncodedText)
    def decode(cls, fs_path: Path, shift: int = 0):
        shift = int(shift)
        node = encoder_task(
            in_file=fs_path, shift=-shift, out_file="out_file.txt"
        )  # Just shift it backwards by the same amount
        return node, node.lzout.out


@mark.task
def encoder_task(
    in_file: ty.Union[str, Path],
    shift: int,
    out_file: ty.Union[str, Path] = "out_file.enc",
) -> ty.Union[str, Path]:
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
