from __future__ import annotations
import typing as ty
from pathlib import Path
from pydra import mark
from arcana.core.data.space import DataSpace
from arcana.core.data.type.file import WithSideCars, BaseFile
from fileformats.common import Text
from arcana.core.mark import converter


class TestDataSpace(DataSpace):
    """Dummy data dimensions for ease of testing"""

    # Per dataset
    _ = 0b0000

    # Basis
    a = 0b1000
    b = 0b0100
    c = 0b0010
    d = 0b0001

    # Secondary combinations
    ab = 0b1100
    ac = 0b1010
    ad = 0b1001
    bc = 0b0110
    bd = 0b0101
    cd = 0b0011

    # Tertiary combinations
    abc = 0b1110
    abd = 0b1101
    acd = 0b1011
    bcd = 0b0111

    # Leaf rows
    abcd = 0b1111


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
