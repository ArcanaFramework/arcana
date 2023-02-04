import os.path
import traceback
from .fileformats import (
    Y,
    Xyz,
    MyFormat,
    MyFormatGz,
    MyFormatX,
    YourFormat,
    ImageWithHeader,
    MyFormatGzX,
    EncodedText,
)
from .space import TestDataSpace


def show_cli_trace(result):
    return "".join(traceback.format_exception(*result.exc_info))
