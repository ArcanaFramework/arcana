import os.path
import traceback


def show_cli_trace(result):
    return "".join(traceback.format_exception(*result.exc_info))
