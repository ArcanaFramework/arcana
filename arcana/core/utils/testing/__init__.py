import os.path
import traceback


def show_cli_trace(result):
    return "".join(traceback.format_exception(*result.exc_info))


def make_dataset_locator(dataset, name=None):
    id_str = "file//" + os.path.abspath(dataset.id)
    if name is not None:
        id_str += "@" + name
    return id_str
