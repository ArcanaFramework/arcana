from __future__ import annotations
import subprocess as sp
import typing as ty
import re
from itertools import zip_longest
from pathlib import Path, PosixPath
import tempfile
import tarfile
import logging
import docker
import os.path
from contextlib import contextmanager
from collections.abc import Iterable
import cloudpickle as cp
from pydra.engine.core import Workflow, LazyField, TaskBase
from pydra.engine.task import FunctionTask
from pydra.engine.specs import BaseSpec, SpecInfo
from arcana.core.exceptions import ArcanaUsageError


logger = logging.getLogger("arcana")


PIPELINE_ANNOTATIONS = "__arcana_pipeline__"
CONVERTER_ANNOTATIONS = "__arcana_converter__"
SWICTH_ANNOTATIONS = "__arcana_switch__"
CHECK_ANNOTATIONS = "__arcana_check__"

ARCANA_SPEC = "__arcana_type__"


PATH_SUFFIX = "_path"
FIELD_SUFFIX = "_field"
CHECKSUM_SUFFIX = "_checksum"

ARCANA_HOME_DIR = Path.home() / ".arcana"

ARCANA_PIP = "git+ssh://git@github.com/australian-imaging-service/arcana.git"

HASH_CHUNK_SIZE = 2**20  # 1MB in calc. checksums to avoid mem. issues


def get_home_dir():
    try:
        home_dir = Path(os.environ["ARCANA_HOME"])
    except KeyError:
        home_dir = ARCANA_HOME_DIR
    if not home_dir.exists():
        home_dir.mkdir()
    return home_dir


def get_config_file_path(name: str):
    """Gets the file path for the configuration file corresponding to `name`

    Parameters
    ----------
    name
        Name of the configuration file to return

    Returns
    -------
    Path
        Path to configuration file
    """
    return get_home_dir() / (name + ".yaml")


# Escape values for invalid characters for Python variable names
PATH_ESCAPES = {
    "_": "_u_",
    "/": "__l__",
    ".": "__o__",
    " ": "__s__",
    "\t": "__t__",
    ",": "__comma__",
    ">": "__gt__",
    "<": "__lt__",
    "-": "__H__",
    "'": "__singlequote__",
    '"': "__doublequote__",
    "(": "__openparens__",
    ")": "__closeparens__",
    "[": "__openbracket__",
    "]": "__closebracket__",
    "{": "__openbrace__",
    "}": "__closebrace__",
    ":": "__colon__",
    ";": "__semicolon__",
    "`": "__tick__",
    "~": "__tilde__",
    "|": "__pipe__",
    "?": "__question__",
    "\\": "__backslash__",
    "$": "__dollar__",
    "@": "__at__",
    "!": "__exclaimation__",
    "#": "__pound__",
    "%": "__percent__",
    "^": "__caret__",
    "&": "__ampersand__",
    "*": "__star__",
    "+": "__plus__",
    "=": "__equals__",
    "XXX": "__tripplex__",
}

PATH_NAME_PREFIX = "XXX"

EMPTY_PATH_NAME = "__empty__"


def path2varname(path):
    """Escape a string (typically a file-system path) so that it can be used as a Python
    variable name by replacing non-valid characters with escape sequences in PATH_ESCAPES.

    Parameters
    ----------
    path : str
        A path containing '/' characters that need to be escaped

    Returns
    -------
    str
        A python safe name
    """
    if not path:
        name = EMPTY_PATH_NAME
    else:
        name = path
        for char, esc in PATH_ESCAPES.items():
            name = name.replace(char, esc)
    if name.startswith("_"):
        name = PATH_NAME_PREFIX + name
    return name


def varname2path(name):
    """Unescape a Pythonic name created by `path2varname`

    Parameters
    ----------
    name : str
        the escaped path

    Returns
    -------
    str
        the original path
    """
    if name.startswith(PATH_NAME_PREFIX):
        path = name[len(PATH_NAME_PREFIX) :]
    else:
        path = name  # strip path-name prefix
    if path == EMPTY_PATH_NAME:
        return ""
    # the order needs to be reversed so that "dunder" (double underscore) is
    # unescaped last
    for char, esc in reversed(PATH_ESCAPES.items()):
        path = path.replace(esc, char)
    return path


def func_task(func, in_fields, out_fields, **inputs):
    """Syntactic sugar for creating a FunctionTask

    Parameters
    ----------
    func : Callable
        The function to wrap
    input_fields : ty.List[ty.Tuple[str, type]]
        The list of input fields to create for the task
    output_fields : ty.List[ty.Tuple[str, type]]
        The list of output fields to create for the task
    **inputs
        Inputs to set for the task

    Returns
    -------
    pydra.FunctionTask
        The wrapped task"""
    func_name = func.__name__.capitalize()
    return FunctionTask(
        func,
        input_spec=SpecInfo(name=f"{func_name}In", bases=(BaseSpec,), fields=in_fields),
        output_spec=SpecInfo(
            name=f"{func_name}Out", bases=(BaseSpec,), fields=out_fields
        ),
        **inputs,
    )


def set_loggers(loglevel, pydra_level="warning", depend_level="warning"):
    """Sets loggers for arcana and pydra. To be used in CLI

    Parameters
    ----------
    loglevel : str
        the threshold to produce logs at (e.g. debug, info, warning, error)
    pydra_level : str, optional
        the threshold to produce logs from Pydra at
    depend_level : str, optional
        the threshold to produce logs in dependency packages
    """

    def parse(level):
        if isinstance(level, str):
            level = getattr(logging, level.upper())
        return level

    logging.getLogger("arcana").setLevel(parse(loglevel))
    logging.getLogger("pydra").setLevel(parse(pydra_level))

    # set logging format
    logging.basicConfig(level=parse(depend_level))


@contextmanager
def set_cwd(path):
    """Sets the current working directory to `path` and back to original
    working directory on exit

    Parameters
    ----------
    path : str
        The file system path to set as the current working directory
    """
    pwd = os.getcwd()
    os.chdir(path)
    try:
        yield path
    finally:
        os.chdir(pwd)


def dir_modtime(dpath):
    """
    Returns the latest modification time of all files/subdirectories in a
    directory
    """
    return max(os.path.getmtime(d) for d, _, _ in os.walk(dpath))


def iscontainer(*items):
    """
    Checks whether all the provided items are containers (i.e of class list,
    dict, tuple, etc...)
    """
    return all(isinstance(i, Iterable) and not isinstance(i, str) for i in items)


def find_mismatch(first, second, indent=""):
    """
    Finds where two objects differ, iterating down into nested containers
    (i.e. dicts, lists and tuples) They can be nested containers
    any combination of primary formats, str, int, float, dict and lists

    Parameters
    ----------
    first : dict | list | tuple | str | int | float
        The first object to compare
    second : dict | list | tuple | str | int | float
        The other object to compare with the first
    indent : str
        The amount newlines in the output string should be indented. Provide
        the actual indent, i.e. a string of spaces.

    Returns
    -------
    mismatch : str
        Human readable output highlighting where two container differ.
    """

    # Basic case where we are dealing with non-containers
    if not (isinstance(first, type(second)) or isinstance(second, type(first))):
        mismatch = " types: self={} v other={}".format(
            type(first).__name__, type(second).__name__
        )
    elif not iscontainer(first, second):
        mismatch = ": self={} v other={}".format(first, second)
    else:
        sub_indent = indent + "  "
        mismatch = ""
        if isinstance(first, dict):
            if sorted(first.keys()) != sorted(second.keys()):
                mismatch += " keys: self={} v other={}".format(
                    sorted(first.keys()), sorted(second.keys())
                )
            else:
                mismatch += ":"
                for k in first:
                    if first[k] != second[k]:
                        mismatch += "\n{indent}'{}' values{}".format(
                            k,
                            find_mismatch(first[k], second[k], indent=sub_indent),
                            indent=sub_indent,
                        )
        else:
            mismatch += ":"
            for i, (f, s) in enumerate(zip_longest(first, second)):
                if f != s:
                    mismatch += "\n{indent}{} index{}".format(
                        i, find_mismatch(f, s, indent=sub_indent), indent=sub_indent
                    )
    return mismatch


def wrap_text(text, line_length, indent, prefix_indent=False):
    """
    Wraps a text block to the specified line-length, without breaking across
    words, using the specified indent to join the lines

    Parameters
    ----------
    text : str
        The text to wrap
    line_length : int
        The desired line-length for the wrapped text (including indent)
    indent : int
        The number of spaces to use as an indent for the wrapped lines
    prefix_indent : bool
        Whether to prefix the indent to the wrapped text

    Returns
    -------
    wrapped : str
        The wrapped text
    """
    lines = []
    nchars = line_length - indent
    if nchars <= 0:
        raise ArcanaUsageError(
            "In order to wrap text, the indent cannot be larger than the " "line-length"
        )
    while text:
        if len(text) > nchars:
            n = text[:nchars].rfind(" ")
            if n < 1:
                next_space = text[nchars:].find(" ")
                if next_space < 0:
                    # No spaces found
                    n = len(text)
                else:
                    n = nchars + next_space
        else:
            n = nchars
        lines.append(text[:n])
        text = text[(n + 1) :]
    wrapped = "\n{}".format(" " * indent).join(lines)
    if prefix_indent:
        wrapped = " " * indent + wrapped
    return wrapped


class classproperty(object):
    def __init__(self, f):
        self.f = f

    def __get__(self, obj, owner):
        return self.f(owner)


extract_import_re = re.compile(r"\s*(?:from|import)\s+([\w\.]+)")

NOTHING_STR = "__PIPELINE_INPUT__"


def pydra_eq(a: TaskBase, b: TaskBase):
    """Compares two Pydra Task/Workflows for equality

    Parameters
    ----------
    a : pydra.engine.core.TaskBase
        first object to compare
    b : pydra.engine.core.TaskBase
        second object to compare

    Returns
    -------
    bool
        whether the two objects are equal
    """
    if type(a) != type(b):
        return False
    if a.name != b.name:
        return False
    if sorted(a.input_names) != sorted(b.input_names):
        return False
    if a.output_spec.fields != b.output_spec.fields:
        return False
    for inpt_name in a.input_names:
        a_input = getattr(a.inputs, inpt_name)
        b_input = getattr(b.inputs, inpt_name)
        if isinstance(a_input, LazyField):
            if a_input.field != b_input.field or a_input.name != b_input.name:
                return False
        elif a_input != b_input:
            return False
    if isinstance(a, Workflow):
        a_node_names = [n.name for n in a.nodes]
        b_node_names = [n.name for n in b.nodes]
        if a_node_names != b_node_names:
            return False
        for node_name in a_node_names:
            if not pydra_eq(getattr(a, node_name), getattr(b, node_name)):
                return False
    else:
        if isinstance(a, FunctionTask):
            if a.inputs._func != b.inputs._func:
                return False
    return True


def show_workflow_errors(
    pipeline_cache_dir: Path, omit_nodes: ty.List[str] = None
) -> str:
    """Extract nodes with errors and display results

    Parameters
    ----------
    pipeline_cache_dir : Path
        the path container the pipeline cache directories
    omit_nodes : list[str], optional
        The names of the nodes to omit from the error message

    Returns
    -------
    str
        a string displaying the error messages
    """
    # PKL_FILES = ["_task.pklz", "_result.pklz", "_error.pklz"]
    out_str = ""

    def load_contents(fpath):
        contents = None
        if fpath.exists():
            with open(fpath, "rb") as f:
                contents = cp.load(f)
        return contents

    for path in pipeline_cache_dir.iterdir():
        if not path.is_dir():
            continue
        if "_error.pklz" in [p.name for p in path.iterdir()]:
            task = load_contents(path / "_task.pklz")
            if task.name in omit_nodes:
                continue
            if task:
                out_str += f"{task.name} ({type(task)}):\n"
                out_str += "    inputs:"
                for inpt_name in task.input_names:
                    out_str += (
                        f"\n        {inpt_name}: {getattr(task.inputs, inpt_name)}"
                    )
                try:
                    out_str += "\n\n    cmdline: " + task.cmdline
                except Exception:
                    pass
            else:
                out_str += "Anonymous task:\n"
            error = load_contents(path / "_error.pklz")
            out_str += "\n\n    errors:\n"
            for k, v in error.items():
                if k == "error message":
                    indent = "            "
                    out_str += (
                        "        message:\n"
                        + indent
                        + "".join(ln.replace("\n", "\n" + indent) for ln in v)
                    )
                else:
                    out_str += f"        {k}: {v}\n"
    return out_str


def extract_file_from_docker_image(
    image_tag, file_path: PosixPath, out_path: Path = None
) -> Path:
    """Extracts a file from a Docker image onto the local host

    Parameters
    ----------
    image_tag : str
        the name/tag of the image to extract the file from
    file_path : PosixPath
        the path to the file inside the image

    Returns
    -------
    Path or None
        path to the extracted file or None if image doesn't exist
    """
    tmp_dir = Path(tempfile.mkdtemp())
    if out_path is None:
        out_path = tmp_dir / "extracted-dir"
    dc = docker.from_env()
    try:
        dc.api.pull(image_tag)
    except docker.errors.APIError as e:
        if e.response.status_code in (404, 500):
            return None
        else:
            raise
    else:
        container = dc.containers.get(dc.api.create_container(image_tag)["Id"])
        try:
            tarfile_path = tmp_dir / "tar-file.tar.gz"
            with open(tarfile_path, mode="w+b") as f:
                try:
                    stream, _ = dc.api.get_archive(container.id, str(file_path))
                except docker.errors.NotFound:
                    pass
                else:
                    for chunk in stream:
                        f.write(chunk)
                    f.flush()
        finally:
            container.remove()
        with tarfile.open(tarfile_path) as f:
            f.extractall(out_path)
    return out_path


def add_exc_note(e, note):
    """Adds a note to an exception in a Python <3.11 compatible way

    Parameters
    ----------
    e : Exception
        the exception to add the note to
    note : str
        the note to add

    Returns
    -------
    Exception
        returns the exception again
    """
    if hasattr(e, "add_note"):
        e.add_note(note)
    else:
        e.args = (e.args[0] + "\n" + note,)
    return e


# Minimum version of Arcana that this version can read the serialisation from
MIN_SERIAL_VERSION = "0.0.0"


DOCKER_HUB = "docker.io"

# Global flag to allow references to classes to be missing from the


package_dir = os.path.join(os.path.dirname(__file__), "..")

try:
    HOSTNAME = sp.check_output("hostname").strip().decode("utf-8")
except sp.CalledProcessError:
    HOSTNAME = None
JSON_ENCODING = {"encoding": "utf-8"}
