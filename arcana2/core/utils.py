from typing import Sequence
import subprocess as sp
import importlib
from itertools import zip_longest
import os.path
import errno
# from nipype.interfaces.matlab import MatlabCommand
import shutil
from contextlib import contextmanager
from collections.abc import Iterable
from importlib import import_module
import tempfile
import logging
from arcana2.exceptions import ArcanaUsageError


PATH_SUFFIX = '_path'
FIELD_SUFFIX = '_field'
CHECKSUM_SUFFIX = '_checksum'

package_dir = os.path.join(os.path.dirname(__file__), '..')

try:
    HOSTNAME = sp.check_output('hostname').strip().decode('utf-8')
except sp.CalledProcessError:
    HOSTNAME = None
JSON_ENCODING = {'encoding': 'utf-8'}    

def set_loggers(loggers):

    # Overwrite earlier (default) versions of logger levels with later options
    loggers = dict(loggers)

    for name, level in loggers.items():
        logger = logging.getLogger(name)
        logger.setLevel(level)
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)


def resolve_class(class_str: str, prefixes: Sequence[str]=()) -> type:
    """
    Resolves a class from the '.' delimted module + class name string

    Parameters
    ----------
    class_str : str
        Path to class preceded by module path, e.g. main_pkg.sub_pkg.MyClass
    prefixes : Sequence[str]
        List of allowable module prefixes to try to append if the fully
        resolved path fails, e.g. ['pydra.tasks'] would allow
        'fsl.preprocess.first.First' to resolve to
        pydra.tasks.fsl.preprocess.first.First

    Returns
    -------
    type:
        The resolved class
    """
    parts = class_str.split('.')
    module_name = '.'.join(parts[:-1])
    class_name = parts[-1]
    cls = None
    for prefix in [None] + list(prefixes):
        if prefix is not None:
            mod_name = prefix + '.' + module_name
        else:
            mod_name = module_name
        if not mod_name:
            continue
        mod_name = mod_name.strip('.')
        try:
            module = import_module(mod_name)
        except ModuleNotFoundError:
            continue
        else:
            try:
                cls = getattr(module, class_name)
            except AttributeError:
                continue
            else:
                break
    if cls is None:
        raise ArcanaUsageError(
            "Did not find class at '{}' or any sub paths of '{}'".format(
                class_str, "', '".join(prefixes)))
    return cls


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


double_exts = ('.tar.gz', '.nii.gz')


def split_extension(path):
    """
    A extension splitter that checks for compound extensions such as
    'file.nii.gz'

    Parameters
    ----------
    filename : str
        A filename to split into base and extension

    Returns
    -------
    base : str
        The base part of the string, i.e. 'file' of 'file.nii.gz'
    ext : str
        The extension part of the string, i.e. 'nii.gz' of 'file.nii.gz'
    """
    for double_ext in double_exts:
        if path.endswith(double_ext):
            return path[:-len(double_ext)], double_ext
    dirname = os.path.dirname(path)
    filename = os.path.basename(path)
    parts = filename.split('.')
    if len(parts) == 1:
        base = filename
        ext = None
    else:
        ext = '.' + parts[-1]
        base = '.'.join(parts[:-1])
    return os.path.join(dirname, base), ext


class classproperty(property):
    def __get__(self, cls, owner):
        return self.fget.__get__(None, owner)()  # pylint: disable=no-member


def lower(s):
    if s is None:
        return None
    return s.lower()




def parse_single_value(value, dtype=None):
    """
    Tries to convert to int, float and then gives up and assumes the value
    is of type string. Useful when excepting values that may be string
    representations of numerical values
    """
    if isinstance(value, str):
        try:
            if value.startswith('"') and value.endswith('"'):
                value = str(value[1:-1])
            elif '.' in value:
                value = float(value)
            else:
                value = int(value)
        except ValueError:
            value = str(value)
    elif not isinstance(value, (int, float, bool)):
        raise ArcanaUsageError(
            "Unrecognised type for single value {}".format(value))
    if dtype is not None:
        value = dtype(value)
    return value


def parse_value(value, dtype=None):
    # Split strings with commas into lists
    if isinstance(value, str):
        if value.startswith('[') and value.endswith(']'):
            value = value[1:-1].split(',')
    else:
        # Cast all iterables (except strings) into lists
        try:
            value = list(value)
        except TypeError:
            pass
    if isinstance(value, list):
        value = [parse_single_value(v, dtype=dtype) for v in value]
        # Check to see if datatypes are consistent
        dtypes = set(type(v) for v in value)
        if len(dtypes) > 1:
            raise ArcanaUsageError(
                "Inconsistent datatypes in values array ({})"
                .format(value))
    else:
        value = parse_single_value(value, dtype=dtype)
    return value


def iscontainer(*items):
    """
    Checks whether all the provided items are containers (i.e of class list,
    dict, tuple, etc...)
    """
    return all(isinstance(i, Iterable) and not isinstance(i, str)
               for i in items)


def find_mismatch(first, second, indent=''):
    """
    Finds where two objects differ, iterating down into nested containers
    (i.e. dicts, lists and tuples) They can be nested containers
    any combination of primary dtypes, str, int, float, dict and lists

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
    if not (isinstance(first, type(second)) or
            isinstance(second, type(first))):
        mismatch = (' types: self={} v other={}'
                    .format(type(first).__name__, type(second).__name__))
    elif not iscontainer(first, second):
        mismatch = ': self={} v other={}'.format(first, second)
    else:
        sub_indent = indent + '  '
        mismatch = ''
        if isinstance(first, dict):
            if sorted(first.keys()) != sorted(second.keys()):
                mismatch += (' keys: self={} v other={}'
                             .format(sorted(first.keys()),
                                     sorted(second.keys())))
            else:
                mismatch += ":"
                for k in first:
                    if first[k] != second[k]:
                        mismatch += ("\n{indent}'{}' values{}"
                                     .format(k,
                                             find_mismatch(first[k], second[k],
                                                           indent=sub_indent),
                                             indent=sub_indent))
        else:
            mismatch += ":"
            for i, (f, s) in enumerate(zip_longest(first, second)):
                if f != s:
                    mismatch += ("\n{indent}{} index{}"
                                 .format(i,
                                         find_mismatch(f, s,
                                                       indent=sub_indent),
                                         indent=sub_indent))
    return mismatch


def extract_package_version(package_name):
    version = None
    try:
        module = importlib.import_module(package_name)
    except ImportError:
        if package_name.startswith('py'):
            try:
                module = importlib.import_module(package_name[2:])
            except ImportError:
                pass
    else:
        try:
            version = module.__version__
        except AttributeError:
            pass
    return version


def get_class_info(cls):
    info = {'class': '{}.{}'.format(cls.__module__, cls.__name__)}
    version = extract_package_version(cls.__module__.split('.')[0])
    if version is not None:
        info['pkg_version'] = version
    return info


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
            "In order to wrap text, the indent cannot be larger than the "
            "line-length")
    while text:
        if len(text) > nchars:
            n = text[:nchars].rfind(' ')
            if n < 1:
                next_space = text[nchars:].find(' ')
                if next_space < 0:
                    # No spaces found
                    n = len(text)
                else:
                    n = nchars + next_space
        else:
            n = nchars
        lines.append(text[:n])
        text = text[(n + 1):]
    wrapped = '\n{}'.format(' ' * indent).join(lines)
    if prefix_indent:
        wrapped = ' ' * indent + wrapped
    return wrapped

