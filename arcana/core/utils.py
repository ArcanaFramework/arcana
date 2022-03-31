from typing import Sequence
import subprocess as sp
import importlib_metadata
import pkgutil
from enum import Enum
import re
from pathlib import Path
import packaging
from importlib import import_module
from inspect import isclass
from itertools import zip_longest
import pkg_resources
import os.path
# from nipype.interfaces.matlab import MatlabCommand
from contextlib import contextmanager
from collections.abc import Iterable
import logging
import attr
from arcana._version import __version__
from pydra.engine.task import FunctionTask
from pydra.engine.specs import BaseSpec, SpecInfo
from arcana.exceptions import ArcanaUsageError, ArcanaNameError, ArcanaVersionError


PATH_SUFFIX = '_path'
FIELD_SUFFIX = '_field'
CHECKSUM_SUFFIX = '_checksum'

ARCANA_HOME_DIR = Path.home() / '.arcana'

DOCKER_HUB = 'https://index.docker.io/v1/'
ARCANA_PIP = "git+ssh://git@github.com/australian-imaging-service/arcana.git"


def get_home_dir():
    try:
        home_dir = Path(os.environ['ARCANA_HOME'])
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
    return get_home_dir() / (name + '.yml')


def path2name(path):
    """Escape the name of an item by replacing '/' with a valid substring

    Parameters
    ----------
    path : str
        A path containing '/' characters that need to be escaped

    Returns
    -------
    str
        A python safe name
    """
    return PATH_SEP.join(str(path).split('/'))


def name2path(name):
    """Unescape a name created by path2name

    Parameters
    ----------
    name : str
        An escaped path

    Returns
    -------
    str
        The derived name
    """
    return '/'.join(name.split(PATH_SEP))


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
        input_spec=SpecInfo(
            name=f'{func_name}In', bases=(BaseSpec,), fields=in_fields),
        output_spec=SpecInfo(
            name=f'{func_name}Out', bases=(BaseSpec,), fields=out_fields),
        **inputs)


PATH_SEP = '__l__'


package_dir = os.path.join(os.path.dirname(__file__), '..')

try:
    HOSTNAME = sp.check_output('hostname').strip().decode('utf-8')
except sp.CalledProcessError:
    HOSTNAME = None
JSON_ENCODING = {'encoding': 'utf-8'}    

# def set_loggers(loggers):

#     # Overwrite earlier (default) versions of logger levels with later options
#     loggers = dict(loggers)

#     for name, level in loggers.items():
#         logger = logging.getLogger(name)
#         logger.setLevel(level)
#         handler = logging.StreamHandler()
#         formatter = logging.Formatter("%(levelname)s - %(message)s")
#         handler.setFormatter(formatter)
#         logger.addHandler(handler)


def set_loggers(loglevel, pydra_level='warning', depend_level='warning'):
    def parse(level):
        if isinstance(level, str):
            level = getattr(logging, level.upper())
        return level

    logging.getLogger("arcana").setLevel(parse(loglevel))
    logging.getLogger("pydra").setLevel(parse(pydra_level))

    # set logging format
    logging.basicConfig(level=parse(depend_level))


def to_list(arg):
    if arg is None:
        arg = []
    else:
        arg = list(arg)
    return arg


def to_dict(arg):
    if arg is None:
        arg = {}
    else:
        arg = dict(arg)
    return arg



def class_location(cls):
    """Records the location of a class so it can be loaded later using 
    `resolve_class`, in the format <module-name>:<class-name>"""
    if not isinstance(cls, type):
        cls = type(cls)  # Get the class rather than the object
    return cls.__module__ + ':' + cls.__name__


def resolve_class(class_str: str, prefixes: Sequence[str]=()) -> type:
    """
    Resolves a class from a location string in the format "<module-name>:<class-name>"

    Parameters
    ----------
    class_str : str
        Module path and name of class joined by ':', e.g. main_pkg.sub_pkg:MyClass
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
    module_path, class_name = class_str.split(':')
    cls = None
    for prefix in [None] + list(prefixes):
        if prefix is not None:
            mod_name = prefix + '.' + module_path
        else:
            mod_name = module_path
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


# def resolve_format(name):
#     """Resolves a in a sub-module of arcana.data.formats based on its
#     name

#     Parameters
#     ----------
#     name : str
#         The name of the format

#     Returns
#     -------
#     type
#         The resolved file format or type
#     """
#     if re.match(r'int|float|str|list\[(int|float|str)\]', name):
#         return eval(name)
#     import arcana.data.formats
#     import arcana.core.data.format
#     return resolve_subclass(arcana.data.formats,
#                         arcana.core.data.format.FileFormat, name)

def resolve_format(name):
    """Resolves a in a sub-module of arcana.file_format based on its
    name

    Parameters
    ----------
    name : str
        The name of the format

    Returns
    -------
    type
        The resolved file format or type
    """
    if re.match(r'int|float|str|list\[(int|float|str)\]', name):
        return eval(name)
    import arcana.data.formats
    data_format = None
    module_names = [
        i.name for i in pkgutil.iter_modules(
            [os.path.dirname(arcana.data.formats.__file__)])]
    for module_name in module_names:
        module = import_module('arcana.data.formats.' + module_name)
        try:
            data_format = getattr(module, name)
        except AttributeError:
            pass
    if data_format is None:
        raise ArcanaNameError(
            name,
            f"Could not find format {name} in installed modules:\n"
            + "\n    ".join(module_names))
    return data_format


def submodules(module):
    for mod_info in pkgutil.iter_modules([str(Path(module.__file__).parent)],
                                         prefix=module.__package__ + '.'):
        yield import_module(mod_info.name)


def list_subclasses(package, base_class):
    """List all available cmds in """
    subclasses = []
    for module in submodules(package):
        for obj_name in dir(module):
            obj = getattr(module, obj_name)
            if isclass(obj) and issubclass(obj, base_class) and obj is not base_class:
                subclasses.append(obj)
    return subclasses


def list_instances(package, cls):
    """List all available cmds in """
    instances = []
    for module in submodules(package):
        for obj_name in dir(module):
            obj = getattr(module, obj_name)
            if isinstance(obj, cls):
                instances.append(obj)
    return instances


def resolve_subclass(package, base_class, name):
    sub_class = None
    for module in submodules(package):
        try:
            sub_class = getattr(module, name)
        except AttributeError:
            pass
    if sub_class is None:
        raise ArcanaNameError(
            name,
            f"Could not find '{name}'' in {package.__package__} sub-modules:\n")
    if not issubclass(sub_class, base_class):
        raise ArcanaUsageError(
            f"{sub_class} is not a sub-class of {base_class}")
    return sub_class


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
        if path.name.endswith(double_ext):
            return str(path)[:-len(double_ext)], double_ext
    parts = path.name.split('.')
    if len(parts) == 1:
        base = path.name
        ext = None
    else:
        ext = '.' + parts[-1]
        base = '.'.join(parts[:-1])
    return path.parent / base, ext

def lower(s):
    if s is None:
        return None
    return s.lower()


def parse_single_value(value, format=None):
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
    if format is not None:
        value = format(value)
    return value


def parse_value(value, datatype=None):
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
        value = [parse_single_value(v, datatype=datatype) for v in value]
        # Check to see if datatypes are consistent
        datatypes = set(type(v) for v in value)
        if len(datatypes) > 1:
            raise ArcanaUsageError(
                "Inconsistent datatypes in values array ({})"
                .format(value))
    else:
        value = parse_single_value(value, datatype=datatype)
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
        module = import_module(package_name)
    except ImportError:
        if package_name.startswith('py'):
            try:
                module = import_module(package_name[2:])
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

class classproperty(object):
    def __init__(self, f):
        self.f = f
    def __get__(self, obj, owner):
        return self.f(owner)


def get_pkg_name(module_path: str):
    """Gets the name of the package that provides the given module

    Parameters
    ----------
    module_path
        The path to the module to retrieve the package for
    """
    if not isinstance(module_path, str):
        module_path = module_path.__module__
    module_path = importlib_metadata.PackagePath(module_path.replace('.', '/'))
    for pkg in pkg_resources.working_set:
        try:
            paths = importlib_metadata.files(pkg.key)
        except importlib_metadata.PackageNotFoundError:
            continue
        for path in paths:
            if path.suffix != '.py':
                continue
            path = path.with_suffix('')
            if path.name == '__init__':
                path = path.parent   
            if module_path in ([path] + list(path.parents)):
                return pkg.key
    raise ArcanaUsageError(f'{module_path} is not an installed module')


def parse_dimensions(dimensions_str):
    """Parse a string representation of DataSpace"""
    parts = dimensions_str.split('.')
    if len(parts) < 2:
        raise ArcanaUsageError(
            f"Value provided to '--dimensions' arg ({dimensions_str}) "
            "needs to include module, either relative to "
            "'arcana.dimensionss' (e.g. medimage.Clinical) or an "
            "absolute path")
    module_path = '.'.join(parts[:-1])
    cls_name = parts[-1]
    try:
        module = import_module('arcana.data.spaces.' + module_path)
    except ImportError:
        module = import_module(module_path)
    return getattr(module, cls_name)


def serialise(obj, skip=(), ignore_instance_method=False):
    """Serialises an object of a class defined with attrs to a dictionary

    Parameters
    ----------
    obj
        The Arcana object to serialised. Must be defined using the attrs
        decorator
    skip: Sequence[str]
        The names of attributes to skip"""

    if hasattr(obj, 'serialise') and not ignore_instance_method:
        serialised = obj.serialise()
    elif isclass(obj):
        serialised = '<' + class_location(obj) + '>'
    elif isinstance(obj, Enum):
        serialised = '|' + class_location(type(obj)) + '|' + str(obj)
    elif isinstance(obj, Path):
        serialised = str(obj)
    elif hasattr(obj, '__attrs_attrs__'):
        serialised = attr.asdict(
            obj,
            recurse=False,
            filter=lambda a, _: a.init and a.name not in skip and a.metadata.get('serialise', False),
            value_serializer=lambda _, __, v: serialise(v))
        serialised['type'] = '<' + class_location(obj) + '>'
        serialised['arcana_version'] = __version__
    elif not isinstance(obj, str) and isinstance(obj, Sequence):
        serialised = [serialise(x) for x in obj]
    elif isinstance(obj, dict):
        serialised = {k: serialise(v) for k, v in obj.items()}
    else:
        serialised = obj

    return serialised


def unserialise(serialised: dict, ignore_class_method=False, **kwargs):
    """Unserialises an object serialised by the `serialise` method from a
    dictionary

    Parameters
    ----------
    serialised : dict
        A dictionary containing a serialsed Arcana object such as a data store
        or dataset definition
    ignore_class_method: bool
        Ignore definition of `unserialised` classmethod in the unserialised class
    **kwargs : dict[str, Any]
        Additional initialisation arguments for the object when it is reinitialised.
        Overrides those stored"""
    if isinstance(serialised, dict) and 'type' in serialised:
        serialised_cls = resolve_class(serialised.pop('type')[1:-1])
        serialised_version = serialised.pop('arcana_version')
        if packaging.version.parse(serialised_version) < packaging.version.parse(MIN_SERIAL_VERSION):
            raise ArcanaVersionError(
                f"Serialised version ('{serialised_version}' is too old to be "
                f"read by this version of arcana ('{__version__}'), the minimum "
                f"version is {MIN_SERIAL_VERSION}")
        if hasattr(serialised_cls, 'unserialise') and not ignore_class_method:
            unserialised = serialised_cls.unserialise(serialised)
        else:
            init_args = {}
            for k, v in serialised.items():
                init_args[k] = unserialise(v)
            init_args.update(kwargs)
            unserialised = serialised_cls(**init_args)
    elif isinstance(serialised, list):
        unserialised = [unserialise(x) for x in serialised]
    elif isinstance(serialised, dict):
        unserialised = {k: unserialise(v) for k, v in serialised.items()}
    elif isinstance(serialised, str):
        if match:= re.match(r'<(.*)>', serialised): # Class location
            unserialised = resolve_class(match.group(1))
        elif match:= re.match(r'\|([^\|]+)\|(.*)', serialised):  # Enum
            unserialised = resolve_class(match.group(1))[match.group(2)]
        else:
            unserialised = serialised    
    else:
        unserialised = serialised
    return unserialised

# Minimum version of Arcana that this 
MIN_SERIAL_VERSION = '0.0.0'

