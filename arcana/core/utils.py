from dataclasses import is_dataclass, fields as dataclass_fields
from typing import Sequence
import subprocess as sp
import importlib_metadata
from itertools import chain
import pkgutil
import typing as ty
from enum import Enum
from copy import copy
import re
import inspect
import cloudpickle as cp
from pydra.engine.core import Workflow, LazyField, TaskBase
from pydra.engine.task import FunctionTask
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
from pydra import Workflow
from pydra.engine.task import FunctionTask, TaskBase
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


# def to_list(arg):
#     if arg is None:
#         arg = []
#     else:
#         arg = list(arg)
#     return arg


# def to_dict(arg):
#     if arg is None:
#         arg = {}
#     else:
#         arg = dict(arg)
#     return arg



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
    if class_str.startswith('<') and class_str.endswith('>'):
        class_str = class_str[1:-1]
    module_path, class_name = class_str.split(':')
    cls = None
    for prefix in [None] + list(prefixes):
        if prefix is not None:
            mod_name = prefix + ('.' if prefix[-1] != '.' else '') + module_path
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


def parse_value(value, format=None):
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
        value = [parse_single_value(v, format=format) for v in value]
        # Check to see if datatypes are consistent
        datatypes = set(type(v) for v in value)
        if len(datatypes) > 1:
            raise ArcanaUsageError(
                "Inconsistent datatypes in values array ({})"
                .format(value))
    else:
        value = parse_single_value(value, format=format)
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


def pkg_from_module(module: Sequence[str]):
    """Resolves the installed package (e.g. from PyPI) that provides the given
    module.

    Parameters
    ----------
    module: str or module or Sequence[str or module]
        a module or its import path string to retrieve the package for. Can be
        provided as a list of modules/strings, in which case a list of packages
        are returned

    Returns
    -------
    PackageInfo or list[PackageInfo]
        the package info object corresponding to the module. If `module`
        parameter is a list of modules/strings then a set of packages are
        returned
    """
    module_paths = set()
    if isinstance(module, Iterable) and not isinstance(module, str):
        modules = module
        as_tuple = True
    else:
        modules = [module]
        as_tuple = False
    for module in modules:
        try:
            module_path = module.__name__
        except AttributeError:
            module_path = module
        module_paths.add(
            importlib_metadata.PackagePath(module_path.replace('.', '/')))
    packages = set()
    for pkg in pkg_resources.working_set:
        try:
            paths = importlib_metadata.files(pkg.key)
        except importlib_metadata.PackageNotFoundError:
            continue
        match = False
        for path in paths:
            if path.suffix != '.py':
                continue
            path = path.with_suffix('')
            if path.name == '__init__':
                path = path.parent
            
            for module_path in copy(module_paths):
                if module_path in ([path] + list(path.parents)):
                    match = True
                    module_paths.remove(module_path)
        if match:
            packages.add(pkg)
            if not module_paths:  # If there are no more modules to find pkgs for
                break
    if module_paths:
        paths_str = "', '".join(module_paths)
        raise ArcanaUsageError(f'Did not find package for {paths_str}')
    return tuple(packages) if as_tuple else next(iter(packages))


def pkg_versions(modules):
    versions = {p.key: p.version for p in pkg_from_module(modules)}
    versions['arcana'] = __version__
    return versions


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


def as_dict(obj, omit: Iterable[str]=(), required_modules: set=None):
    """Serialises an object of a class defined with attrs to a dictionary

    Parameters
    ----------
    obj
        The Arcana object to as_dict. Must be defined using the attrs
        decorator
    omit: Iterable[str]
        the names of attributes to omit from the dictionary
    required_modules: set
        modules required to reload the serialised object into memory"""

    def filter(atr, value):
        return (atr.init and atr.metadata.get('serialise', True))

    if required_modules is None:
        required_modules = set()
        include_versions = True  # Assume top-level dictionary so need to include
    else:
        include_versions = False

    def serialise_class(klass):
        required_modules.add(klass.__module__)
        return '<' + class_location(klass) + '>'
    
    def value_as_dict(value):
        if isclass(value):
            value = serialise_class(value)
        elif hasattr(value, 'as_dict'):
            value = value.as_dict(required_modules=required_modules)
        elif attr.has(value):  # is class with attrs
            value_class = serialise_class(type(value))
            value = attr.asdict(
                value,
                recurse=False,
                filter=filter,
                value_serializer=lambda i, f, v: value_as_dict(v))
            value['class'] = value_class
        elif isinstance(value, Enum):
            value = serialise_class(type(value)) + '[' + str(value) + ']'
        elif isinstance(value, Path):
            value = 'file://' + str(value.resolve())
        elif isinstance(value, (tuple, list, set, frozenset)):
            value = [value_as_dict(x) for x in value]    
        elif isinstance(value, dict):
            value = {value_as_dict(k): value_as_dict(v) for k, v in value.items()}
        elif is_dataclass(value):
            value = [value_as_dict(getattr(value, f.name))
                     for f in dataclass_fields(value)]
        return value

    dct = attr.asdict(
        obj,
        recurse=False,
        filter=lambda a, v: filter(a, v) and a.name not in omit,
        value_serializer=lambda i, f, v: value_as_dict(v))

    dct['class'] = serialise_class(type(obj))
    if include_versions:
        dct['pkg_versions'] = pkg_versions(required_modules)

    return dct


def from_dict(dct: dict, **kwargs):
    """Unserialise an object from a dict created by the `as_dict` method

    Parameters
    ----------
    dct : dict
        A dictionary containing a serialsed Arcana object such as a data store
        or dataset definition
    omit: Iterable[str]
        key names to ignore when unserialising
    **kwargs : dict[str, Any]
        Additional initialisation arguments for the object when it is reinitialised.
        Overrides those stored"""
    try:
        arcana_version = dct['pkg_versions']['arcana']
    except KeyError:
        pass
    else:
        if packaging.version.parse(arcana_version) < packaging.version.parse(MIN_SERIAL_VERSION):
            raise ArcanaVersionError(
                f"Serialised version ('{arcana_version}' is too old to be "
                f"read by this version of arcana ('{__version__}'), the minimum "
                f"version is {MIN_SERIAL_VERSION}")

    def field_filter(klass, field_name):
        if attr.has(klass):
            return field_name in (f.name for f in attr.fields(klass))
        else:
            return field_name != 'class'

    def from_dict(value):
        if isinstance(value, dict):
            if 'class' in value:
                klass = resolve_class(value['class'])
                if hasattr(klass, 'from_dict'):
                    return klass.from_dict(value)
            value = {from_dict(k): from_dict(v) for k, v in value.items()}
            if 'class' in value:
                value = klass(**{k: v for k, v in value.items()
                                 if field_filter(klass, k)})
        elif isinstance(value, str):
            if match:= re.match(r'<(.*)>$', value): # Class location
                value = resolve_class(match.group(1))
            elif match:= re.match(r'<(.*)>\[(.*)\]$', value):  # Enum
                value = resolve_class(match.group(1))[match.group(2)]
            elif match:= re.match(r'file://(.*)', value):
                value = Path(match.group(1))
        elif isinstance(value, Sequence):
            value = [from_dict(x) for x in value]
        return value

    klass = resolve_class(dct['class'])

    kwargs.update({k: from_dict(v) for k, v in dct.items()
                   if field_filter(klass, k) and k not in kwargs})

    return klass(**kwargs)


extract_import_re = re.compile(r'\s*(?:from|import)\s+([\w\.]+)')


def pydra_as_dict(obj: TaskBase, required_modules: ty.Set[str],
                  workflow: Workflow=None) -> dict:
    """Converts a Pydra Task/Workflow into a dictionary that can be serialised

    Parameters
    ----------
    obj : pydra.engine.core.TaskBase
        the Pydra object to convert to a dictionary
    required_modules : set[str]
        a set of modules that are required to load the pydra object back
        out from disk and run it
    workflow : pydra.Workflow, optional
        the containing workflow that the object to serialised is part of

    Returns
    -------
    dict
        the dictionary containing the contents of the Pydra object
    """
    dct = {'name': obj.name,
           'class': '<' + class_location(obj) + '>'}
    if isinstance(obj, Workflow):
        dct['nodes'] = [pydra_as_dict(n, required_modules=required_modules,
                                      workflow=obj)
                        for n in obj.nodes]
        dct['outputs'] = outputs = {}
        for outpt_name, lf in obj._connections:
            outputs[outpt_name] = {"task": lf.name, "field": lf.field}
    else:
        if isinstance(obj, FunctionTask):
            func = cp.loads(obj.inputs._func)
            module = inspect.getmodule(func)
            dct['class'] = '<' + module.__name__ + ':' + func.__name__ + '>'
            required_modules.add(module.__name__)
            # inspect source for any import lines (should be present in function
            # not module)
            for line in inspect.getsourcelines(func)[0]:
                if match:= extract_import_re.match(line):
                    required_modules.add(match.group(1))
            # TODO: check source for references to external modules that aren't
            #       imported within function
        elif type(obj).__module__ != 'pydra.engine.task':
            pkg = pkg_from_module(type(obj).__module__)
            dct['package'] = pkg.key
            dct['version'] = pkg.version
        if hasattr(obj, 'container'):
            dct['container'] = {"type": obj.container,
                                "image": obj.image}
    dct['inputs'] = inputs = {} 
    for inpt_name in obj.input_names:
        if not inpt_name.startswith('_'):
            inpt_value = getattr(obj.inputs, inpt_name)
            if isinstance(inpt_value, LazyField):
                inputs[inpt_name] = {'pydra_field': inpt_value.field}
                # If the lazy field comes from the workflow lazy in, we omit
                # the 'task' item
                if workflow is None or inpt_value.name != workflow.name:
                    inputs[inpt_name]['task'] = inpt_value.name
                                     
            elif inpt_value == attr.NOTHING:
                inputs[inpt_name] = '__NOTHING__'
            else:
                inputs[inpt_name] = inpt_value
    return dct


def pydra_from_dict(dct: dict, name: str=None, workflow: Workflow=None,
                    inputs_from_upstream: ty.List[str]=None,
                    **kwargs) -> TaskBase:
    """Recreates a Pydra Task/Workflow from a dictionary object created by
    `pydra_as_dict`

    Parameters
    ----------
    dct : dict
        dictionary representations of the object to recreate
    name : str
        name to give the object
    workflow : pydra.Workflow, optional
        the containing workflow that the object to recreate is connected to
    inputs_from_upstream: list[str]
        additional inputs to set in the input_spec of the Pydra object, which
        were dropped from the dictionary because they hadn't been connected
        yet

    Returns
    -------
    pydra.engine.core.TaskBase
        the recreated Pydra object
    """
    from arcana.core.pipeline import Pipeline
    if name is None:
        name = dct['name']
    klass = resolve_class(dct['class'])
    # Resolve lazy-field references to workflow fields
    inputs = {}
    for inpt_name, inpt_val in dct['inputs'].items():
        # Check for 'pydra_field' key in a dictionary val and convert to a
        # LazyField object
        if isinstance(inpt_val, dict) and 'pydra_field' in inpt_val:
            if 'task' in inpt_val:
                inpt_task = getattr(workflow, inpt_val['task'])
                inpt_val = getattr(inpt_task.inputs, inpt_val['pydra_field'])
            else:
                inpt_val = getattr(workflow.lzin, inpt_val['pydra_field'])
        inputs[inpt_name] = inpt_val
    if klass is Workflow:
        obj = Workflow(
            name=name,
            input_spec=list(dct['inputs']) + inputs_from_upstream,
            **inputs)
        for node_dict in dct['nodes']:
            obj.add(pydra_from_dict(node_dict, workflow=obj))
        obj.set_output(dct['outputs'].items())
    else:
        obj = klass(name=name, **inputs)
    return obj


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
    if a.input_names != b.input_names:
        return False
    if a.output_spec != b.output_spec:
        return False
    for inpt_name in a.input_names:
        if getattr(a.inputs, inpt_name) != getattr(b.inputs, inpt_name):
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


# Minimum version of Arcana that this version can read the serialisation from
MIN_SERIAL_VERSION = '0.0.0'
