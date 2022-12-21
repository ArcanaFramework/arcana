from __future__ import annotations
from dataclasses import is_dataclass, fields as dataclass_fields
from typing import Sequence
import typing as ty
from enum import Enum
import builtins
from copy import copy
import json
import re
import inspect
from importlib import import_module
from inspect import isclass, isfunction
from pathlib import PurePath, Path
import logging
import cloudpickle as cp
import attrs
from pydra.engine.core import Workflow, LazyField, TaskBase
from pydra.engine.task import FunctionTask
from arcana.core.exceptions import ArcanaUsageError
from .packaging import pkg_versions, package_from_module
from .misc import add_exc_note


logger = logging.getLogger("arcana")


ARCANA_PIP = "git+ssh://git@github.com/australian-imaging-service/arcana.git"

HASH_CHUNK_SIZE = 2**20  # 1MB in calc. checksums to avoid mem. issues


@attrs.define
class _FallbackContext:
    """Used to specify that class resolution is permitted to fail within this context
    and return just a string (i.e. in build environments where the required modules
    aren't installed)
    """

    permit: bool = False

    def __enter__(self):
        self.permit = True

    def __exit__(self, exception_type, exception_value, traceback):
        self.permit = False


@attrs.define
class ClassResolver:
    """
    Parameters
    ----------
    base_class : type
        the target class to resolve the string representation to
    prefixes : Sequence[str]
        List of allowable module prefixes to try to append if the fully
        resolved path fails, e.g. ['pydra.tasks'] would allow
        'fsl.preprocess.first.First' to resolve to
        pydra.tasks.fsl.preprocess.first.First
    """

    base_class: type = None
    allow_none: bool = False
    alternative_types: list[type] = attrs.field(factory=list)

    def __call__(self, class_str: str) -> type:
        """
        Resolves a class from a location string in the format "<module-name>:<class-name>"

        Parameters
        ----------
        class_str : str
            Module path and name of class joined by ':', e.g. main_pkg.sub_pkg:MyClass

        Returns
        -------
        type:
            The resolved class
        """
        if class_str is None and self.allow_none:
            return None
        klass = self.fromstr(class_str, subpkg=self._get_subpkg(self.base_class))
        self._check_type(klass)
        return klass

    @classmethod
    def _get_subpkg(cls, klass):
        try:
            return klass.SUBPACKAGE
        except AttributeError:
            return None

    @classmethod
    def fromstr(cls, class_str, subpkg=None):
        """Resolves a class/function from a string containing its module an its name
        separated by a ':'

        Parameters
        ----------
        class_str : str
            the string representation to resolve to a class or function
        subpkg : str, optional
            the sub-package that the class should belong to within the extension

        Returns
        -------
        type or callable
            the resolved class or function

        Raises
        ------
        ValueError
            raised if the string doesn't contain a ':'
        ArcanaUsageError
            raised if the class wasn't found in the sub-package
        ArcanaUsageError
            raised if a sub-package couldn't be found
        """
        if not isinstance(class_str, str):
            return class_str  # Assume that it is already resolved
        if class_str.startswith("<") and class_str.endswith(">"):
            class_str = class_str[1:-1]
        try:
            module_path, class_name = class_str.split(":")
        except ValueError:
            try:
                return getattr(builtins, class_str)
            except AttributeError:
                raise ValueError(
                    f"Class location '{class_str}' should contain a ':' unless it is in the "
                    "builtins module"
                ) from None

        if "." in module_path:
            # Interpret as an absolute path not a relative path from an extension
            module_path = module_path.rstrip(
                "."
            )  # trailing '.' signifies top-level pkg
            subpkg = None
        module = None

        if subpkg:
            full_mod_path = ".".join(("arcana", module_path, subpkg))
        else:
            full_mod_path = module_path
        try:
            module = import_module(full_mod_path)
        except ModuleNotFoundError:
            if cls.FALLBACK_TO_STR.permit:
                return class_str
            else:
                raise ArcanaUsageError(
                    f"Did not find module {full_mod_path}' when resolving {class_str} "
                    f"with subpkg={subpkg}"
                )
        try:
            klass = getattr(module, class_name)
        except AttributeError:
            raise ArcanaUsageError(
                f"Did not find '{class_str}' class/function in module '{module.__name__}'"
            )
        return klass

    @classmethod
    def tostr(cls, klass, strip_prefix: bool = True):
        """Records the location of a class so it can be loaded later using
        `ClassResolver`, in the format <module-name>:<class-name>

        Parameters
        ----------
        klass : Any
            the class/function to serialise to a string
        strip_prefix : bool
            whether to strip the SUBPACKAGE prefix from the module path when writing
            to file
        """
        if isinstance(klass, str):
            return klass
        if not (isclass(klass) or isfunction(klass)):
            klass = type(klass)  # Get the class rather than the object
        module_name = klass.__module__
        if module_name == "builtins":
            return klass.__name__
        if strip_prefix and cls._get_subpkg(klass):
            subpkg = cls._get_subpkg(klass)
            if match := re.match(r"arcana\.(\w+)\." + subpkg, module_name):
                module_name = match.group(
                    1
                )  # just use the name of the extension module
            elif "." not in module_name:
                module_name += "."  # To distinguish it from extension module name
        return module_name + ":" + klass.__name__

    def _check_type(self, klass):
        if self.FALLBACK_TO_STR.permit and isinstance(klass, str):
            return
        if self.base_class:
            if isfunction(klass):
                if ty.Callable in self.alternative_types:
                    return  # ok
                else:
                    raise ValueError(
                        f"Found callable {klass}, but Callable isn't in alternative_types"
                    )
            if klass in self.alternative_types:
                return  # ok
            if not isclass(klass) or not issubclass(klass, self.base_class):
                raise ValueError(
                    f"Found {klass}, which is not a subclass of {self.base_class}"
                )

    FALLBACK_TO_STR = _FallbackContext()


def asdict(obj, omit: ty.Iterable[str] = (), required_modules: set = None):
    """Serialises an object of a class defined with attrs to a dictionary

    Parameters
    ----------
    obj
        The Arcana object to asdict. Must be defined using the attrs
        decorator
    omit: Iterable[str]
        the names of attributes to omit from the dictionary
    required_modules: set
        modules required to reload the serialised object into memory"""

    def filter(atr, value):
        return atr.init and atr.metadata.get("asdict", True)

    if required_modules is None:
        required_modules = set()
        include_versions = True  # Assume top-level dictionary so need to include
    else:
        include_versions = False

    def serialise_class(klass):
        required_modules.add(klass.__module__)
        return "<" + ClassResolver.tostr(klass, strip_prefix=False) + ">"

    def value_asdict(value):
        if isclass(value):
            value = serialise_class(value)
        elif hasattr(value, "asdict"):
            value = value.asdict(required_modules=required_modules)
        elif attrs.has(value):  # is class with attrs
            value_class = serialise_class(type(value))
            value = attrs.asdict(
                value,
                recurse=False,
                filter=filter,
                value_serializer=lambda i, f, v: value_asdict(v),
            )
            value["class"] = value_class
        elif isinstance(value, Enum):
            value = serialise_class(type(value)) + "[" + str(value) + "]"
        elif isinstance(value, PurePath):
            value = "file://" + str(value.resolve())
        elif isinstance(value, (tuple, list, set, frozenset)):
            value = [value_asdict(x) for x in value]
        elif isinstance(value, dict):
            value = {value_asdict(k): value_asdict(v) for k, v in value.items()}
        elif is_dataclass(value):
            value = [
                value_asdict(getattr(value, f.name)) for f in dataclass_fields(value)
            ]
        return value

    dct = attrs.asdict(
        obj,
        recurse=False,
        filter=lambda a, v: filter(a, v) and a.name not in omit,
        value_serializer=lambda i, f, v: value_asdict(v),
    )

    dct["class"] = serialise_class(type(obj))
    if include_versions:
        dct["pkg_versions"] = pkg_versions(required_modules)

    return dct


def fromdict(dct: dict, **kwargs):
    """Unserialise an object from a dict created by the `asdict` method

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
    # try:
    #     arcana_version = dct["pkg_versions"]["arcana"]
    # except (TypeError, KeyError):
    #     pass
    #     else:
    #         if packaging.version.parse(arcana_version) < packaging.version.parse(MIN_SERIAL_VERSION):
    #             raise ArcanaVersionError(
    #                 f"Serialised version ('{arcana_version}' is too old to be "
    #                 f"read by this version of arcana ('{__version__}'), the minimum "
    #                 f"version is {MIN_SERIAL_VERSION}")

    def field_filter(klass, field_name):
        if attrs.has(klass):
            return field_name in (f.name for f in attrs.fields(klass))
        else:
            return field_name != "class"

    def fromdict(value):
        if isinstance(value, dict):
            if "class" in value:
                klass = ClassResolver()(value["class"])
                if hasattr(klass, "fromdict"):
                    return klass.fromdict(value)
            value = {fromdict(k): fromdict(v) for k, v in value.items()}
            if "class" in value:
                value = klass(
                    **{k: v for k, v in value.items() if field_filter(klass, k)}
                )
        elif isinstance(value, str):
            if match := re.match(r"<(.*)>$", value):  # Class location
                value = ClassResolver()(match.group(1))
            elif match := re.match(r"<(.*)>\[(.*)\]$", value):  # Enum
                value = ClassResolver()(match.group(1))[match.group(2)]
            elif match := re.match(r"file://(.*)", value):
                value = Path(match.group(1))
        elif isinstance(value, Sequence):
            value = [fromdict(x) for x in value]
        return value

    klass = ClassResolver()(dct["class"])

    kwargs.update(
        {
            k: fromdict(v)
            for k, v in dct.items()
            if field_filter(klass, k) and k not in kwargs
        }
    )

    return klass(**kwargs)


extract_import_re = re.compile(r"\s*(?:from|import)\s+([\w\.]+)")

NOTHING_STR = "__PIPELINE_INPUT__"


def pydra_asdict(
    obj: TaskBase, required_modules: ty.Set[str], workflow: Workflow = None
) -> dict:
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
    dct = {
        "name": obj.name,
        "class": "<" + ClassResolver.tostr(obj, strip_prefix=False) + ">",
    }
    if isinstance(obj, Workflow):
        dct["nodes"] = [
            pydra_asdict(n, required_modules=required_modules, workflow=obj)
            for n in obj.nodes
        ]
        dct["outputs"] = outputs = {}
        for outpt_name, lf in obj._connections:
            outputs[outpt_name] = {"task": lf.name, "field": lf.field}
    else:
        if isinstance(obj, FunctionTask):
            func = cp.loads(obj.inputs._func)
            module = inspect.getmodule(func)
            dct["class"] = "<" + module.__name__ + ":" + func.__name__ + ">"
            required_modules.add(module.__name__)
            # inspect source for any import lines (should be present in function
            # not module)
            for line in inspect.getsourcelines(func)[0]:
                if match := extract_import_re.match(line):
                    required_modules.add(match.group(1))
            # TODO: check source for references to external modules that aren't
            #       imported within function
        elif type(obj).__module__ != "pydra.engine.task":
            pkg = package_from_module(type(obj).__module__)
            dct["package"] = pkg.key
            dct["version"] = pkg.version
        if hasattr(obj, "container"):
            dct["container"] = {"type": obj.container, "image": obj.image}
    dct["inputs"] = inputs = {}
    for inpt_name in obj.input_names:
        if not inpt_name.startswith("_"):
            inpt_value = getattr(obj.inputs, inpt_name)
            if isinstance(inpt_value, LazyField):
                inputs[inpt_name] = {"field": inpt_value.field}
                # If the lazy field comes from the workflow lazy in, we omit
                # the "task" item
                if workflow is None or inpt_value.name != workflow.name:
                    inputs[inpt_name]["task"] = inpt_value.name
            elif inpt_value == attrs.NOTHING:
                inputs[inpt_name] = NOTHING_STR
            else:
                inputs[inpt_name] = inpt_value
    return dct


def lazy_field_fromdict(dct: dict, workflow: Workflow):
    """Unserialises a LazyField object from a dictionary"""
    if "task" in dct:
        inpt_task = getattr(workflow, dct["task"])
        lf = getattr(inpt_task.lzout, dct["field"])
    else:
        lf = getattr(workflow.lzin, dct["field"])
    return lf


def pydra_fromdict(dct: dict, workflow: Workflow = None, **kwargs) -> TaskBase:
    """Recreates a Pydra Task/Workflow from a dictionary object created by
    `pydra_asdict`

    Parameters
    ----------
    dct : dict
        dictionary representations of the object to recreate
    name : str
        name to give the object
    workflow : pydra.Workflow, optional
        the containing workflow that the object to recreate is connected to
    **kwargs
        additional keyword arguments passed to the pydra Object init method

    Returns
    -------
    pydra.engine.core.TaskBase
        the recreated Pydra object
    """
    klass = ClassResolver()(dct["class"])
    # Resolve lazy-field references to workflow fields
    inputs = {}
    for inpt_name, inpt_val in dct["inputs"].items():
        if inpt_val == NOTHING_STR:
            continue
        # Check for 'field' key in a dictionary val and convert to a
        # LazyField object
        if isinstance(inpt_val, dict) and "field" in inpt_val:
            inpt_val = lazy_field_fromdict(inpt_val, workflow=workflow)
        inputs[inpt_name] = inpt_val
    kwargs.update((k, v) for k, v in inputs.items() if k not in kwargs)
    if klass is Workflow:
        obj = Workflow(name=dct["name"], input_spec=list(dct["inputs"]), **kwargs)
        for node_dict in dct["nodes"]:
            obj.add(pydra_fromdict(node_dict, workflow=obj))
        obj.set_output(
            [
                (n, lazy_field_fromdict(f, workflow=obj))
                for n, f in dct["outputs"].items()
            ]
        )
    else:
        obj = klass(name=dct["name"], **kwargs)
    return obj


@attrs.define
class ObjectConverter:

    klass: type
    allow_none: bool = False
    default_if_none: ty.Any = None
    accept_metadata: bool = False

    def __call__(self, value):
        return self._create_object(value)

    def _create_object(self, value, **kwargs):
        if value is None:
            if kwargs:
                value = {}
            elif self.allow_none:
                if callable(self.default_if_none):
                    default = self.default_if_none()
                else:
                    default = self.default_if_none
                return default
            else:
                raise ValueError(
                    f"None values not accepted in automatic conversion to {self.klass}"
                )
        if isinstance(value, dict):
            if self.accept_metadata:
                klass_attrs = set(attrs.fields_dict(self.klass))
                value_kwargs = {k: v for k, v in value.items() if k in klass_attrs}
                value_kwargs["metadata"] = {
                    k: v for k, v in value.items() if k not in klass_attrs
                }
            else:
                value_kwargs = value
            value_kwargs.update(kwargs)
            try:
                obj = self.klass(**value_kwargs)
            except TypeError as e:
                msg = f"when creating {self.klass} from {value_kwargs}"
                add_exc_note(e, msg)
                raise
        elif isinstance(value, (list, tuple)):
            obj = self.klass(*value, **kwargs)
        elif isinstance(value, self.klass):
            obj = copy(value)
            for k, v in kwargs.items():
                setattr(obj, k, v)
        elif isinstance(value, (str, int, float, bool)):
            # If there are kwargs that are in the first N positions of the
            # argument list, add them in as positional arguments first and then
            # append the value to the end of the args list
            args = []
            kgs = copy(kwargs)
            for field_name in attrs.fields_dict(self.klass):
                try:
                    args.append(kgs.pop(field_name))
                except KeyError:
                    break
            args.append(value)
            obj = self.klass(*args, **kgs)
        else:
            raise ValueError(f"Cannot convert {value} into {self.klass}")
        return obj


@attrs.define
class ObjectListConverter(ObjectConverter):
    def __call__(self, value):
        converted = []
        if isinstance(value, dict):
            for name, item in value.items():
                converted.append(self._create_object(item, name=name))
        else:
            for item in value:
                converted.append(self._create_object(item))
        return converted

    @classmethod
    def asdict(cls, objs: list, **kwargs) -> dict:
        dct = {}
        for obj in objs:
            obj_dict = attrs.asdict(obj, **kwargs)
            dct[obj_dict.pop("name")] = obj_dict
        return dct


def parse_value(value):
    """Parses values from string representations"""
    try:
        value = json.loads(
            value
        )  # FIXME: Is this value replace really necessary, need to investigate where it is used again
    except (TypeError, json.decoder.JSONDecodeError):
        pass
    return value
