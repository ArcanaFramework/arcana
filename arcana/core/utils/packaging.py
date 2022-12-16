from typing import Sequence
import importlib_metadata
import pkgutil
from copy import copy
from importlib import import_module
from inspect import isclass
import pkg_resources
from pathlib import Path
from collections.abc import Iterable
from arcana.exceptions import ArcanaUsageError


# Avoid arcana.__version__ causing a circular import
from arcana._version import get_versions

__version__ = get_versions()["version"]
del get_versions


def submodules(package):
    """Iterates all modules within the given package

    Parameters
    ----------
    package : module
        the package to iterate over

    Yields
    ------
    module
        all modules within the package
    """
    for mod_info in pkgutil.iter_modules(
        [str(Path(package.__file__).parent)], prefix=package.__package__ + "."
    ):
        yield import_module(mod_info.name)


def list_subclasses(package, base_class):
    """List all available subclasses of a base class in modules within the given
    package

    Parameters
    ----------
    package : module
        the package to list the subclasses within
    base_class : type
        the base class

    Returns
    -------
    list
        all subclasses of the base-class found with the package
    """
    subclasses = []
    for module in submodules(package):
        for obj_name in dir(module):
            obj = getattr(module, obj_name)
            if isclass(obj) and issubclass(obj, base_class) and obj is not base_class:
                subclasses.append(obj)
    return subclasses


def package_from_module(module: Sequence[str]):
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
        module_paths.add(importlib_metadata.PackagePath(module_path.replace(".", "/")))
    packages = set()
    for pkg in pkg_resources.working_set:
        try:
            paths = importlib_metadata.files(pkg.key)
        except importlib_metadata.PackageNotFoundError:
            continue
        match = False
        for path in paths:
            if path.suffix != ".py":
                continue
            path = path.with_suffix("")
            if path.name == "__init__":
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
        paths_str = "', '".join(str(p) for p in module_paths)
        raise ArcanaUsageError(f"Did not find package for {paths_str}")
    return tuple(packages) if as_tuple else next(iter(packages))


def pkg_versions(modules):
    versions = {p.key: p.version for p in package_from_module(modules)}
    versions["arcana"] = __version__
    return versions
