from __future__ import annotations
from typing import Sequence
import json
import importlib_metadata
import pkgutil
from importlib import import_module
from inspect import isclass
import pkg_resources
from pathlib import Path
from collections.abc import Iterable
from arcana.core.exceptions import ArcanaUsageError
from arcana.core import __version__


def submodules(package, subpkg=None):
    """Iterates all modules within the given package

    Parameters
    ----------
    package : module
        the package to iterate over
    subpkg : str, optional
        the sub-package (of the sub-packages) to return instead of the first level down.
        e.g. package=arcana, subpkg=data -> arcana.dirtree.data, arcana.xnat.data, etc...

    Yields
    ------
    module
        all modules within the package
    """
    for mod_info in pkgutil.iter_modules(
        package.__path__, prefix=package.__package__ + "."
    ):
        if subpkg is not None:
            try:
                yield import_module(mod_info.name + "." + subpkg)
            except ImportError:
                continue
        else:
            yield import_module(mod_info.name)


def list_subclasses(package, base_class, subpkg=None):
    """List all available subclasses of a base class in modules within the given
    package

    Parameters
    ----------
    package : module
        the package to list the subclasses within
    base_class : type
        the base class
    subpkg : str, optional
        the sub-package (of the sub-packages) to return instead of the first level down.
        e.g. package=arcana, subpkg=data -> arcana.dirtree.data, arcana.xnat.data, etc...

    Returns
    -------
    list
        all subclasses of the base-class found with the package
    """
    subclasses = []
    for module in submodules(package, subpkg=subpkg):
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
        if editable_dir := get_editable_dir(pkg):

            def is_in_pkg(module_path):
                pth = editable_dir.joinpath(module_path)
                return pth.with_suffix(".py").exists() or (pth / "__init__.py").exists()

        else:
            installed_paths = installed_module_paths(pkg)

            def is_in_pkg(module_path):
                return module_path in installed_paths

        if in_pkg := set(m for m in module_paths if is_in_pkg(m)):
            packages.add(pkg)
            module_paths -= in_pkg
            if not module_paths:  # If there are no more modules to find pkgs for break
                break
    if module_paths:
        paths_str = "', '".join(str(p) for p in module_paths)
        raise ArcanaUsageError(f"Did not find package for {paths_str}")
    return tuple(packages) if as_tuple else next(iter(packages))


def get_editable_dir(pkg: pkg_resources.DistInfoDistribution):
    """Returns the path to the editable dir to a package if it exists

    Parameters
    ----------
    pkg : pkg_resources.DistInfoDistribution
        the package to get the editable directory for

    Returns
    ------
    Path or None
        the path to the editable file or None if the package isn't installed in editable mode
    """
    direct_url_path = Path(pkg.egg_info) / "direct_url.json"
    if not direct_url_path.exists():
        return None
    with open(direct_url_path) as f:
        url_spec = json.load(f)
    url = url_spec["url"]
    if not url_spec["dir_info"].get("editable"):
        return None
    assert url.startswith("file://")
    return Path(url[len("file://") :])


def installed_module_paths(pkg: pkg_resources.DistInfoDistribution):
    """Returns the list of modules that are part of an installed package

    Parameters
    ----------
    pkg
        the package to list the installed modules
    """
    try:
        paths = importlib_metadata.files(pkg.key)
    except importlib_metadata.PackageNotFoundError:
        paths = []
    paths = set(
        p.parent if p.name == "__init__.py" else p.with_suffix("")
        for p in paths
        if p.suffix == ".py"
    )
    return paths


def pkg_versions(modules):
    versions = {p.key: p.version for p in package_from_module(modules)}
    versions["arcana"] = __version__
    return versions
