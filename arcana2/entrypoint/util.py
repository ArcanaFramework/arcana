from collections import Sequence
import typing as ty
import logging
from importlib import import_module
from arcana2.exceptions import ArcanaUsageError
from arcana2.data.repository import Repository, FileSystem, Xnat, XnatCS


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


def init_repository(args: Sequence[ty.Any]) -> Repository:
    try:
        repo_type = args.pop(0)
    except IndexError:
        raise ArcanaUsageError(
            f"Repository type not provided to '--repository' option")
    nargs = len(args)
    if repo_type == 'file_system':
        if unrecognised := [a for a in args
                            if a not in FileSystem.POSSIBLE_LEVELS]:
            raise ArcanaUsageError(
                f"Unrecognised levels {unrecognised} for "
                f"FileSystem repo (allowed {FileSystem.POSSIBLE_LEVELS}")
        repo = FileSystem(levels=args)
    elif repo_type == 'xnat':
        if nargs < 1 or nargs > 3:
            raise ArcanaUsageError(
                f"Incorrect number of arguments passed to an Xnat "
                f"repository ({args}), at least 1 (SERVER) and no more than 3 "
                f"are required (SERVER, USER, PASSWORD)")
        repository = Xnat(
            server=args[0],
            user=args[1] if nargs > 1 else None,
            password=args[2] if nargs > 2 else None)
    elif repo_type == 'xnat_cs':
        if nargs < 1 or nargs > 3:
            raise ArcanaUsageError(
                f"Incorrect number of arguments passed to an Xnat "
                f"repository ({args}), at least 1 (LEVEL) and no more than 3 "
                f"are required (LEVEL, SUBJECT, VISIT)")
        repository = XnatCS(level=args[0],
                            subject=args[1] if nargs > 1 else None,
                            visit=args[2] if nargs > 2 else None)
    else:
        raise ArcanaUsageError(
            f"Unrecognised repository type provided as first argument "
            f"to '--repository' option ({repo_type})")
    return repository


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
