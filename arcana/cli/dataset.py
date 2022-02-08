
import os
from importlib import import_module
import click
from arcana.data.dimensions.clinical import Clinical
from arcana.exceptions import ArcanaUsageError
from arcana.data.stores.file_system import FileSystem
from arcana.data.stores.xnat import Xnat
from arcana.data.stores.xnat.cs import XnatViaCS
from arcana.core.cli import cli


XNAT_CACHE_DIR = 'xnat-cache'

@click.group()
def dataset():
    pass


@dataset.command(help=("""Define a dataset in a data store

id
    ID of the dataset in the data store. For XNAT repositories this is the
    project ID, for file-system stores this is the path to the root directory
store
    The data store in the dataset is stored in
hierarchy
    The data frequencies that are present in the data tree. "
    For some store types this is fixed (e.g. XNAT) but "
    for more flexible (e.g. FileSystem) the number of hierarchy "
    in the data tree, and what each layer corresponds to, "
    needs to specified. Defaults to all the hierarchy in the "
    data dimensions"
"""))
@click.argument('id')
@click.argument('store')
@click.argument('hierarchy', nargs=-1)
@click.option(
    '--included', nargs=2, default=[], metavar=('FREQ', 'ID'),
    multiple=True,
    help=("The nodes to include in the dataset. First value is the "
           "frequency of the ID (e.g. 'group', 'subject', 'session') "
           "followed by the IDs to be included in the dataset. "
           "If the second arg contains '/' then it is interpreted as "
           "the path to a text file containing a list of IDs"))
@click.option(
    '--excluded', nargs=2, default=[], metavar=('FREQ', 'ID'),
    multiple=True,
    help=("The nodes to exclude from the dataset. First value is the "
          "frequency of the ID (e.g. 'group', 'subject', 'session') "
          "followed by the IDs to be included in the dataset. "
          "If the second arg contains '/' then it is interpreted as "
          "the path to a text file containing a list of IDs"))    
@click.option(
    '--dataspace', type=str, default='clinical.Clinical',
    help=("The enum that specifies the data dimensions of the dataset. "
          "Defaults to `Clinical`, which "
          "consists of the typical dataset>group>subject>session "
          "data tree used in clinical trials/studies"))
@click.option(
    '--id_inference', nargs=2, metavar=('SOURCE', 'REGEX'),
    multiple=True,
    help="""Specifies how IDs of node frequencies that not explicitly
provided are inferred from the IDs that are. For example, given a set
of subject IDs that are a combination of the ID of the group that they belong
to + their member IDs (i.e. matched test/controls have same member ID), e.g.

CONTROL01, CONTROL02, CONTROL03, ... and TEST01, TEST02, TEST03

the group ID can be extracted by providing the ID to source it from
(i.e. subject) and a regular expression (in Python regex syntax:
https://docs.python.org/3/library/re.html) with a named
groups corresponding to the inferred IDs

--id_inference subject '(?P<group>[A-Z]+)(?P<member>[0-9]+)'

""")
@click.option(
    '--name', '-n', default='default',
    help=("A name to distinguish this dataset from the others defined for this "
          "directory/project (e.g. with different sub-sets of subjects)."))
def define(id, store, hierarchy):

    dimensions = cls.parse_dataspace(args)
    hierarchy = [dimensions[l]
                    for l in args.hierarchy] if args.hierarchy else None
    
    repo_args = list(args.store)
    repo_type = repo_args.pop(0)
    nargs = len(repo_args)
    if repo_type == 'file_system':
        store = FileSystem()
    elif repo_type == 'xnat':
        if nargs < 1 or nargs > 3:
            raise ArcanaUsageError(
                f"Incorrect number of arguments passed to an Xnat "
                f"store ({args}), at least 1 (SERVER) and no more "
                f"than 3 are required (SERVER, USER, PASSWORD)")
        optional_args = ['user', 'password']
        store = Xnat(
            server=repo_args[0],
            cache_dir=work_dir / XNAT_CACHE_DIR,
            **{k: v for k, v in zip(optional_args, repo_args[1:])})
        hierarchy = [Clinical.subject, Clinical.session]
    elif repo_type == 'xnat_via_cs':
        if nargs < 1 or nargs > 7:
            raise ArcanaUsageError(
                f"Incorrect number of arguments passed to an Xnat "
                f"store ({args}), at least 1 (FREQUENCY) is required "
                "and no more than 7 are permitted (FREQUENCY, NODE_ID, "
                "SERVER, USER, PASSWORD, INPUT_MOUNT, OUTPUT_MOUNT)")
        optional_args = ['node_id', 'server', 'user', 'password',
                            'input_mount', 'output_mount']
        store = XnatViaCS(
            cache_dir=work_dir / XNAT_CACHE_DIR,
            frequency=Clinical[repo_args[0]],
            **{k: v for k, v in zip(optional_args, repo_args[1:])})
        hierarchy = [Clinical.subject, Clinical.session]
    else:
        raise ArcanaUsageError(
            f"Unrecognised store type provided as first argument "
            f"to '--store' option ({repo_type})")

    if args.id_inference:
        id_inference = {t: (s, r) for t, s, r in args.ids_inference}
    else:
        id_inference = None

    if hierarchy is None:
        hierarchy = [max(dimensions)]

    def parse_ids(ids_args):
        parsed_ids = {}
        for iargs in ids_args:
            freq = dimensions[iargs.pop(0)]
            if len(iargs) == 1 and '/' in iargs[0]:
                with open(args.ids[0]) as f:
                    ids = f.read().split()
            else:
                ids = args.ids
            parsed_ids[freq] = ids
        return parsed_ids
    
    return store.dataset(args.dataset_name,
                                hierarchy=hierarchy,
                                id_inference=id_inference,
                                included=parse_ids(args.included),
                                excluded=parse_ids(args.excluded))

@dataset.command(help="""
Renames a data store saved in the stores.yml to a new name

old_name
    The current name of the store
new_name
    The new name for the store""")
@click.argument('old_name')
@click.argument('new_name')
def rename(old_name, new_name):
    pass


def parse_dataspace(args):
    parts = args.dataspace.split('.')
    if len(parts) < 2:
        raise ArcanaUsageError(
            f"Value provided to '--dataspace' arg ({args.dataspace}) "
            "needs to include module, either relative to "
            "'arcana.dataspaces' (e.g. clinical.Clinical) or an "
            "absolute path")
    module_path = '.'.join(parts[:-1])
    cls_name = parts[-1]
    try:
        module = import_module('arcana.data.dimensions.' + module_path)
    except ImportError:
        module = import_module(module_path)
    return getattr(module, cls_name)


def optional_args(names, args):
    kwargs = {}