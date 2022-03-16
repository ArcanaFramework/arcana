
import os
from importlib import import_module
import click
from arcana.core.cli import cli
from arcana.data.spaces.medicalimaging import Clinical
from arcana.exceptions import ArcanaUsageError
from arcana.data.stores.file_system import FileSystem
from arcana.data.stores.xnat import Xnat
from arcana.data.stores.xnat.cs import XnatViaCS



XNAT_CACHE_DIR = 'xnat-cache'

@cli.group()
def dataset():
    pass


@dataset.command(help=("""Define the tree structure and IDs to include in a
dataset. Where possible, the definition file is saved inside the dataset for
use by multiple users, if not possible it is stored in the ~/.arcana directory.

store
    The name data store the dataset is located in (see 'arcana store' command).
    For basic file system directories use 'file', and bids-formatted directories
    use 'bids'.
id
    ID of the dataset in the data store. For XNAT repositories this is the
    project ID, for file-system directories this is the path to the rdirectory
hierarchy
    The data frequencies that are present in the data tree. "
    For some store types this is fixed (e.g. XNAT-> subject > session) but "
    for more flexible (e.g. FileSystem), which dimension each layer of
    sub-directories corresponds to can be arbitrarily specified. dimensions
    "
"""))
@click.argument('id')
@click.argument('hierarchy', nargs=-1)
@click.option(
    '--included', nargs=2, default=[], metavar='<freq-id>',
    multiple=True,
    help=("The nodes to include in the dataset. First value is the "
           "frequency of the ID (e.g. 'group', 'subject', 'session') "
           "followed by the IDs to be included in the dataset. "
           "If the second arg contains '/' then it is interpreted as "
           "the path to a text file containing a list of IDs"))
@click.option(
    '--excluded', nargs=2, default=[], metavar='<freq-id>',
    multiple=True,
    help=("The nodes to exclude from the dataset. First value is the "
          "frequency of the ID (e.g. 'group', 'subject', 'session') "
          "followed by the IDs to be included in the dataset. "
          "If the second arg contains '/' then it is interpreted as "
          "the path to a text file containing a list of IDs"))
@click.option(
    '--space', type=str, default='medicalimaging.Clinical',
    help=("The enum that specifies the data dimensions of the dataset. "
          "Defaults to `Clinical`, which "
          "consists of the typical dataset>group>subject>session "
          "data tree used in medicalimaging trials/studies"))
@click.option(
    '--id_inference', nargs=2, metavar='<source-regex>',
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
def define(id, hierarchy, included, excluded, space, id_inference):



    hierarchy = [dimensions[l]
                    for l in args.hierarchy] if args.hierarchy else None
    
    repo_args = list(args.store)
    repo_type = repo_args.pop(0)
    nargs = len(repo_args)


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
    raise NotImplementedError


def optional_args(names, args):
    kwargs = {}


@dataset.command(name='add-source', help="""Adds a source column to a dataset. A source column
selects comparable items along a dimension of the dataset to serve as
an input to pipelines and analyses.

Arguments
---------
dataset_path
    The path to the dataset including store and dataset name (where
    applicable), e.g. central-xnat//MYXNATPROJECT:pass_t1w_qc
name
    The name the source will be referenced by
datatype
    The data type of the column. Can be a field (int|float|str|bool),
    field array (list[int|float|str|bool]) or "file-group"
    (file, file+header/side-cars or directory)
""")
@click.argument('dataset_path')
@click.argument('column_name')
@click.argument('datatype')
@click.option(
    '--frequency', '-f', metavar='<dimension>',
    help=("The frequency that items appear in the dataset (e.g. per "
          "'session', 'subject', 'timepoint', 'group', 'dataset' for "
          "medicalimaging:Clinical data dimensions"),
    show_default="highest")
@click.option(
    '--path', '-p',
    help=("Path to item in the dataset. If 'regex' option is provided it will "
          "be treated as a regular-expression (in Python syntax)"))
@click.option(
    '--order', type=int,
    help=("If multiple items match the remaining criteria within a session, "
          "select the <order>th of the matching items"))
@click.option(
    '--quality', '-q', default='usable',
    help=("For data stores that enable flagging of data quality, "
          "this option can filter out poor quality scans"))
@click.option(
    '--regex/--no-regex', 'is_regex', default=True,
    help=("Whether the 'path' option should be treated as a regular expression "
          "or not"))
@click.option(
    '--header', '-h', nargs=2, metavar='<key-val>',
    help=("Match on specific header value. This option is only valid for "
          "select datatypes that the implement the 'header_val()' method "
          "(e.g. medicalimaging:dicom)."))
def add_source(dataset_path, column_name, datatype, frequency, path, order,
               quality, is_regex, header):
    raise NotImplementedError


@dataset.command(name='add-sink', help="""Adds a sink column to a dataset. A sink column
specifies how data should be writen into the dataset.

Arguments
---------
dataset_path
    The path to the dataset including store and dataset name (where
    applicable), e.g. central-xnat//MYXNATPROJECT:pass_t1w_qc
name
    The name the source will be referenced by
datatype
    The data type of the column. Can be a field (int|float|str|bool),
    field array (list[int|float|str|bool]) or "file-group"
    (file, file+header/side-cars or directory)
""")
@click.argument('dataset_path')
@click.argument('name')
@click.argument('datatype')
@click.option(
    '--frequency', '-f', metavar='<dimension>',
    help=("The frequency that items appear in the dataset (e.g. per "
          "'session', 'subject', 'timepoint', 'group', 'dataset' for "
          "medicalimaging:Clinical data dimensions"),
    show_default="highest")
@click.option(
    '--path', '-p',
    help=("Path to item in the dataset. If 'regex' option is provided it will "
          "be treated as a regular-expression (in Python syntax)"))
def add_sink(dataset_path, name, datatype, frequency, path):
    raise NotImplementedError


@dataset.command(
    name="missing-items",
    help="""Finds the IDs of nodes that are missing a valid entry for an item in
the column.

Arguments
---------
dataset_path
    The path to the dataset including store and dataset name (where
    applicable), e.g. central-xnat//MYXNATPROJECT:pass_t1w_qc
name
    The name of the column to check
""")
@click.argument('dataset_path')
@click.argument('name')
def missing_items(name):
    raise NotImplementedError

    