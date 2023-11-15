from __future__ import annotations
import click
from .base import cli
from arcana.core.data.set.base import Dataset
from arcana.core.data.store import DataStore
from arcana.core.data.space import DataSpace
from fileformats.core import DataType
from arcana.core.utils.serialize import ClassResolver


@cli.group()
def dataset():
    pass


@dataset.command(
    name="define",
    help=(
        """Define the tree structure and IDs to include in a
dataset. Where possible, the definition file is saved inside the dataset for
use by multiple users, if not possible it is stored in the ~/.arcana directory.

DATASET_LOCATOR string containing the nick-name of the store, the ID of the dataset
(e.g. XNAT project ID or file-system directory) and the dataset's name in the
format <store-nickname>//<dataset-id>[@<dataset-name>]

HIERARCHY the data frequencies that are present in the data tree. For some
store types this is fixed (e.g. XNAT-> subject > session) but for more flexible
(e.g. MockRemote), which dimension each layer of sub-directories corresponds to
can be arbitrarily specified. dimensions"""
    ),
)
@click.argument("dataset_locator")
@click.argument("hierarchy", nargs=-1)
@click.option(
    "--space",
    default="common:Clinical",
    type=str,
    help=(
        "The enum that specifies the data dimensions of the dataset. "
        "Defaults to `Clinical`, which "
        "consists of the typical dataset>group>subject>session "
        "data tree used in medimage trials/studies"
    ),
)
@click.option(
    "--include",
    nargs=2,
    default=(),
    metavar="<freq-id>",
    multiple=True,
    type=str,
    help=(
        "The rows to include in the dataset. First value is the "
        "row-frequency of the ID (e.g. 'group', 'subject', 'session') "
        "followed by the IDs to be included in the dataset. "
        "If the second arg contains '/' then it is interpreted as "
        "the path to a text file containing a list of IDs"
    ),
)
@click.option(
    "--exclude",
    nargs=2,
    default=(),
    metavar="<freq-id>",
    multiple=True,
    type=str,
    help=(
        "The rows to exclude from the dataset. First value is the "
        "row-frequency of the ID (e.g. 'group', 'subject', 'session') "
        "followed by the IDs to be included in the dataset. "
        "If the second arg contains '/' then it is interpreted as "
        "the path to a text file containing a list of IDs"
    ),
)
@click.option(
    "--id-pattern",
    nargs=2,
    metavar="<row-frequency> <pattern>",
    multiple=True,
    help="""Specifies how IDs of row frequencies that not explicitly
provided are inferred from the IDs that are. For example, given a set
of subject IDs that are a combination of the ID of the group that they belong
to + their member IDs (i.e. matched test/controls have same member ID), e.g.

CONTROL01, CONTROL02, CONTROL03, ... and TEST01, TEST02, TEST03

the group ID can be extracted by providing the ID to source it from
(i.e. subject) and a regular expression (in Python regex syntax:
https://docs.python.org/3/library/re.html) with a single group corresponding to
the inferred IDs

--id-pattern group 'subject:([A-Z]+)[0-9]+' --id-pattern member 'subject:[A-Z]+([0-9]+)'

""",
)
def define(dataset_locator, hierarchy, include, exclude, space, id_pattern):

    store_name, id, name = Dataset.parse_id_str(dataset_locator)

    if not hierarchy:
        hierarchy = None

    store = DataStore.load(store_name)

    if space:
        space = ClassResolver(DataSpace)(space)

    dataset = store.define_dataset(
        id,
        hierarchy=hierarchy,
        space=space,
        id_patterns=dict(id_pattern),
        include=dict(include),
        exclude=dict(exclude),
    )

    dataset.save(name)


@dataset.command(
    name="add-source",
    help="""Adds a source column to a dataset. A source column
selects comparable items along a dimension of the dataset to serve as
an input to pipelines and analyses.

DATASET_LOCATOR The path to the dataset including store and dataset name
(where applicable), e.g. central-xnat//MYXNATPROJECT:pass_t1w_qc

NAME: The name the source will be referenced by

FORMAT: The data type of the column. Can be a field (int|float|str|bool),
field array (ty.List[int|float|str|bool]) or
"file-set" (file, file+header/side-cars or directory)
""",
)
@click.argument("dataset_locator")
@click.argument("name")
@click.argument("datatype")
@click.option(
    "--row-frequency",
    "-f",
    metavar="<dimension>",
    help=(
        "The row-frequency that items appear in the dataset (e.g. per "
        "'session', 'subject', 'timepoint', 'group', 'dataset' for "
        "common:Clinical data dimensions"
    ),
    show_default="highest",
)
@click.option(
    "--path",
    "-p",
    help=(
        "Path to item in the dataset. If 'regex' option is provided it will "
        "be treated as a regular-expression (in Python syntax)"
    ),
)
@click.option(
    "--order",
    type=int,
    help=(
        "If multiple items match the remaining criteria within a session, "
        "select the <order>th of the matching items"
    ),
)
@click.option(
    "--quality",
    "-q",
    default="usable",
    help=(
        "For data stores that enable flagging of data quality, "
        "this option can filter out poor quality scans"
    ),
)
@click.option(
    "--regex/--no-regex",
    "is_regex",
    default=True,
    help=(
        "Whether the 'path' option should be treated as a regular expression " "or not"
    ),
)
@click.option(
    "--header",
    "-h",
    nargs=2,
    metavar="<key-val>",
    multiple=True,
    help=(
        "Match on specific header value. This option is only valid for "
        "select formats that the implement the 'header_val()' method "
        "(e.g. medimage/dicom-series)."
    ),
)
def add_source(
    dataset_locator,
    name,
    datatype,
    row_frequency,
    path,
    order,
    quality,
    is_regex,
    header,
):
    dataset = Dataset.load(dataset_locator)
    dataset.add_source(
        name=name,
        path=path,
        datatype=ClassResolver(DataType)(datatype),
        row_frequency=row_frequency,
        quality_threshold=quality,
        order=order,
        required_metadata=dict(header),
        is_regex=is_regex,
    )
    dataset.save()


@dataset.command(
    name="add-sink",
    help="""Adds a sink column to a dataset. A sink column
specifies how data should be written into the dataset.

Arguments
---------
dataset_path
    The path to the dataset including store and dataset name (where
    applicable), e.g. central-xnat//MYXNATPROJECT:pass_t1w_qc
name
    The name the source will be referenced by
datatype
    The data type of the column. Can be a field (int|float|str|bool),
    field array (ty.List[int|float|str|bool]) or "file-set"
    (file, file+header/side-cars or directory)
""",
)
@click.argument("dataset_locator")
@click.argument("name")
@click.argument("datatype")
@click.option(
    "--row-frequency",
    "-f",
    metavar="<dimension>",
    help=(
        "The row-frequency that items appear in the dataset (e.g. per "
        "'session', 'subject', 'timepoint', 'group', 'dataset' for "
        "Clinical data dimensions"
    ),
    show_default="highest",
)
@click.option(
    "--path",
    "-p",
    help=(
        "Path to item in the dataset. If 'regex' option is provided it will "
        "be treated as a regular-expression (in Python syntax)"
    ),
)
@click.option(
    "--salience",
    "-s",
    help=(
        "The salience of the column, i.e. whether it will show up on "
        "'arcana derive menu'"
    ),
)
def add_sink(dataset_locator, name, datatype, row_frequency, path, salience):
    dataset = Dataset.load(dataset_locator)
    dataset.add_sink(
        name=name,
        path=path,
        datatype=ClassResolver(DataType)(datatype),
        row_frequency=row_frequency,
        salience=salience,
    )
    dataset.save()


@dataset.command(
    name="missing-items",
    help="""Finds the IDs of rows that are missing a valid entry for an item in
the column.

DATASET_LOCATOR of the dataset including store and dataset name (where
    applicable), e.g. central-xnat//MYXNATPROJECT:pass_t1w_qc

COLUMN_NAMES, [COLUMN_NAMES, ...] for the columns to check, defaults to all source columns
""",
)
@click.argument("dataset_locator")
@click.argument("column_names", nargs=-1)
def missing_items(dataset_locator, column_names):
    dataset = Dataset.load(dataset_locator)
    if not column_names:
        column_names = [n for n, c in dataset.columns.items() if not c.is_sink]
    for column_name in column_names:
        column = dataset.columns[column_name]
        empty_cells = [c for c in column.cells() if c.is_empty]
        if empty_cells:
            click.echo(f"'{column.name}': " + ", ".join(c.row.id for c in empty_cells))


@dataset.command(
    help="""
Exports a dataset from one data store into another

DATASET_LOCATOR of the dataset to copy

STORE_NICKNAME of the destination store

ID for the dataset in the destination store

COLUMN_NAMES, [COLUMN_NAMES, ...] to be included in the export, by default all will be included
"""
)
@click.argument("dataset_locator")
@click.argument("store_nickname")
@click.argument("imported_id")
@click.argument("column_names", nargs=-1)
@click.option(
    "--id-pattern",
    nargs=2,
    metavar="<id> <regex>",
    help="mapping of ID from hierarchy of the source dataset to that of the destination",
)
@click.option(
    "--hierarchy",
    "-h",
    type=str,
    default=None,
    help=(
        "The hierarchy to use for the target dataset, e.g. whether the layers of the "
        "data tree correspond to subject>session or group>subject>timeptoint, etc..."
    ),
)
@click.option(
    "--use-original-paths/--use-column-names",
    type=bool,
    default=False,
    help=(
        "whether to rename the paths of the exported data items to match their column "
        "names, or whether to use the original paths in the source store"
    ),
)
def export(
    dataset_locator,
    store_nickname,
    imported_id,
    column_names,
    id_pattern,
    hierarchy,
    use_original_paths,
):
    dataset = Dataset.load(dataset_locator)
    store = DataStore.load(store_nickname)
    if hierarchy:
        hierarchy = hierarchy.split(",")
    if not column_names:
        column_names = None
    store.import_dataset(
        id=imported_id,
        dataset=dataset,
        column_names=column_names,
        id_patterns=id_pattern,
        hierarchy=hierarchy,
        use_original_paths=use_original_paths,
    )


@dataset.command(
    help="""
Creates a copy of a dataset definition under a new name (so it can be modified, e.g.
for different analysis)

DATASET_LOCATOR of the dataset to copy

NEW_NAME for the dataset
"""
)
@click.argument("dataset_locator")
@click.argument("new_name")
def copy(dataset_locator, new_name):
    dataset = Dataset.load(dataset_locator)
    dataset.save(new_name)
