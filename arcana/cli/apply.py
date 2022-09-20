import click
from arcana.core.data.set import Dataset
from arcana.core.cli import cli
from arcana.core.utils import resolve_class, parse_value


@cli.group()
def apply():
    pass


@apply.command(
    name="pipeline",
    help="""Apply a Pydra workflow to a dataset as a pipeline between
two columns

DATASET_ID_STR string containing the nickname of the data store, the ID of the dataset
(e.g. XNAT project ID or file-system directory) and the dataset's name in the
format <STORE-NICKNAME>//<DATASET-ID>:<DATASET-NAME>

PIPELINE_NAME is the name of the pipeline

WORKFLOW_LOCATION is the location to a Pydra workflow on the Python system path,
<MODULE>:<WORKFLOW>""",
)
@click.argument("dataset_id_str")
@click.argument("pipeline_name")
@click.argument("workflow_location")
@click.option(
    "--input",
    "-i",
    nargs=3,
    default=(),
    metavar="<col-name> <pydra-field> <required-format>",
    multiple=True,
    type=str,
    help=(
        "the link between a column and an input of the workflow. "
        "The required format is the location (<module-path>:<class>) of the format "
        "expected by the workflow"
    ),
)
@click.option(
    "--output",
    "-o",
    nargs=3,
    default=(),
    metavar="<col-name> <pydra-field> <produced-format>",
    multiple=True,
    type=str,
    help=(
        "the link between an output of the workflow and a sink column. "
        "The produced format is the location (<module-path>:<class>) of the format "
        "produced by the workflow"
    ),
)
@click.option(
    "--parameter",
    "-p",
    nargs=2,
    default=(),
    metavar="<name> <value>",
    multiple=True,
    type=str,
    help=("a fixed parameter of the workflow to set when applying it"),
)
@click.option(
    "--source",
    "-s",
    nargs=3,
    default=(),
    metavar="<col-name> <pydra-field> <required-format>",
    multiple=True,
    type=str,
    help=(
        "add a source to the dataset and link it to an input of the workflow "
        "in a single step. The source column must be able to be specified by its "
        "path alone and be already in the format required by the workflow"
    ),
)
@click.option(
    "--sink",
    "-k",
    nargs=3,
    default=(),
    metavar="<col-name> <pydra-field> <produced-format>",
    multiple=True,
    type=str,
    help=(
        "add a sink to the dataset and link it to an output of the workflow "
        "in a single step. The sink column be in the same format as produced "
        "by the workflow"
    ),
)
@click.option(
    "--row-frequency",
    "-f",
    default=None,
    type=str,
    help=(
        "the row-frequency of the rows the pipeline will be executed over, i.e. "
        "will it be run once per-session, per-subject or per whole dataset, "
        "by default the highest row-frequency rows (e.g. per-session)"
    ),
)
@click.option(
    "--overwrite/--no-overwrite",
    default=False,
    help=("whether to overwrite previous pipelines"),
)
def apply_pipeline(
    dataset_id_str,
    pipeline_name,
    workflow_location,
    input,
    output,
    parameter,
    source,
    sink,
    row_frequency,
    overwrite,
):

    dataset = Dataset.load(dataset_id_str)
    workflow = resolve_class(workflow_location)(
        name="workflow", **{n: parse_value(v) for n, v in parameter}
    )

    inputs = parse_col_option(input)
    outputs = parse_col_option(output)
    sources = parse_col_option(source)
    sinks = parse_col_option(sink)

    for col_name, pydra_field, format in sources:
        dataset.add_source(col_name, format)
        inputs.append((col_name, pydra_field, format))

    for col_name, pydra_field, format in sinks:
        dataset.add_sink(col_name, format)
        outputs.append((col_name, pydra_field, format))

    dataset.apply_pipeline(
        pipeline_name,
        workflow,
        inputs,
        outputs,
        row_frequency=row_frequency,
        overwrite=overwrite,
    )

    dataset.save()


@apply.command(name="analysis", help="""Applies an analysis class to a dataset""")
def apply_analysis():
    raise NotImplementedError


@apply.command(name="bids-app", help="Apply a BIDS app to a dataset as a pipeline")
@click.option(
    "--container",
    nargs=2,
    default=None,
    metavar="<engine-tag>",
    help=(
        "The container engine ('docker'|'singularity') and the image"
        " to run the app in"
    ),
)
@click.option(
    "--virtualisation",
    default="none",
    type=click.Choice(["docker", "singularity", "none"], case_sensitive=False),
    help=(
        "The virtualisation method to run with the task with (only "
        "applicable to BIDS app tasks)"
    ),
)
def apply_bids_app():
    raise NotImplementedError


def parse_col_option(option):
    return [
        (c, p, resolve_class(f, prefixes=["arcana.data.formats"])) for c, p, f in option
    ]
