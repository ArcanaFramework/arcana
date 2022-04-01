from inspect import Arguments
import click
from arcana.core.data.set import Dataset
from arcana.core.cli import cli
from arcana.core.utils import resolve_class, parse_value


@cli.group()
def apply():
    pass


@apply.command(name='workflow', help="""Apply a Pydra workflow to a dataset as a pipeline between
two columns

ID_STR string containing the nick-name of the store, the ID of the dataset
(e.g. XNAT project ID or file-system directory) and the dataset's name in the
format <NICKNAME>//DATASET_ID:DATASET_NAME

NAME of the pipeline

WORKFLOW_LOCATION is the location to a Pydra workflow on the Python system path,
<MODULE>:<WORKFLOW>""")
@click.argument('id_str')
@click.argument('name')
@click.argument('workflow_location')
@click.option(
    '--input', '-i', nargs=3, default=(), metavar='<col-name> <pydra-field> <required-format>',
    multiple=True, type=str,
    help=("the link between a column and an input of the workflow. "
          "The required format is the location (<module-path>:<class>) of the format "
          "expected by the workflow"))
@click.option(
    '--output', '-o', nargs=3, default=(), metavar='<col-name> <pydra-field> <produced-format>',
    multiple=True, type=str,
    help=("the link between an output of the workflow and a sink column. "
          "The produced format is the location (<module-path>:<class>) of the format "
          "produced by the workflow"))
@click.option(
    '--parameter', '-p', nargs=2, default=(), metavar='<name> <value>', multiple=True, type=str,
    help=("a fixed parameter of the workflow to set when applying it"))
@click.option(
    '--source', '-s', nargs=3, default=(), metavar='<col-name> <pydra-field> <required-format>',
    multiple=True, type=str,
    help=("add a source to the dataset and link it to an input of the workflow "
          "in a single step. The source column must be able to be specified by its "
          "path alone and be already in the format required by the workflow"))
@click.option(
    '--sink', '-k', nargs=3, default=(), metavar='<col-name> <pydra-field> <produced-format>',
    multiple=True, type=str,
    help=("add a sink to the dataset and link it to an output of the workflow "
          "in a single step. The sink column be in the same format as produced "
          "by the workflow"))
@click.option(
    '--frequency', '-f', default=None, type=str,
    help=("the frequency of the nodes the pipeline will be executed over, i.e. "
          "will it be run once per-session, per-subject or per whole dataset, "
          "by default the highest frequency nodes (e.g. per-session)"))
@click.option(
    '--overwrite/--no-overwrite', default=False,
    help=("whether to overwrite previous connections to existing sink columns "
          "or not"))
def apply_workflow(id_str, name, workflow_location, input, output, parameter,
                   source, sink, frequency, overwrite):

    dataset = Dataset.load(id_str)
    workflow = resolve_class(workflow_location)(
        name='inner',
        **{n: parse_value(v) for n, v in parameter})

    def parse_col_option(option):
        return [(c, p, resolve_class(f, prefixes=['arcana.data.formats']))
                for c, p, f in option]
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
    
    dataset.apply_workflow(
        name, workflow, inputs, outputs, frequency=frequency,
        overwrite=overwrite)

    dataset.save()


# @apply.command(name='analysis', help="""Applies an analysis class to a dataset""")
# def apply_analysis():
#     raise NotImplementedError
