import click
from arcana.core.deploy.command import entrypoint_opts
from arcana.deploy.common import PipelineImage
from .base import common_group


@common_group.command(
    name="pipeline-entrypoint",
    help="""Loads/creates a dataset, then applies and launches a pipeline
in a single command. To be used within the command configuration of an XNAT
Container Service ready Docker image.

DATASET_LOCATOR string containing the nickname of the data store, the ID of the
dataset (e.g. XNAT project ID or file-system directory) and the dataset's name
in the format <store-nickname>//<dataset-id>[@<dataset-name>]

""",
)
@click.argument("dataset_locator")
@entrypoint_opts.data_columns
@entrypoint_opts.parameterisation
@entrypoint_opts.execution
@entrypoint_opts.dataset_config
@entrypoint_opts.debugging
def pipeline_entrypoint(
    dataset_locator,
    spec_path,
    **kwargs,
):

    image_spec = PipelineImage.load(spec_path=spec_path)

    image_spec.command.run(
        dataset_locator,
        **kwargs,
    )
