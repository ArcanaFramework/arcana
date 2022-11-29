from __future__ import annotations
import shutil
import re
from copy import copy
import json
import logging
from pathlib import Path
import typing as ty
import sys
from collections import defaultdict
import attrs
from attrs.converters import default_if_none
import pydra.engine.task
from arcana.core.utils import (
    path2varname,
    ListDictConverter,
    format_resolver,
    task_resolver,
    class_location,
    resolve_class,
)
from arcana.core.pipeline import Input as PipelineInput, Output as PipelineOutput
from arcana.core.utils import show_workflow_errors
from arcana.core.data.row import DataRow
from arcana.core.data.set import Dataset
from arcana.core.data.store import DataStore
from arcana.core.exceptions import ArcanaUsageError
import arcana.data.formats.common
from arcana.core.data.space import DataSpace

if ty.TYPE_CHECKING:
    from .image import ContainerImageWithCommand

logger = logging.getLogger("arcana")


@attrs.define
class CommandInput:
    name: str  # How the input will be referred to in the XNAT dialog, defaults to the task_field name
    description: str  # description of the input
    format: type = attrs.field(converter=format_resolver)
    path: str = attrs.field()
    stored_format: type = attrs.field(
        converter=format_resolver
    )  # the format the input is stored in the data store in
    task_field: str = attrs.field()  # Must match the name of the Pydra task input
    row_frequency: DataSpace = None

    @format.default
    def format_default(self):
        return arcana.data.formats.common.File

    @stored_format.default
    def stored_format_default(self):
        return self.format

    @task_field.default
    def field_default(self):
        return self.name

    @path.default
    def path_default(self):
        return self.name

    def command_config_arg(self):
        """a formatted command argument that can be passed to the
        `arcana deploy run-in-image` command
        """
        return (
            f"--input-config {self.name} {self.stored_format.class_location()} "
            f"{self.task_field} {self.format.class_location()}"
        )


@attrs.define
class CommandOutput:
    name: str
    description: str  # description of the input
    path: str = attrs.field()  # The path the output is stored at in XNAT
    format: type = attrs.field(converter=format_resolver)
    task_field: str = (
        attrs.field()
    )  # Must match the name of the Pydra task output, defaults to the path
    stored_format: type = attrs.field(
        converter=format_resolver
    )  # the format the output is to be stored in the data store in

    @format.default
    def format_default(self):
        return arcana.data.formats.common.File

    @stored_format.default
    def stored_format_default(self):
        return self.format

    @task_field.default
    def field_default(self):
        return self.name

    @path.default
    def path_default(self):
        return self.name

    def command_config_arg(self):
        """a formatted command argument that can be passed to the
        `arcana deploy run-in-image` command
        """
        return (
            f"--output-config {self.name} {self.stored_format.class_location()} "
            f"{self.task_field} {self.format.class_location()}"
        )


@attrs.define
class CommandParameter:
    name: str  # How the input will be referred to in the XNAT dialog, defaults to task_field name
    description: str  # description of the parameter
    type: type = attrs.field(converter=resolve_class)
    task_field: str = attrs.field()  # Name of parameter to expose in Pydra task
    required: bool = False
    default: ty.Union[str, int, float, bool] = None

    @task_field.default
    def task_field_default(self):
        return path2varname(self.name)

    def command_config_arg(self):
        """a formatted command argument that can be passed to the
        `arcana deploy run-in-image` command
        """
        return (
            f"--parameter-config {self.name} {self.task_field} "
            f"{class_location(self.type)} {self.default}"
        )


@attrs.define(kw_only=True)
class ContainerCommand:

    STORE_TYPE = "file"
    DATA_SPACE = None

    task: pydra.engine.task.TaskBase = attrs.field(converter=task_resolver)
    row_frequency: DataSpace = None
    inputs: list[CommandInput] = attrs.field(
        factory=list, converter=ListDictConverter(CommandInput)
    )
    outputs: list[CommandOutput] = attrs.field(
        factory=list, converter=ListDictConverter(CommandOutput)
    )
    parameters: list[CommandParameter] = attrs.field(
        factory=list, converter=ListDictConverter(CommandParameter)
    )
    configuration: dict[str, ty.Any] = attrs.field(
        factory=dict, converter=default_if_none(dict)
    )
    image: ContainerImageWithCommand = None

    def __attrs_post_init__(self):
        if isinstance(self.row_frequency, DataSpace):
            pass
        elif isinstance(self.row_frequency, str):
            try:
                self.row_frequency = DataSpace.fromstr(self.row_frequency)
            except ValueError:
                if self.DATA_SPACE:
                    self.row_frequency = self.DATA_SPACE[self.row_frequency]
                else:
                    raise ValueError(
                        f"Cannot par'{self.row_frequency}' cannot be resolved to a data space, "
                        "needs to be of form <data-space-enum>[<row-frequency-name>]"
                    )
        elif self.DATA_SPACE:
            self.row_frequency = self.DATA_SPACE.default()
        else:
            raise ValueError(
                f"Value for row_frequency must be provided to {type(self).__name__}.__init__ "
                "because it doesn't have a defined DATA_SPACE class attribute"
            )

    @property
    def name(self):
        if self.image is None:
            raise RuntimeError(
                f"Cannot access name of unbound container commands {self}"
            )
        return self.image.name

    @property
    def data_space(self):
        return type(self.row_frequency)

    def command_line(
        self,
        dataset_id_str: str = None,
        options: list[str] = (),
    ):
        """Generate a command line to run the command from within the container

        Parameters
        ----------
        dataset_id_str : str
            the ID str of a dataset relative to a data store,
            e.g. file///absolute/path/to/dataset or xnat-cs//MY_PROJECT
        options : list[str]
            options to the arcana deploy run-in-image command that are fixed for the
            given deployment

        Returns
        -------
        str
            generated commandline
        """

        cmdline = (
            f"conda run --no-capture-output -n {self.image.CONDA_ENV} "  # activate conda
            f"arcana deploy image-entrypoint " + " ".join(options)
        )
        if dataset_id_str is not None:
            cmdline += f" {dataset_id_str} "

        return cmdline

    def configuration_args(self):

        # Set up fixed arguments used to configure the workflow at initialisation
        cmd_args = []
        if self.configuration is not None:
            for cname, cvalue in self.configuration.items():
                cvalue_json = json.dumps(cvalue)
                cmd_args.append(f"--configuration {cname} '{cvalue_json}' ")

        return cmd_args

    def license_args(self):
        cmd_args = []
        for lic_name, lic in self.image.licenses.items():
            if lic.source is None:
                cmd_args.append(f"--download-license {lic_name} {lic.destination}")
        return cmd_args

    def run(
        self,
        dataset: Dataset,
        input_values: dict[str, str],
        output_values: dict[str, str],
        parameter_values: dict[str, ty.Any],
        pipeline_cache_dir: Path,
        plugin: str,
        ids: list[str],
        overwrite: bool = False,
        export_work: Path = False,
        raise_errors: bool = False,
        keep_running_on_errors=False,
    ):
        """Runs the command within the entrypoint of the container image.

        Performs a number of steps in one long pipeline that would typically be done
        in separate command calls when running manually, i.e.:

            * Loads a dataset, creating if it doesn't exist
            * create input and output columns if they don't exist
            * applies the pipeline to the dataset
            * runs the pipeline

        Parameters
        ----------
        dataset : Dataset
            dataset ID str (<store-nickname>//<dataset-id>:<dataset-name>)
        input_values : dict[str, str]
            values passed to the inputs of the command
        output_values : dict[str, str]
            values passed to the outputs of the command
        parameter_values : dict[str, ty.Any]
            values passed to the parameters of the command
        store_cache_dir : Path
            cache path used to download data from the store to the working node (if necessary)
        pipeline_cache_dir : Path
            cache path created when running the pipelines
        plugin : str
            Pydra plugin used to execute the pipeline
        ids : list[str]
            IDs of the dataset rows to run the pipeline over
        overwrite : bool, optional
            overwrite existing outputs
        export_work : Path
            export work directory to an alternate location after the workflow is run
            (e.g. for forensics)
        raise_errors : bool
            raise errors instead of capturing and logging (for debugging)
        """

        # Install required software licenses from store into container
        licenses_to_download = [
            lic.name for lic in self.image.licenses.values() if lic.source is None
        ]
        dataset.install_licenses(licenses_to_download)

        input_values = dict(input_values)
        output_values = dict(output_values)
        parameter_values = dict(parameter_values)

        pipeline_inputs = []
        converter_args = {}  # Arguments passed to converter
        for inpt in self.inputs:
            if not input_values[inpt.name] and inpt.format != DataRow:
                logger.warning(
                    f"Skipping '{inpt.name}' source column as no input was provided"
                )
                continue
            pipeline_inputs.append(
                PipelineInput(
                    col_name=inpt.name,
                    task_field=inpt.task_field,
                    required_format=inpt.format,
                )
            )
            if DataRow in (inpt.stored_format, inpt.format):
                if (inpt.stored_format, inpt.format) != (DataRow, DataRow):
                    raise ArcanaUsageError(
                        "Cannot convert to/from built-in data type `DataRow`: "
                        f"col_format={inpt.stored_format}, format={inpt.format}"
                    )
                logger.info(
                    f"No column added for '{inpt.name}' column as it uses built-in "
                    "type `arcana.core.data.row.DataRow`"
                )
                continue
            path, qualifiers = self.extract_qualifiers_from_path(
                input_values[inpt.name]
            )
            source_kwargs = qualifiers.pop("criteria", {})
            converter_args[inpt.name] = qualifiers.pop("converter", {})
            if qualifiers:
                raise ArcanaUsageError(
                    "Unrecognised qualifier namespaces extracted from path for "
                    f"{inpt.name} (expected ['criteria', 'converter']): {qualifiers}"
                )
            if inpt.name in dataset.columns:
                column = dataset[inpt.name]
                logger.info(f"Found existing source column {column}")
            else:
                logger.info(f"Adding new source column '{inpt.name}'")
                dataset.add_source(
                    name=inpt.name,
                    format=inpt.stored_format,
                    path=path,
                    is_regex=True,
                    **source_kwargs,
                )

        logger.debug("Pipeline inputs: %s", pipeline_inputs)

        pipeline_outputs = []
        for output in self.outputs:
            pipeline_outputs.append(
                PipelineOutput(output.name, output.task_field, output.format)
            )
            path, qualifiers = self.extract_qualifiers_from_path(
                output_values.get(output.name, output.name)
            )
            converter_args[output.name] = qualifiers.pop("converter", {})
            if qualifiers:
                raise ArcanaUsageError(
                    "Unrecognised qualifier namespaces extracted from path for "
                    f"{output.name} (expected ['criteria', 'converter']): {qualifiers}"
                )
            if output.name in dataset.columns:
                column = dataset[output.name]
                if not column.is_sink:
                    raise ArcanaUsageError(
                        "Output column name '{output.name}' shadows existing source column"
                    )
                logger.info(f"Found existing sink column {column}")
            else:
                logger.info(f"Adding new source column '{output.name}'")
                dataset.add_sink(
                    name=output.name, format=output.stored_format, path=path
                )

        logger.debug("Pipeline outputs: %s", pipeline_outputs)

        kwargs = copy(self.configuration)
        if "name" not in kwargs:
            kwargs["name"] = "workflow_to_run"

        task = self.task(**kwargs)

        for param in self.parameters:
            param_value = parameter_values.get(param.name, None)
            logger.info(
                "Parameter %s (type %s) passed value %s",
                param.name,
                param.type,
                param_value,
            )
            if param_value == "" and param.type is not str:
                param_value = None
                logger.info(
                    "Non-string parameter '%s' passed empty string, setting to NOTHING",
                    param.name,
                )
            if param_value is None:
                if param.default is None:
                    raise RuntimeError(
                        f"A value must be provided to required '{param.name}' parameter"
                    )
                param_value = param.default
                logger.info("Using default value for %s, %s", param.name, param_value)

            # Convert parameter to parameter type
            try:
                param_value = param.type(param_value)
            except ValueError:
                raise ValueError(
                    f"Could not convert value passed to '{param.name}' parameter, "
                    f"{param_value}, into {param.type}"
                )
            setattr(task.inputs, param.task_field, param_value)

        if self.name in dataset.pipelines and not overwrite:
            pipeline = dataset.pipelines[self.name]
            if task != pipeline.workflow:
                raise RuntimeError(
                    f"A pipeline named '{self.name}' has already been applied to "
                    "which differs from one specified. Please use '--overwrite' option "
                    "if this is intentional"
                )
        else:
            pipeline = dataset.apply_pipeline(
                self.name,
                task,
                inputs=pipeline_inputs,
                outputs=pipeline_outputs,
                row_frequency=self.row_frequency,
                overwrite=overwrite,
                converter_args=converter_args,
            )

        # Instantiate the Pydra workflow
        wf = pipeline(cache_dir=pipeline_cache_dir)

        if ids is not None:
            ids = ids.split(",")

        # execute the workflow
        try:
            result = wf(ids=ids, plugin=plugin)
        except Exception:
            msg = show_workflow_errors(
                pipeline_cache_dir, omit_nodes=["per_node", wf.name]
            )
            logger.error(
                "Pipeline failed with errors for the following nodes:\n\n%s", msg
            )
            if raise_errors or not msg:
                raise
            else:
                errors = True
        else:
            logger.info(
                "Pipeline %s ran successfully for the following data rows:\n%s",
                self.name,
                "\n".join(result.output.processed),
            )
            errors = False
        finally:
            if export_work:
                logger.info("Exporting work directory to '%s'", export_work)
                export_work.mkdir(parents=True, exist_ok=True)
                shutil.copytree(pipeline_cache_dir, export_work / "pydra")

        # Abort at the end after the working directory can be copied back to the
        # host so that XNAT knows there was an error
        if errors:
            if keep_running_on_errors:
                while True:
                    pass
            else:
                sys.exit(1)

    @classmethod
    def extract_qualifiers_from_path(cls, user_input: str):
        """Extracts out "qualifiers" from the user-inputted paths. These are
        in the form 'path ns1.arg1=val1 ns1.arg2=val2, ns2.arg1=val3...

        Parameters
        ----------
        col_name : str
            name of the column the
        user_input : str
            The path expression + qualifying keyword args to extract

        Returns
        -------
        path : str
            the path expression stripped of qualifiers
        qualifiers : defaultdict[dict]
            the extracted qualifiers
        """
        qualifiers = defaultdict(dict)
        if "=" in user_input:  # Treat user input as containing qualifiers
            parts = re.findall(r'(?:[^\s"]|"(?:\\.|[^"])*")+', user_input)
            path = parts[0].strip('"')
            for part in parts[1:]:
                try:
                    full_name, val = part.split("=", maxsplit=1)
                except ValueError as e:
                    e.args = ((e.args[0] + f" attempting to split '{part}' by '='"),)
                    raise e
                try:
                    ns, name = full_name.split(".", maxsplit=1)
                except ValueError as e:
                    e.args = (
                        (e.args[0] + f" attempting to split '{full_name}' by '.'"),
                    )
                    raise e
                try:
                    val = json.loads(val)
                except json.JSONDecodeError:
                    pass
                qualifiers[ns][name] = val
        else:
            path = user_input
        return path, qualifiers

    def load_dataset(
        self,
        dataset_id_str: str,
        cache_dir: Path,
        dataset_hierarchy: str,
        dataset_name: str,
    ):
        """Loads a dataset from within an image, to be used in image entrypoints

        Parameters
        ----------
        dataset_id_str : str
            dataset ID str
        cache_dir : Path
            the directory to use for the store cache
        dataset_hierarchy : str, optional
            the hierarchy of the dataset
        dataset_name : str
            overwrite dataset name loaded from ID str

        Returns
        -------
        _type_
            _description_
        """
        try:
            dataset = Dataset.load(dataset_id_str)
        except KeyError:

            store_name, id, name = Dataset.parse_id_str(dataset_id_str)

            if dataset_name is not None:
                name = dataset_name

            store = DataStore.load(store_name, cache_dir=cache_dir)

            if dataset_hierarchy is None:
                hierarchy = self.data_space.default().span()
            else:
                hierarchy = dataset_hierarchy.split(",")

            try:
                dataset = store.load_dataset(
                    id, name
                )  # FIXME: Does this need to be here or this covered by L253??
            except KeyError:
                dataset = store.new_dataset(
                    id, hierarchy=hierarchy, space=self.data_space
                )
        return dataset
