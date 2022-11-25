from __future__ import annotations
import shutil
import re
import json
import logging
import typing as ty
import sys
from collections import defaultdict
import attrs
import pydra.engine.task
from arcana.core.utils import (
    path2varname,
    ListDictConverter,
    format_resolver,
    data_space_resolver,
    task_resolver,
    class_location,
)
from arcana.core.pipeline import Input as PipelineInput, Output as PipelineOutput
from arcana.core.utils import resolve_class, parse_value, show_workflow_errors
from arcana.core.data.row import DataRow
from arcana.core.data.set import Dataset
from arcana.core.data.store import DataStore
from arcana.exceptions import ArcanaUsageError
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
    task_field: str = attrs.field()  # Name of parameter to expose in Pydra task
    type: type = str
    required: bool = False
    default = None

    @task_field.default
    def task_field_default(self):
        return path2varname(self.name)


@attrs.define(kw_only=True)
class ContainerCommand:

    STORE_TYPE = "file"
    DEFAULT_DATA_SPACE = None

    task: pydra.engine.task.TaskBase = attrs.field(converter=task_resolver)
    row_frequency: DataSpace
    data_space: type = attrs.field(converter=data_space_resolver)
    inputs: list[CommandInput] = attrs.field(
        factory=list, converter=ListDictConverter(CommandInput)
    )
    outputs: list[CommandOutput] = attrs.field(
        factory=list, converter=ListDictConverter(CommandOutput)
    )
    parameters: list[CommandParameter] = attrs.field(
        factory=list, converter=ListDictConverter(CommandParameter)
    )
    configuration: dict[str, ty.Any] = None
    image: ContainerImageWithCommand = None

    def __attrs_post_init__(self):
        if isinstance(self.row_frequency, str):
            try:
                self.row_frequency = DataSpace.fromstr(self.row_frequency)
            except ValueError:
                self.row_frequency = self.data_space[self.row_frequency]

    @property
    def name(self):
        if self.image is None:
            raise RuntimeError(
                f"Cannot access name of unbound container commands {self}"
            )
        return self.image.name

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

        data_space = type(self.row_frequency)

        cmdline = (
            f"conda run --no-capture-output -n {self.image.CONDA_ENV} "  # activate conda
            f"arcana deploy run-in-image "
            f"--dataset-space {class_location(data_space)} "
            f"--row-frequency {self.row_frequency} "
            + " ".join(
                [i.command_config_arg() for i in self.inputs]
                + [o.command_config_arg() for o in self.outputs]
                + self.configuration_args()
                + self.license_args()
                + list(options)
            )
            + f" {class_location(self.task)} {self.name} "
        )
        if dataset_id_str is not None:
            cmdline += f"{dataset_id_str} "

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

    @classmethod
    def run(
        cls,
        dataset_id_str: str,
        pipeline_name: str,
        task_cls: type,
        parameters: list[tuple[str, str, str]],
        inputs,
        outputs,
        input_configs,
        output_configs,
        row_frequency,
        overwrite,
        plugin,
        download_licenses,
        dataset_name,
        dataset_space,
        ids,
        configuration,
        single_row,
        export_work,
        raise_errors,
        store_cache_dir,
        pipeline_cache_dir,
        dataset_hierarchy=None,
        keep_running_on_errors=False,
    ):

        # ApplyApply a pipeline to a dataset (creating the dataset if necessary)

        try:
            dataset = Dataset.load(dataset_id_str)
        except KeyError:

            store_name, id, name = Dataset.parse_id_str(dataset_id_str)

            if dataset_name is not None:
                name = dataset_name

            if dataset_space is None:
                raise RuntimeError(
                    f"If the dataset ID string ('{dataset_id_str}') doesn't "
                    "reference an existing dataset '--dataset-space' must be provided"
                )

            store = DataStore.load(store_name, cache_dir=store_cache_dir)
            space = resolve_class(dataset_space, ["arcana.data.spaces"])

            if dataset_hierarchy is None:
                hierarchy = space.default().span()
            else:
                hierarchy = dataset_hierarchy.split(",")

            try:
                dataset = store.load_dataset(
                    id, name
                )  # FIXME: Does this need to be here or this covered by L253??
            except KeyError:
                dataset = store.new_dataset(id, hierarchy=hierarchy, space=space)

        # Install required software licenses from store into container
        dataset.install_licenses(download_licenses)

        if single_row is not None:
            # Adds a single row to the dataset (i.e. skips a full scan)
            dataset.add_leaf(single_row.split(","))

        pipeline_inputs = []
        converter_args = {}  # Arguments passed to converter
        match_criteria = dict(inputs)
        for (
            col_name,
            col_format_name,
            task_field,
            format_name,
        ) in input_configs:
            col_format = resolve_class(
                col_format_name, prefixes=["arcana.data.formats"]
            )
            format = resolve_class(format_name, prefixes=["arcana.data.formats"])
            if not match_criteria[col_name] and format != DataRow:
                logger.warning(
                    f"Skipping '{col_name}' source column as no input was provided"
                )
                continue
            pipeline_inputs.append(PipelineInput(col_name, task_field, format))
            if DataRow in (col_format, format):
                if (col_format, format) != (DataRow, DataRow):
                    raise ArcanaUsageError(
                        "Cannot convert to/from built-in data type `DataRow`: "
                        f"col_format={col_format}, format={format}"
                    )
                logger.info(
                    f"No column added for '{col_name}' column as it uses built-in "
                    "type `arcana.core.data.row.DataRow`"
                )
                continue
            path, qualifiers = cls.extract_qualifiers_from_path(
                match_criteria[col_name]
            )
            source_kwargs = qualifiers.pop("criteria", {})
            converter_args[col_name] = qualifiers.pop("converter", {})
            if qualifiers:
                raise ArcanaUsageError(
                    "Unrecognised qualifier namespaces extracted from path for "
                    f"{col_name} (expected ['criteria', 'converter']): {qualifiers}"
                )
            if col_name in dataset.columns:
                column = dataset[col_name]
                logger.info(f"Found existing source column {column}")
            else:
                logger.info(f"Adding new source column '{col_name}'")
                dataset.add_source(
                    name=col_name,
                    format=col_format,
                    path=path,
                    is_regex=True,
                    **source_kwargs,
                )

        logger.debug("Pipeline inputs: %s", pipeline_inputs)

        pipeline_outputs = []
        output_paths = dict(outputs)
        for col_name, col_format_name, task_field, format_name in output_configs:
            format = resolve_class(format_name, prefixes=["arcana.data.formats"])
            col_format = resolve_class(
                col_format_name, prefixes=["arcana.data.formats"]
            )
            pipeline_outputs.append(PipelineOutput(col_name, task_field, format))
            path, qualifiers = cls.extract_qualifiers_from_path(
                output_paths.get(col_name, col_name)
            )
            converter_args[col_name] = qualifiers.pop("converter", {})
            if qualifiers:
                raise ArcanaUsageError(
                    "Unrecognised qualifier namespaces extracted from path for "
                    f"{col_name} (expected ['criteria', 'converter']): {qualifiers}"
                )
            if col_name in dataset.columns:
                column = dataset[col_name]
                if not column.is_sink:
                    raise ArcanaUsageError(
                        "Output column name '{col_name}' shadows existing source column"
                    )
                logger.info(f"Found existing sink column {column}")
            else:
                logger.info(f"Adding new source column '{col_name}'")
                dataset.add_sink(name=col_name, format=col_format, path=path)

        logger.debug("Pipeline outputs: %s", pipeline_outputs)

        kwargs = {n: parse_value(v) for n, v in configuration}
        if "name" not in kwargs:
            kwargs["name"] = "workflow_to_run"

        task = task_cls(**kwargs)

        for pname, pval in parameters:
            if pval != "":
                setattr(task.inputs, pname, parse_value(pval))

        if pipeline_name in dataset.pipelines and not overwrite:
            pipeline = dataset.pipelines[pipeline_name]
            if task != pipeline.workflow:
                raise RuntimeError(
                    f"A pipeline named '{pipeline_name}' has already been applied to "
                    "which differs from one specified. Please use '--overwrite' option "
                    "if this is intentional"
                )
        else:
            pipeline = dataset.apply_pipeline(
                pipeline_name,
                task,
                inputs=pipeline_inputs,
                outputs=pipeline_outputs,
                row_frequency=row_frequency,
                overwrite=overwrite,
                converter_args=converter_args,
            )

        # Instantiate the Pydra workflow
        wf = pipeline(cache_dir=pipeline_cache_dir)

        if ids is not None:
            ids = ids.split(",")

        # Install dataset-specific licenses within the container
        # dataset.install_licenses(install_licenses)

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
                pipeline_name,
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
