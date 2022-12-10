from __future__ import annotations
import shutil
import re
from copy import copy
import json
import logging
from pathlib import Path
import typing as ty
import sys
import builtins
from collections import defaultdict
import attrs
from attrs.converters import default_if_none
import pydra.engine.task
from pydra.engine.core import TaskBase
from arcana.core.utils.serialize import (
    ObjectListConverter,
    ObjectConverter,
    ClassResolver,
)
from arcana.core.analysis.pipeline import (
    PipelineField,
)
from arcana.core.utils.misc import show_workflow_errors, add_exc_note
from arcana.core.data.row import DataRow
from arcana.core.data.set import Dataset
from arcana.core.data.type import DataType
from arcana.core.data.store import DataStore
from arcana.core.exceptions import ArcanaUsageError, ArcanaFormatConversionError
from arcana.core.data.space import DataSpace

if ty.TYPE_CHECKING:
    from .image import CommandImage


logger = logging.getLogger("arcana")


@attrs.define
class DefaultColumn:
    """
    Values to set the default column of a command input/output

    Parameters
    ----------
    datatype : str
        the type the data items will be stored in (e.g. file-format)
    row_frequency : DataSpace
        the "row-frequency" of the input column to be added
    path : str
        path to where the data will be placed in the repository
    """

    datatype: type = attrs.field(
        default=None, converter=ClassResolver(DataType, allow_none=True)
    )
    row_frequency: DataSpace = None
    path: str = None


@attrs.define(kw_only=True)
class CommandField(PipelineField):
    """An input/output to a command

    Parameters
    ----------
    name : str
        Name of the input and how it will be referred to in UI
    field : str, optional
        the name of the pydra input field to connect to, defaults to name
    datatype : type, optional
        the type of the items to be passed to the input, arcana.data.types.common.File by default
    help_string : str
        short description of the field to be displayed in the UI
    configuration : dict[str, Any] or bool
        Arguments that should be passed onto the the command configuration dict in
        special the ``inputs``, ``outputs`` or ``parameters`` input fields for the given
        field alongside the name and datatype. Should be set to True if just the name
        and dataset
    """

    help_string: str
    configuration: ty.Union[dict[str, ty.Any], bool] = attrs.field(factory=dict)

    @property
    def config_dict(self):
        """Returns a dictionary to be passed to the task/workflow in order to configure
        it to receive input/output

        Parameters
        ----------
        configuration : _type_
            _description_
        list_name : _type_
            _description_
        """
        if self.configuration:
            config = {
                "name": self.name,
                "datatype": self.datatype,
            }
            if isinstance(self.configuration, dict):  # Otherwise just True
                config.update(self.configuration)
        else:
            config = {}
        return config


@attrs.define(kw_only=True)
class CommandInput(CommandField):
    """Defines an input or output to a command

    Parameters
    ----------
    name : str
        Name of the input and how it will be referred to in UI
    field : str, optional
        the name of the pydra input field to connect to, defaults to name
    datatype : type, optional
        the type of the items to be passed to the input
    help_string : str
        description of the input/output field
    configuration : dict
        additional attributes to be used in the configuration of the
        task/workflow/analysis (e.g. ``bids_path`` or ``argstr``). If the configuration
        is not explicitly False (i.e. provided in the YAML definition) then it will
        be passed on as an element in the `inputs` input field to the task/workflow
    default_columm: DefaultColumn, optional
        the values to use to configure a default column if the name doesn't match an
        existing column
    """

    default_column: DefaultColumn = attrs.field(
        converter=ObjectConverter(
            DefaultColumn, allow_none=True, default_if_none=DefaultColumn
        ),
        default=None,
    )

    @default_column.validator
    def default_column_validator(self, _, default_column: DefaultColumn):
        if default_column.datatype is not None:
            try:
                self.datatype.find_converter(default_column.datatype)
            except ArcanaFormatConversionError as e:
                add_exc_note(
                    e,
                    f"required to convert from the default column to the '{self.name}' input",
                )
                raise

    def __attrs_post_init__(self):
        if self.default_column.datatype is None:
            self.default_column.datatype = self.datatype


@attrs.define(kw_only=True)
class CommandOutput(CommandField):
    """Defines an input or output to a command

    Parameters
    ----------
    name : str
        Name of the input and how it will be referred to in UI
    field : str, optional
        the name of the pydra input field to connect to, defaults to name
    datatype : type, optional
        the type of the items to be passed to the input
    help_string : str
        description of the input/output field
    configuration : dict
        additional attributes to be used in the configuration of the
        task/workflow/analysis (e.g. ``bids_path`` or ``argstr``). If the configuration
        is not explicitly False (i.e. provided in the YAML definition) then it will
        be passed on as an element in the `outputs` input field to the task/workflow
    default_columm: DefaultColumn, optional
        the values to use to configure a default column if the name doesn't match an
        existing column
    """

    default_column: DefaultColumn = attrs.field(
        converter=ObjectConverter(
            DefaultColumn, allow_none=True, default_if_none=DefaultColumn
        ),
        default=None,
    )

    @default_column.validator
    def default_column_validator(self, _, default_column: DefaultColumn):
        if default_column.datatype is not None:
            try:
                default_column.datatype.find_converter(self.datatype)
            except ArcanaFormatConversionError as e:
                add_exc_note(
                    e,
                    f"required to convert to the default column from the '{self.name}' output",
                )
                raise

    def __attrs_post_init__(self):
        if self.default_column.datatype is None:
            self.default_column.datatype = self.datatype


@attrs.define(kw_only=True)
class CommandParameter(CommandField):
    """Defines a fixed parameter of the task/workflow/analysis to be exposed in the UI

    Parameters
    ----------
    name : str
        Name of the input and how it will be referred to in UI
    field : str, optional
        the name of the pydra input field to connect to, defaults to name
    datatype : type, optional
        the type of the items to be passed to the input
    help_string : str
        description of the input/output field
    configuration : dict[str, Any]
        additional attributes to be used in the configuration of the
        task/workflow/analysis (e.g. ``bids_path`` or ``argstr``). If the configuration
        is not explicitly False (i.e. provided in the YAML definition) then it will
        be passed on as an element in the `parameters` input field to the task/workflow
    required : bool
        whether the parameter is required or not
    default : Any
        the default value for the parameter, must be able to be
    """

    datatype: ty.Union[int, float, bool, str] = attrs.field(
        converter=lambda x: getattr(builtins, x) if isinstance(x, str) else x
    )
    required: bool = False
    default: ty.Any = None


@attrs.define(kw_only=True)
class ContainerCommand:
    """A definition of a command to be run within a container. A command wraps up a
    task or workflow to provide/configure a UI for convenient launching.

    Parameters
    ----------
    task : pydra.engine.task.TaskBase or str
        the task to run or the location of the class
    row_frequency: DataSpace, optional
        the frequency that the command operates on
    inputs: list[CommandInput]
        inputs of the command
    outputs: list[CommandOutput]
        outputs of the command
    parameters: list[CommandParameter]
        parameters of the command
    configuration: dict[str, ty.Any]
        constant values used to configure the task/workflow
    image: CommandImage
        back-reference to the image the command is installed in
    """

    STORE_TYPE = "file"
    DATA_SPACE = None

    task: pydra.engine.task.TaskBase = attrs.field(
        converter=ClassResolver(TaskBase, alternative_types=[ty.Callable])
    )
    row_frequency: DataSpace = None
    inputs: list[CommandInput] = attrs.field(
        factory=list,
        converter=ObjectListConverter(CommandInput),
        metadata={"serializer": ObjectListConverter.asdict},
    )
    outputs: list[CommandOutput] = attrs.field(
        factory=list,
        converter=ObjectListConverter(CommandOutput),
        metadata={"serializer": ObjectListConverter.asdict},
    )
    parameters: list[CommandParameter] = attrs.field(
        factory=list,
        converter=ObjectListConverter(CommandParameter),
        metadata={"serializer": ObjectListConverter.asdict},
    )
    configuration: dict[str, ty.Any] = attrs.field(
        factory=dict, converter=default_if_none(dict)
    )
    image: CommandImage = None

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
            lic.name for lic in self.image.licenses if lic.source is None
        ]
        dataset.install_licenses(licenses_to_download)

        input_values = dict(input_values)
        output_values = dict(output_values)
        parameter_values = dict(parameter_values)

        input_configs = []
        converter_args = {}  # Arguments passed to converter
        for inpt in self.inputs:
            if not input_values[inpt.name] and inpt.datatype != DataRow:
                logger.warning(
                    f"Skipping '{inpt.name}' source column as no input was provided"
                )
                continue
            if inpt.datatype is DataRow:
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
                    datatype=inpt.default_column.datatype,
                    path=path,
                    is_regex=True,
                    **source_kwargs,
                )
            if input_config := inpt.config_dict:
                input_configs.append(input_config)

        output_configs = []
        for output in self.outputs:
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
                    name=output.name,
                    datatype=output.default_column.datatype,
                    path=path,
                )
            if output_config := output.config_dict:
                output_configs.append(output_config)

        kwargs = copy(self.configuration)

        param_configs = []
        for param in self.parameters:
            param_value = parameter_values.get(param.name, None)
            logger.info(
                "Parameter %s (type %s) passed value %s",
                param.name,
                param.datatype,
                param_value,
            )
            if param_value == "" and param.datatype is not str:
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
                param_value = param.datatype(param_value)
            except ValueError:
                raise ValueError(
                    f"Could not convert value passed to '{param.name}' parameter, "
                    f"{param_value}, into {param.datatype}"
                )
            kwargs[param.field] = param_value
            if param_config := param.config_dict:
                param_configs.append(param_config)

        if "name" not in kwargs:
            kwargs["name"] = "pipeline_task"

        if input_configs:
            kwargs["inputs"] = input_configs
        if output_configs:
            kwargs["outputs"] = output_configs
        if param_configs:
            kwargs["parameters"] = param_configs

        task = self.task(**kwargs)

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
                inputs=self.inputs,
                outputs=self.outputs,
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
