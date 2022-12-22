from __future__ import annotations
import attrs
import typing as ty
from collections import OrderedDict
from pathlib import Path
import logging
from copy import copy, deepcopy
import attrs.converters
import pydra.mark
from pydra.engine.core import Workflow
from arcana.core.exceptions import (
    ArcanaNameError,
    ArcanaUsageError,
    ArcanaDesignError,
    ArcanaPipelinesStackError,
    ArcanaOutputNotProducedException,
    ArcanaDataMatchError,
    ArcanaFormatConversionError,
)
from ..data.type.base import DataType, FileGroup, Field
import arcana.core.data.set
import arcana.core.data.row
from ..data.space import DataSpace
from ..utils.misc import (
    func_task,
    pydra_eq,
    path2varname,
    add_exc_note,
)
from ..utils.serialize import (
    asdict,
    fromdict,
    pydra_asdict,
    pydra_fromdict,
    ClassResolver,
    ObjectListConverter,
)


logger = logging.getLogger("arcana")


@attrs.define
class PipelineField:
    """Defines an input to a pipeline

    Parameters
    ----------
    name : str
        Name of the input and how it will be referred to in UI
    field : str, optional
        the name of the pydra input field to connect to, defaults to name
    datatype : type, optional
        the type of the items to be passed to the input, arcana.dirtree.data.File by default
    """

    name: str
    field: str = attrs.field()
    datatype: type = attrs.field(
        default=None,
        converter=ClassResolver(
            DataType,
            allow_none=True,
            alternative_types=[arcana.core.data.row.DataRow],
        ),
    )

    @field.default
    def field_default(self):
        return self.name


logger = logging.getLogger("arcana")


@attrs.define
class Pipeline:
    """A thin wrapper around a Pydra workflow to link it to sources and sinks
    within a dataset

    Parameters
    ----------
    name : str
        the name of the pipeline, used to differentiate it from others
    row_frequency : DataSpace, optional
        The row_frequency of the pipeline, i.e. the row_frequency of the
        derivatvies within the dataset, e.g. per-session, per-subject, etc,
        by default None
    workflow : Workflow
        The pydra workflow that performs the actual analysis
    inputs : Sequence[ty.Union[str, ty.Tuple[str, type]]]
        List of column names (i.e. either data sources or sinks) to be
        connected to the inputs of the pipeline. If the pipelines requires
        the input to be in a datatype to the source, then it can be specified
        in a tuple (NAME, FORMAT)
    outputs : Sequence[ty.Union[str, ty.Tuple[str, type]]]
        List of sink names to be connected to the outputs of the pipeline
        If the input to be in a specific datatype, then it can be provided in
        a tuple (NAME, FORMAT)
    converter_args : dict[str, dict]
        keyword arguments passed on to the converter to control how the
        conversion is performed.
    dataset : Dataset
        the dataset the pipeline has been applied to
    """

    name: str = attrs.field()
    row_frequency: DataSpace = attrs.field()
    workflow: Workflow = attrs.field(eq=attrs.cmp_using(pydra_eq))
    inputs: list[PipelineField] = attrs.field(
        converter=ObjectListConverter(PipelineField)
    )
    outputs: list[PipelineField] = attrs.field(
        converter=ObjectListConverter(PipelineField)
    )
    converter_args: ty.Dict[str, dict] = attrs.field(
        factory=dict, converter=attrs.converters.default_if_none(factory=dict)
    )
    dataset: arcana.core.data.set.Dataset = attrs.field(
        metadata={"asdict": False}, default=None, eq=False, hash=False
    )

    def __attrs_post_init__(self):
        for field in self.inputs + self.outputs:
            if field.datatype is None:
                field.datatype = self.dataset[field.name].datatype

    @inputs.validator
    def inputs_validator(self, _, inputs: list[PipelineField]):
        for inpt in inputs:
            if inpt.datatype is arcana.core.data.row.DataRow:  # special case
                continue
            if self.dataset:
                column = self.dataset[inpt.name]
                # Check that a converter can be found if required
                if inpt.datatype:
                    try:
                        inpt.datatype.find_converter(column.datatype)
                    except ArcanaFormatConversionError as e:
                        msg = (
                            f"required to in conversion of '{inpt.name}' input "
                            f"to '{self.name}' pipeline"
                        )
                        add_exc_note(e, msg)
                        raise
            elif inpt.datatype is None:
                raise ValueError(
                    f"Datatype must be explicitly set for {inpt.name} in unbound Pipeline"
                )
            if inpt.field not in self.workflow.input_names:
                raise ArcanaNameError(
                    inpt.field,
                    f"{inpt.field} is not in the input spec of '{self.name}' "
                    f"pipeline: " + "', '".join(self.workflow.input_names),
                )

    @outputs.validator
    def outputs_validator(self, _, outputs: list[PipelineField]):
        for outpt in outputs:
            if self.dataset:
                column = self.dataset[outpt.name]
                if column.row_frequency != self.row_frequency:
                    raise ArcanaUsageError(
                        f"Pipeline row_frequency ('{str(self.row_frequency)}') doesn't match "
                        f"that of '{outpt.name}' output ('{str(self.row_frequency)}')"
                    )
                # Check that a converter can be found if required
                if outpt.datatype:
                    try:
                        column.datatype.find_converter(outpt.datatype)
                    except ArcanaFormatConversionError as e:
                        msg = (
                            f"required to in conversion of '{outpt.name}' output "
                            f"from '{self.name}' pipeline"
                        )
                        add_exc_note(e, msg)
                        raise
            elif outpt.datatype is None:
                raise ValueError(
                    f"Datatype must be explicitly set for {outpt.name} in unbound Pipeline"
                )
            if outpt.field not in self.workflow.output_names:
                raise ArcanaNameError(
                    f"{outpt.field} is not in the output spec of '{self.name}' "
                    f"pipeline: " + "', '".join(self.workflow.output_names)
                )

    @property
    def input_varnames(self):
        return [
            i.name for i in self.inputs
        ]  # [path2varname(i.name) for i in self.inputs]

    @property
    def output_varnames(self):
        return [
            o.name for o in self.outputs
        ]  # [path2varname(o.name) for o in self.outputs]

    # parameterisation = self.get_parameterisation(kwargs)
    # self.wf.to_process.inputs.parameterisation = parameterisation
    # self.wf.per_node.source.inputs.parameterisation = parameterisation

    def __call__(self, **kwargs):
        """
        Create an "outer" workflow that interacts with the dataset to pull input
        data, process it and then push the derivatives back to the store.

        Parameters
        ----------
        **kwargs
            passed directly to the Pydra.Workflow init. The `ids` arg can be
            used to filter the data rows over which the pipeline is run.

        Returns
        -------
        pydra.Workflow
            a Pydra workflow that iterates through the dataset, pulls data to the
            processing node, executes the analysis workflow on each data row,
            then uploads the outputs back to the data store

        Raises
        ------
        ArcanaUsageError
            If the new pipeline will overwrite an existing pipeline connection
            with overwrite == False.
        """

        # Create the outer workflow to link the analysis workflow with the
        # data row iteration and store connection rows
        wf = Workflow(name=self.name, input_spec=["ids"], **kwargs)

        # Generate list of rows to process checking existing outputs
        wf.add(
            to_process(
                dataset=self.dataset,
                row_frequency=self.row_frequency,
                outputs=self.outputs,
                requested_ids=None,  # FIXME: Needs to be set dynamically
                name="to_process",
            )
        )

        # Create the workflow that will be split across all rows for the
        # given data row_frequency
        wf.add(
            Workflow(
                name="per_row", input_spec=["id"], id=wf.to_process.lzout.ids
            ).split("id")
        )

        # Automatically output interface for source node to include sourced
        # columns
        source_out_dct = {}
        for inpt in self.inputs:
            # If the row frequency of the column is not a parent of the pipeline
            # then the input will be a sequence of all the child rows
            if inpt.datatype is arcana.core.data.row.DataRow:
                dtype = arcana.core.data.row.DataRow
            else:
                dtype = self.dataset[inpt.name].datatype
                # If the row frequency of the source column is higher than the frequency
                # of the pipeline, then the related elements of the source column are
                # collected into a list and passed to the pipeline
                if not self.dataset[inpt.name].row_frequency.is_parent(
                    self.row_frequency, if_match=True
                ):
                    dtype = list[dtype]
            source_out_dct[inpt.name] = dtype
        source_out_dct["provenance_"] = ty.Dict[str, ty.Any]

        wf.per_row.add(
            func_task(
                source_items,
                in_fields=[
                    ("dataset", arcana.core.data.set.Dataset),
                    ("row_frequency", DataSpace),
                    ("id", str),
                    ("inputs", ty.List[PipelineField]),
                    ("parameterisation", ty.Dict[str, ty.Any]),
                ],
                out_fields=list(source_out_dct.items()),
                name="source",
                dataset=self.dataset,
                row_frequency=self.row_frequency,
                inputs=self.inputs,
                id=wf.per_row.lzin.id,
            )
        )

        # Set the inputs
        sourced = {
            i.name: getattr(wf.per_row.source.lzout, i.name) for i in self.inputs
        }

        # Do input datatype conversions if required
        for inpt in self.inputs:
            if inpt.datatype == arcana.core.data.row.DataRow:
                continue
            stored_format = self.dataset[inpt.name].datatype
            if not (
                inpt.datatype is stored_format
                or issubclass(stored_format, inpt.datatype)
            ):
                logger.info(
                    "Adding implicit conversion for input '%s' " "from %s to %s",
                    inpt.name,
                    stored_format.class_name(),
                    inpt.datatype.class_name(),
                )
                source_name = inpt.name
                converter = inpt.datatype.converter_task(
                    stored_format,
                    name=f"{source_name}_input_converter",
                    **self.converter_args.get(inpt.name, {}),
                )
                converter.inputs.to_convert = sourced.pop(source_name)
                if issubclass(source_out_dct[source_name], ty.Sequence):
                    # Iterate over all items in the sequence and convert them
                    # separately
                    converter.split("to_convert")
                # Insert converter
                wf.per_row.add(converter)
                # Map converter output to input_interface
                sourced[source_name] = converter.lzout.converted

        # Create identity row to accept connections from user-defined rows
        # via `set_output` method
        wf.per_row.add(
            func_task(
                access_paths_and_values,
                in_fields=[
                    (i.name, ty.Union[DataType, arcana.core.data.row.DataRow])
                    for i in self.inputs
                ],
                out_fields=[(i.name, ty.Any) for i in self.inputs],
                name="input_interface",
                **sourced,
            )
        )

        # Add the "inner" workflow of the pipeline that actually performs the
        # analysis/processing
        wf.per_row.add(deepcopy(self.workflow))
        # Make connections to "inner" workflow
        for inpt in self.inputs:
            setattr(
                getattr(wf.per_row, self.workflow.name).inputs,
                inpt.field,
                getattr(wf.per_row.input_interface.lzout, inpt.name),
            )

        # Creates a row to accept values from user-defined rows and
        # encapsulate them into DataTypes
        wf.per_row.add(
            func_task(
                encapsulate_paths_and_values,
                in_fields=[("outputs", ty.List[PipelineField])]
                + [(o.name, ty.Union[str, Path]) for o in self.outputs],
                out_fields=[(o.name, DataType) for o in self.outputs],
                name="output_interface",
                outputs=self.outputs,
                **{
                    o.name: getattr(
                        getattr(wf.per_row, self.workflow.name).lzout, o.field
                    )
                    for o in self.outputs
                },
            )
        )

        # Set datatype converters where required
        to_sink = {
            o: getattr(wf.per_row.output_interface.lzout, o)
            for o in self.output_varnames
        }

        # Do output datatype conversions if required
        for outpt in self.outputs:
            stored_format = self.dataset[outpt.name].datatype
            if not (
                outpt.datatype is stored_format
                or issubclass(outpt.datatype, stored_format)
            ):
                logger.info(
                    "Adding implicit conversion for output '%s' " "from %s to %s",
                    outpt.name,
                    outpt.datatype.class_name(),
                    stored_format.class_name(),
                )
                # Insert converter
                sink_name = path2varname(outpt.name)
                converter = stored_format.converter_task(
                    outpt.datatype,
                    name=f"{sink_name}_output_converter",
                    **self.converter_args.get(outpt.name, {}),
                )
                converter.inputs.to_convert = to_sink.pop(sink_name)
                wf.per_row.add(converter)
                # Map converter output to workflow output
                to_sink[sink_name] = converter.lzout.converted

        # Can't use a decorated function as we need to allow for dynamic
        # arguments
        wf.per_row.add(
            func_task(
                sink_items,
                in_fields=(
                    [
                        ("dataset", arcana.core.data.set.Dataset),
                        ("row_frequency", DataSpace),
                        ("id", str),
                        ("provenance", ty.Dict[str, ty.Any]),
                    ]
                    + [(s, DataType) for s in to_sink]
                ),
                out_fields=[("id", str)],
                name="sink",
                dataset=self.dataset,
                row_frequency=self.row_frequency,
                id=wf.per_row.lzin.id,
                provenance=wf.per_row.source.lzout.provenance_,
                **to_sink,
            )
        )

        wf.per_row.set_output([("id", wf.per_row.sink.lzout.id)])

        wf.set_output(
            [
                ("processed", wf.per_row.lzout.id),
                ("couldnt_process", wf.to_process.lzout.cant_process),
            ]
        )

        return wf

    PROVENANCE_VERSION = "1.0"
    WORKFLOW_NAME = "processing"

    def asdict(self, required_modules=None):
        dct = asdict(self, omit=["workflow"], required_modules=required_modules)
        dct["workflow"] = pydra_asdict(self.workflow, required_modules=required_modules)
        return dct

    @classmethod
    def fromdict(cls, dct, **kwargs):
        return fromdict(dct, workflow=pydra_fromdict(dct["workflow"]), **kwargs)

    @classmethod
    def stack(cls, *sinks):
        """Determines the pipelines stack, in order of execution,
        required to generate the specified sink columns.

        Parameters
        ----------
        sinks : Iterable[DataSink or str]
            the sink columns, or their names, that are to be generated

        Returns
        -------
        list[tuple[Pipeline, list[DataSink]]]
            stack of pipelines required to produce the specified data sinks,
            along with the sinks each stage needs to produce.

        Raises
        ------
        ArcanaDesignError
            when there are circular references in the pipelines stack
        """

        # Stack of pipelines to process in reverse order of required execution
        stack = OrderedDict()

        def push_pipeline_on_stack(sink, downstream: ty.Tuple[Pipeline] = None):
            """
            Push a pipeline onto the stack of pipelines to be processed,
            detecting common upstream pipelines and resolving them to a single
            pipeline

            Parameters
            ----------
            sink: DataSink
                the sink to push its deriving pipeline for
            downstream : tuple[Pipeline]
                The pipelines directly downstream of the pipeline to be added.
                Used to detect circular dependencies
            """
            if downstream is None:
                downstream = []
            if sink.pipeline_name is None:
                raise ArcanaDesignError(
                    f"{sink} hasn't been connected to a pipeline yet"
                )
            pipeline = sink.dataset.pipelines[sink.pipeline_name]
            if sink.name not in pipeline.output_varnames:
                raise ArcanaOutputNotProducedException(
                    f"{pipeline.name} does not produce {sink.name}"
                )
            # Check downstream piplines for circular dependencies
            downstream_pipelines = [p for p, _ in downstream]
            if pipeline in downstream_pipelines:
                recur_index = downstream_pipelines.index(pipeline)
                raise ArcanaDesignError(
                    f"{pipeline} cannot be a dependency of itself. Call-stack:\n"
                    + "\n".join(
                        "{} ({})".format(p, ", ".join(ro))
                        for p, ro in (
                            [[pipeline, sink.name]] + downstream[: (recur_index + 1)]
                        )
                    )
                )
            if pipeline.name in stack:
                # Pop pipeline from stack in order to add it to the end of the
                # stack and ensure it is run before all downstream pipelines
                prev_pipeline, to_produce = stack.pop(pipeline.name)
                assert pipeline is prev_pipeline
                # Combined required output to produce
                to_produce.append(sink)
            else:
                to_produce = []
            # Add the pipeline to the stack
            stack[pipeline.name] = pipeline, to_produce
            # Recursively add all the pipeline's prerequisite pipelines to the stack
            for inpt in pipeline.inputs:
                inpt_column = sink.dataset[inpt.name]
                if inpt_column.is_sink:
                    try:
                        push_pipeline_on_stack(
                            inpt_column,
                            downstream=[(pipeline, to_produce)] + downstream,
                        )
                    except ArcanaPipelinesStackError as e:
                        e.msg += (
                            "\nwhich are required as inputs to the '{}' "
                            "pipeline to produce '{}'".format(
                                pipeline.name, "', '".join(s.name for s in to_produce)
                            )
                        )
                        raise e

        # Add all pipelines
        for sink in sinks:
            push_pipeline_on_stack(sink)

        return reversed(stack.values())


def append_side_car_suffix(name, suffix):
    """Creates a new combined field name out of a basename and a side car"""
    return f"{name}__o__{suffix}"


def split_side_car_suffix(name):
    """Splits the basename from a side car sufix (as combined by `append_side_car_suffix`"""
    return name.split("__o__")


@pydra.mark.task
@pydra.mark.annotate({"return": {"ids": ty.List[str], "cant_process": ty.List[str]}})
def to_process(
    dataset: arcana.core.data.set.Dataset,
    row_frequency: DataSpace,
    outputs: ty.List[PipelineField],
    requested_ids: ty.Union[list[str], None],
    parameterisation: dict[str, ty.Any],
):
    if requested_ids is None:
        requested_ids = dataset.row_ids(row_frequency)
    ids = []
    cant_process = []
    for row in dataset.rows(row_frequency, ids=requested_ids):
        # TODO: Should check provenance of existing rows to see if it matches
        not_exist = [not row[o.name].exists for o in outputs]
        if all(not_exist):
            ids.append(row.id)
        elif any(not_exist):
            cant_process.append(row.id)
    logger.debug("Found %s ids to process, and can't process %s", ids, cant_process)
    return ids, cant_process


def source_items(
    dataset: arcana.core.data.set.Dataset,
    row_frequency: DataSpace,
    id: str,
    inputs: ty.List[PipelineField],
    parameterisation: dict,
):
    """Selects the items from the dataset corresponding to the input
    sources and retrieves them from the store to a cache on
    the host

    Parameters
    ----------
    dataset : Dataset
        the dataset to source the data from
    row_frequency : DataSpace
        the frequency of the row to source the data from
    id : str
        the ID of the row to source from
    parameterisation : dict
        provenance information... can't remember why this was used here...
    """
    logger.debug("Sourcing %s", inputs)
    provenance = copy(parameterisation)
    sourced = []
    row = dataset.row(row_frequency, id)
    with dataset.store:
        missing_inputs = {}
        for inpt in inputs:
            # If the required datatype is of type DataRow then provide the whole
            # row to the pipeline input
            if inpt.datatype == arcana.core.data.row.DataRow:
                sourced.append(row)
                continue
            try:
                item = row[inpt.name]
            except ArcanaDataMatchError as e:
                missing_inputs[inpt.name] = str(e)
            else:
                item.get()  # download to host if required
                sourced.append(item)
        if missing_inputs:
            raise ArcanaDataMatchError("\n\n" + "\n\n".join(missing_inputs.values()))
    return tuple(sourced) + (provenance,)


def sink_items(dataset, row_frequency, id, provenance, **to_sink):
    """Stores items generated by the pipeline back into the store

    Parameters
    ----------
    dataset : Dataset
        the dataset to source the data from
    row_frequency : DataSpace
        the frequency of the row to source the data from
    id : str
        the ID of the row to source from
    provenance : dict
        provenance information to be stored alongside the generated data
    **to_sink : dict[str, DataType]
        data items to be stored in the data store
    """
    logger.debug("Sinking %s", to_sink)
    row = dataset.row(row_frequency, id)
    with dataset.store:
        for outpt_name, output in to_sink.items():
            row_item = row[outpt_name]
            row_item.put(output.value)  # Store value/path
    return id


def access_paths_and_values(**data_items):
    """Copies files into the CWD renaming so the basenames match
    except for extensions"""
    logger.debug("Extracting paths/values from %s", data_items)
    values = []
    for name, item in data_items.items():
        if isinstance(item, FileGroup):
            cpy = item.copy_to(Path.cwd() / name, symlink=True)
            values.append(cpy.fs_path)
        elif isinstance(item, Field):
            values.append(item.value)
        else:
            values.append(item)
    return tuple(values) if len(values) > 1 else values[0]


def encapsulate_paths_and_values(outputs, **kwargs):
    """Copies files into the CWD renaming so the basenames match
    except for extensions"""
    logger.debug("Encapsulating %s into %s", kwargs, outputs)
    items = []
    for outpt in outputs:
        val = kwargs[outpt.name]
        if issubclass(outpt.datatype, FileGroup):
            obj = outpt.datatype.from_fs_path(val)
        else:
            obj = outpt.datatype(val)
        items.append(obj)
    if len(items) > 1:
        return tuple(items)
    elif items:
        return items[0]
    else:
        return None


# Provenance mismatch detection methods salvaged from data.provenance

# def mismatches(self, other, include=None, exclude=None):
#     """
#     Compares information stored within provenance objects with the
#     exception of version information to see if they match. Matches are
#     constrained to the name_paths passed to the 'include' kwarg, with the
#     exception of sub-name_paths passed to the 'exclude' kwarg

#     Parameters
#     ----------
#     other : Provenance
#         The provenance object to compare against
#     include : list[list[str]] | None
#         Paths in the provenance to include in the match. If None all are
#         incluced
#     exclude : list[list[str]] | None
#         Paths in the provenance to exclude from the match. In None all are
#         excluded
#     """
#     if include is not None:
#         include_res = [self._gen_prov_path_regex(p) for p in include]
#     if exclude is not None:
#         exclude_res = [self._gen_prov_path_regex(p) for p in exclude]
#     diff = DeepDiff(self._prov, other._prov, ignore_order=True)
#     # Create regular expressions for the include and exclude name_paths in
#     # the datatype that deepdiff uses for nested dictionary/lists

#     def include_change(change):
#         if include is None:
#             included = True
#         else:
#             included = any(rx.match(change) for rx in include_res)
#         if included and exclude is not None:
#             included = not any(rx.match(change) for rx in exclude_res)
#         return included

#     filtered_diff = {}
#     for change_type, changes in diff.items():
#         if isinstance(changes, dict):
#             filtered = dict((k, v) for k, v in changes.items()
#                             if include_change(k))
#         else:
#             filtered = [c for c in changes if include_change(c)]
#         if filtered:
#             filtered_diff[change_type] = filtered
#     return filtered_diff

# @classmethod
# def _gen_prov_path_regex(self, file_path):
#     if isinstance(file_path, str):
#         if file_path.startswith('/'):
#             file_path = file_path[1:]
#         regex = re.compile(r"root\['{}'\].*"
#                             .format(r"'\]\['".join(file_path.split('/'))))
#     elif not isinstance(file_path, re.Pattern):
#         raise ArcanaUsageError(
#             "Provenance in/exclude name_paths can either be name_path "
#             "strings or regexes, not '{}'".format(file_path))
#     return regex
