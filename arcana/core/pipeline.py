from __future__ import annotations
import attr
import typing as ty
from collections import OrderedDict
from dataclasses import dataclass
import logging
from copy import copy, deepcopy
import re
from collections.abc import Iterable
import attr
import pydra.mark
from pydra.engine.core import Workflow
from arcana.exceptions import (
    ArcanaNameError, ArcanaUsageError, ArcanaDesignError,
    ArcanaPipelinesStackError, ArcanaOutputNotProducedException)
from .data.format import DataItem, FileGroup
import arcana.core.data.set
from .data.space import DataSpace
from .utils import (
    func_task, asdict, fromdict, pydra_asdict, pydra_fromdict, pydra_eq)

logger = logging.getLogger('arcana')

@dataclass
class Input():
    col_name: str
    pydra_field: str
    required_format: type

@dataclass
class Output():
    col_name: str
    pydra_field: str
    produced_format: type

@attr.s
class Pipeline():
    """A thin wrapper around a Pydra workflow to link it to sources and sinks
    within a dataset

    Parameters
    ----------
    frequency : DataSpace, optional
        The frequency of the pipeline, i.e. the frequency of the
        derivatvies within the dataset, e.g. per-session, per-subject, etc,
        by default None
    workflow : Workflow
        The pydra workflow that performs the actual analysis      
    inputs : Sequence[ty.Union[str, ty.Tuple[str, type]]]
        List of column names (i.e. either data sources or sinks) to be
        connected to the inputs of the pipeline. If the pipelines requires
        the input to be in a format to the source, then it can be specified
        in a tuple (NAME, FORMAT)
    outputs : Sequence[ty.Union[str, ty.Tuple[str, type]]]
        List of sink names to be connected to the outputs of the pipeline
        If the input to be in a specific format, then it can be provided in
        a tuple (NAME, FORMAT)
    """

    name: str = attr.ib()
    frequency: DataSpace = attr.ib()
    workflow: Workflow = attr.ib(
        eq=attr.cmp_using(pydra_eq))
    inputs: ty.List[Input] = attr.ib(
        converter=lambda lst: [Input(*i) if isinstance(i, Iterable) else i
                               for i in lst])
    outputs: ty.List[Output] = attr.ib(
        converter=lambda lst: [Output(*o) if isinstance(o, Iterable) else o
                               for o in lst])
    dataset: arcana.core.data.set.Dataset = attr.ib(
        metadata={'asdict': False}, default=None, eq=False, hash=False)

    @inputs.validator
    def inputs_validator(self, _, inpt):
        column = self.dataset.column[inpt.col_name]
        inpt.required_format.find_converter(column.format)
        if inpt.pydra_field not in self.workflow.input_names:
            raise ArcanaNameError(
                f"{inpt.pydra_field} is not in the input spec of '{self.name}' "
                f"pipeline: " + "', '".join(self.workflow.input_names))

    @outputs.validator
    def outputs_validator(self, _, outpt):
        column = self.dataset.column[outpt.col_name]
        if column.frequency != self.frequency:
            raise ArcanaUsageError(
                f"Pipeline frequency ('{str(self.frequency)}') doesn't match "
                f"that of '{outpt.col_name}' output ('{str(self.frequency)}')")
        column.format.find_converter(outpt.produced_format)
        if outpt.pydra_field not in self.workflow.output_names:
            raise ArcanaNameError(
                f"{outpt.pydra_field} is not in the output spec of '{self.name}' "
                f"pipeline: " + "', '".join(self.workflow.output_names))            

    @property
    def input_col_names(self):
        return [i.col_name for i in self.inputs]

    @property
    def output_col_names(self):
        return [o.col_name for o in self.outputs]

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
            used to filter the data nodes over which the pipeline is run.

        Returns
        -------
        pydra.Workflow
            a Pydra workflow that iterates through the dataset, pulls data to the
            processing node, executes the analysis workflow on each data node,
            then uploads the outputs back to the data store

        Raises
        ------
        ArcanaUsageError
            If the new pipeline will overwrite an existing pipeline connection
            with overwrite == False.
        """

        # Create the outer workflow to link the analysis workflow with the
        # data node iteration and store connection nodes
        wf = Workflow(name=self.name, input_spec=['ids'], **kwargs)

        # Generate list of nodes to process checking existing outputs
        wf.add(to_process(
            dataset=self.dataset,
            frequency=self.frequency,
            outputs=self.outputs,
            requested_ids=None,  # FIXME: Needs to be set dynamically
            name='to_process'))

        # Create the workflow that will be split across all nodes for the 
        # given data frequency
        wf.add(Workflow(
            name='per_node',
            input_spec=['id'],
            id=wf.to_process.lzout.ids).split('id'))

        source_in = [
            ('dataset', arcana.core.data.set.Dataset),
            ('frequency', DataSpace),
            ('id', str),
            ('inputs', ty.Sequence[str]),
            ('parameterisation', ty.Dict[str, ty.Any])]

        source_out_dct = {
            s: (DataItem
                if self.dataset[s].frequency.is_parent(self.frequency,
                                                       if_match=True)
                else ty.Sequence[DataItem])
            for s in self.input_col_names}
        source_out_dct['provenance_'] = ty.Dict[str, ty.Any]

        wf.per_node.add(func_task(
            source_items,
            in_fields=source_in,
            out_fields=list(source_out_dct.items()),
            name='source',
            dataset=self.dataset,
            frequency=self.frequency,
            inputs=self.input_col_names,
            id=wf.per_node.lzin.id))

        # Set the inputs
        sourced = {i: getattr(wf.per_node.source.lzout, i)
                   for i in self.input_col_names}

        # Do input format conversions if required
        for inpt in self.inputs:
            stored_format = self.dataset[inpt.col_name].format
            if not (inpt.required_format is stored_format
                    or issubclass(stored_format, inpt.required_format)):
                logger.info("Adding implicit conversion for input '%s' "
                            "from %s to %s", inpt.col_name, stored_format,
                            inpt.required_format)
                converter = inpt.required_format.converter_task(
                    stored_format, name=f"{inpt.col_name}_input_converter")
                converter.inputs.to_convert = sourced.pop(inpt.col_name)
                if issubclass(source_out_dct[inpt.col_name], ty.Sequence):
                    # Iterate over all items in the sequence and convert them
                    # separately
                    converter.split('to_convert')
                # Insert converter
                wf.per_node.add(converter)
                # Map converter output to input_interface
                sourced[inpt.col_name] = converter.lzout.converted

        # Create identity node to accept connections from user-defined nodes
        # via `set_output` method
        wf.per_node.add(func_task(
            access_paths_and_values,
            in_fields=[(i, DataItem) for i in self.input_col_names],
            out_fields=[(i, ty.Any) for i in self.input_col_names],
            name='input_interface',
            **sourced))

        # Add the "inner" workflow of the pipeline that actually performs the
        # processing
        wf.per_node.add(deepcopy(self.workflow))
        # Make connections to "inner" workflow
        for inpt in self.inputs:
            setattr(getattr(wf.per_node, self.workflow.name).inputs,
                    inpt.pydra_field,
                    getattr(wf.per_node.input_interface.lzout, inpt.col_name))

        # Creates a node to accept values from user-defined nodes and
        # encapsulate them into DataItems
        wf.per_node.add(func_task(
            encapsulate_paths_and_values,
            in_fields=[('outputs', ty.Dict[str, type])] + [
                (o, ty.Any) for o in self.output_col_names],
            out_fields=[(o, DataItem) for o in self.output_col_names],
            name='output_interface',
            outputs=self.outputs,
            **{o.col_name: getattr(
                getattr(wf.per_node, self.workflow.name).lzout, o.pydra_field)
               for o in self.outputs}))

        # Set format converters where required
        to_sink = {o: getattr(wf.per_node.output_interface.lzout, o)
                   for o in self.output_col_names}

        # Do output format conversions if required
        for outpt in self.outputs:
            stored_format = self.dataset[outpt.col_name].format
            if not (outpt.produced_format is stored_format
                    or issubclass(outpt.produced_format, stored_format)):
                logger.info("Adding implicit conversion for output '%s' "
                    "from %s to %s", outpt.col_name, outpt.produced_format,
                    stored_format)
                # Insert converter
                converter = stored_format.converter_task(
                    outpt.produced_format,
                    name=f"{outpt.col_name}_output_converter")
                converter.inputs.to_convert = to_sink.pop(outpt.col_name)
                wf.per_node.add(converter)
                # Map converter output to workflow output
                to_sink[outpt.col_name] = converter.lzout.converted

        # Can't use a decorated function as we need to allow for dynamic
        # arguments
        wf.per_node.add(func_task(
            sink_items,
            in_fields=(
                [('dataset', arcana.core.data.set.Dataset),
                 ('frequency', DataSpace),
                 ('id', str),
                 ('provenance', ty.Dict[str, ty.Any])]
                + [(s, DataItem) for s in to_sink]),
            out_fields=[('id', str)],
            name='sink',
            dataset=self.dataset,
            frequency=self.frequency,
            id=wf.per_node.lzin.id,
            provenance=wf.per_node.source.lzout.provenance_,
            **to_sink))

        wf.per_node.set_output(
            [('id', wf.per_node.sink.lzout.id)])

        wf.set_output(
            [('processed', wf.per_node.lzout.id),
             ('couldnt_process', wf.to_process.lzout.cant_process)])

        return wf

    PROVENANCE_VERSION = '1.0'
    WORKFLOW_NAME = 'processing'

    def asdict(self, required_modules=None):
        dct = asdict(self, omit=['workflow'],
                      required_modules=required_modules)
        dct['workflow'] = pydra_asdict(
            self.workflow,
            required_modules=required_modules)
        return dct

    @classmethod
    def fromdict(cls, dct, **kwargs):
        return fromdict(
            dct,
            workflow=pydra_fromdict(dct['workflow']),
            **kwargs)

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

        def push_pipeline_on_stack(sink, downstream: ty.Tuple[Pipeline]=None):
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
                    f"{sink} hasn't been connected to a pipeline yet")
            pipeline = sink.dataset.pipelines[sink.pipeline_name]
            if sink.name not in pipeline.output_col_names:
                raise ArcanaOutputNotProducedException(
                    f"{pipeline.name} does not produce {sink.name}")
            # Check downstream piplines for circular dependencies
            downstream_pipelines = [p for p, _ in downstream]
            if pipeline in downstream_pipelines:
                recur_index = downstream_pipelines.index(pipeline)
                raise ArcanaDesignError(
                    f"{pipeline} cannot be a dependency of itself. Call-stack:\n"
                    + '\n'.join('{} ({})'.format(p, ', '.join(ro))
                                for p, ro in ([[pipeline, sink.name]]
                                              + downstream[:(recur_index + 1)])))
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
                inpt_column = sink.dataset[inpt.col_name]
                if inpt_column.is_sink:
                    try:
                        push_pipeline_on_stack(
                            inpt_column,
                            downstream=[(pipeline, to_produce)] + downstream)
                    except ArcanaPipelinesStackError as e:
                        e.msg += ("\nwhich are required as inputs to the '{}' "
                                  "pipeline to produce '{}'".format(
                                      pipeline.name,
                                      "', '".join(s.name for s in to_produce)))
                        raise e

        # Add all pipelines
        for sink in sinks:
            push_pipeline_on_stack(sink)

        return reversed(stack.values())

def append_side_car_suffix(name, suffix):
    """Creates a new combined field name out of a basename and a side car"""
    return f'{name}__o__{suffix}'


def split_side_car_suffix(name):
    """Splits the basename from a side car sufix (as combined by `append_side_car_suffix`"""
    return name.split('__o__')


@pydra.mark.task
@pydra.mark.annotate({
    'dataset': arcana.core.data.set.Dataset,
    'frequency': DataSpace,
    'outputs': ty.Sequence[str],
    'requested_ids': ty.Sequence[str] or None,
    'parameterisation': ty.Dict[str, ty.Any],
    'return': {
        'ids': ty.List[str],
        'cant_process': ty.List[str]}})
def to_process(dataset, frequency, outputs, requested_ids, parameterisation):
    if requested_ids is None:
        requested_ids = dataset.node_ids(frequency)
    ids = []
    cant_process = []
    for data_node in dataset.nodes(frequency, ids=requested_ids):
        # TODO: Should check provenance of existing nodes to see if it matches
        not_exist = [not data_node[o.col_name].exists for o in outputs]
        if all(not_exist):
            ids.append(data_node.id)
        elif any(not_exist):
            cant_process.append(data_node.id)
    logger.debug("Found %s ids to process, and can't process %s",
                 ids, cant_process)
    return ids, cant_process


def source_items(dataset, frequency, id, inputs, parameterisation):
    """Selects the items from the dataset corresponding to the input 
    sources and retrieves them from the store to a cache on 
    the host"""
    logger.debug("Sourcing %s", inputs)
    provenance = copy(parameterisation)
    sourced = []
    data_node = dataset.node(frequency, id)
    with dataset.store:
        for inpt_name in inputs:
            item = data_node[inpt_name]
            item.get()  # download to host if required
            sourced.append(item)
    return tuple(sourced) + (provenance,)


def sink_items(dataset, frequency, id, provenance, **to_sink):
    """Stores items generated by the pipeline back into the store"""
    logger.debug("Sinking %s", to_sink)
    data_node = dataset.node(frequency, id)
    with dataset.store:
        for outpt_name, output in to_sink.items():
            node_item = data_node[outpt_name]
            node_item.put(output.value) # Store value/path
    return id


def access_paths_and_values(**data_items):
    """Copies files into the CWD renaming so the basenames match
    except for extensions"""
    logger.debug("Extracting paths/values from %s", data_items)
    values = []
    for name, item in data_items.items():
        if isinstance(item, FileGroup):
            cpy = item.copy_to('./' + name, symlink=True)
            values.append(cpy.fs_path)
        else:
            values.append(item.value)
    return tuple(values) if len(values) > 1 else values[0]


def encapsulate_paths_and_values(outputs, **kwargs):
    """Copies files into the CWD renaming so the basenames match
    except for extensions"""
    logger.debug("Encapsulating %s into %s", kwargs, outputs)
    items = []
    for outpt in outputs:
        val = kwargs[outpt.col_name]
        if issubclass(outpt.produced_format, FileGroup):
            obj = outpt.produced_format.from_fs_path(val)
        else:
            obj = outpt.produced_format(val)
        items.append(obj)
    return tuple(items) if len(items) > 1 else items[0]




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
#     include : ty.List[ty.List[str]] | None
#         Paths in the provenance to include in the match. If None all are
#         incluced
#     exclude : ty.List[ty.List[str]] | None
#         Paths in the provenance to exclude from the match. In None all are
#         excluded
#     """
#     if include is not None:
#         include_res = [self._gen_prov_path_regex(p) for p in include]
#     if exclude is not None:
#         exclude_res = [self._gen_prov_path_regex(p) for p in exclude]
#     diff = DeepDiff(self._prov, other._prov, ignore_order=True)
#     # Create regular expresssions for the include and exclude name_paths in
#     # the format that deepdiff uses for nested dictionary/lists

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
