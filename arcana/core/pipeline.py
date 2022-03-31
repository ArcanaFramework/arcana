from __future__ import annotations
import attr
import typing as ty
from dataclasses import dataclass
import logging
from copy import copy, deepcopy
from operator import itemgetter
import attr
from pydra import Workflow, mark
from pydra.engine.task import FunctionTask
from pydra.engine.specs import BaseSpec, SpecInfo
from arcana.exceptions import ArcanaNameError, ArcanaUsageError
from .data.format import DataItem, FileGroup
from .data.set import Dataset
from .data.space import DataSpace
from .utils import func_task

logger = logging.getLogger('arcana')


@dataclass
def Input():

    col_name: str
    pydra_field: str
    required_format: type


@dataclass
def Output():

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
    template : Workflow
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
    dataset: Dataset = attr.ib()
    frequency: DataSpace = attr.ib()
    template: Workflow = attr.ib()
    inputs: ty.List[Input] = attr.ib(
        converter=lambda lst: [Input(i) for i in lst])
    outputs: ty.List[Output] = attr.ib(
        converter=lambda lst: [Output(o) for o in lst])

    @inputs.validator
    def inputs_validator(self, _, inpt):
        column = self.dataset.column[inpt.col_name]
        inpt.required_format.find_converter(column.format)
        if inpt.pydra_field not in self.template.input_names:
            raise ArcanaNameError(
                f"{inpt.pydra_field} is not in the input spec of '{self.name}' "
                f"pipeline: " + "', '".join(self.template.input_names))

    @outputs.validator
    def outputs_validator(self, _, outpt):
        column = self.dataset.column[outpt.col_name]
        if column.frequency != self.frequency:
            raise ArcanaUsageError(
                f"Pipeline frequency ('{str(self.frequency)}') doesn't match "
                f"that of '{outpt.col_name}' output ('{str(self.frequency)}')")
        column.format.find_converter(outpt.produced_format)
        if outpt.pydra_field not in self.template.output_names:
            raise ArcanaNameError(
                f"{outpt.pydra_field} is not in the output spec of '{self.name}' "
                f"pipeline: " + "', '".join(self.template.output_names))            

    @property
    def input_col_names(self):
        return (i.col_name for i in self.inputs)

    @property
    def output_names(self):
        return (o.col_name for o in self.outputs)

    # parameterisation = self.get_parameterisation(kwargs)
    # self.wf.to_process.inputs.parameterisation = parameterisation
    # self.wf.per_node.source.inputs.parameterisation = parameterisation

    def internal_workflow(self, **kwargs):
        cpy = deepcopy(self.template)
        cpy.name = 'internal'
        for name, lf in kwargs.items():
            setattr(cpy.inputs, name, lf)
        return cpy

    def __call__(self, **kwargs):
        """        
        Parameters
        ----------
        name : str
            Name of the pipeline
        dataset : Dataset
            The dataset to connect the pipeline to
        **kwargs
            Passed to Pydra.Workflow init

        Returns
        -------
        Workflow
            The newly created pipeline ready for analysis nodes to be added to
            it

        Raises
        ------
        ArcanaUsageError
            If the new pipeline will overwrite an existing pipeline connection
            with overwrite == False.
        """
        # # Set derivatives as existing
        # for node in self.dataset.nodes(self.frequency):
        #     for output in self.output_names:
        #         node[output].get(assume_exists=True)

        # # Separate required formats and input names
        # input_types = dict(i for i in self.inputs if not isinstance(i, str))
        # input_names = [i if isinstance(i, str) else i[0] for i in self.inputs]

        # # Separate produced formats and output names
        # output_types = dict(o for o in self.outputs if not isinstance(o, str))
        # output_names = [o if isinstance(o, str) else o[0] for o in self.outputs]

        # Create the outer workflow to link the analysis workflow with the
        # data node iteration and store connection nodes
        wf = Workflow(name=self.name, input_spec=['ids'], **kwargs)

        # # Add sinks for the output of the workflow
        # sources = {}
        # for input_name in input_names:
        #     try:
        #         source = dataset.column_specs[input_name]
        #     except KeyError as e:
        #         raise ArcanaNameError(
        #             input_name,
        #             f"{input_name} is not the name of a source in {dataset}") from e
        #     sources[input_name] = source
        #     try:
        #         required_format = input_types[input_name]
        #     except KeyError:
        #         input_types[input_name] = required_format = source.format
        #     self.inputs.append((input_name, required_format))

        # # Add sinks for the output of the workflow
        # sinks = {}
        # for output_name in output_names:
        #     try:
        #         sink = dataset.column_specs[output_name]
        #     except KeyError as e:
        #         raise ArcanaNameError(
        #             output_name,
        #             f"{output_name} is not the name of a sink in {dataset}") from e

        #     sink.pipeline_name = name
        #     sinks[output_name] = sink
        #     try:
        #         produced_format = output_types[output_name]
        #     except KeyError:
        #         output_types[output_name] = produced_format = sink.format
        #     pipeline.outputs.append((output_name, produced_format))

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
            ('dataset', Dataset),
            ('frequency', DataSpace),
            ('id', str),
            ('inputs', ty.Sequence[str]),
            ('parameterisation', ty.Dict[str, ty.Any])]

        source_out_dct = {
            s: (DataItem
                if self.dataset.column_specs[s].frequency.is_parent(
                    self.frequency, if_match=True)
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
            stored_format = self.dataset.column_specs[inpt.col_name].format
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

        wf.per_node.add(
            self.internal_workflow(
                **{i.pydra_field: getattr(wf.per_node.input_interface, i.col_name)
                   for i in self.inputs}))

        # Creates a node to accept values from user-defined nodes and
        # encapsulate them into DataItems
        wf.per_node.add(func_task(
            encapsulate_paths_and_values,
            in_fields=[('outputs', ty.Dict[str, type])] + [
                (o, ty.Any) for o in self.output_col_names],
            out_fields=[(o, DataItem) for o in self.output_col_names],
            name='output_interface',
            outputs=self.outputs,
            **{o.col_name: getattr(wf.per_node.internal, o.pydra_field)
               for o in self.outputs}))

        # Set format converters where required
        to_sink = {o: getattr(wf.per_node.output_interface.lzout, o)
                   for o in self.output_col_names}

        # Do output format conversions if required
        for outpt in self.outputs:
            stored_format = self.dataset.column_specs[outpt.col_name].format
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
                [('dataset', Dataset),
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


def append_side_car_suffix(name, suffix):
    """Creates a new combined field name out of a basename and a side car"""
    return f'{name}__o__{suffix}'


def split_side_car_suffix(name):
    """Splits the basename from a side car sufix (as combined by `append_side_car_suffix`"""
    return name.split('__o__')


@mark.task
@mark.annotate({
    'dataset': Dataset,
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
        not_exist = [not data_node[o[0]].exists for o in outputs]
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
