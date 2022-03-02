from __future__ import annotations
import attr
import typing as ty
from types import SimpleNamespace
import logging
from copy import copy
import attr
from pydra import Workflow, mark
from pydra.engine.task import FunctionTask
from pydra.engine.specs import BaseSpec, SpecInfo
from arcana.exceptions import ArcanaNameError, ArcanaUsageError
from .data.item import DataItem, FileGroup
from .data.set import Dataset
from .data.format import FileFormat
from .data.space import DataSpace
from .utils import func_task

logger = logging.getLogger('arcana')


@attr.s
class Pipeline():
    """A thin wrapper around a Pydra workflow to link it to sources and sinks
    within a dataset

    Parameters
    ----------
    wf : Workflow
        The pydra workflow
    """

    wf: Workflow = attr.ib()
    frequency: DataSpace = attr.ib()
    inputs: ty.List[ty.Tuple[str, FileFormat]] = attr.ib(factory=list)
    outputs: ty.List[ty.Tuple[str, FileFormat]] = attr.ib(factory=list)
    _connected: ty.Set[str] = attr.ib(factory=set, repr=False)

    @property
    def lzin(self):
        """
        Treat the 'lzout' of the source node as the 'lzin' of the pipeline to
        allow pipelines to be treated the same as normal Pydra workflow
        """
        return self.wf.per_node.input_interface.lzout

    def set_output(self, connections):
        """Connect the output using the same syntax as used for a Pydra workflow

        Parameters
        ----------
        connections : ty.List[ty.Tuple[str, ty.Any]] or ty.Tuple[str, ty.Any] or ty.Dict[str, ty.Any]
            The connections to set

        Raises
        ------
        Exception
            An exception is raised if the connections are provided in the wrong
            format
        """
        if isinstance(connections, tuple) and len(connections) == 2:
            connections = [connections]
        elif isinstance(connections, dict):
            connections = list(connections.items())
        elif not (isinstance(connections, list)
                  and all([len(el) == 2 for el in connections])):
            raise Exception(
                "Connections can be a 2-elements tuple, a list of these "
                "tuples, or dictionary")
        # Connect "outputs" the pipeline to the 
        for out_name, node_out in connections:
            setattr(self.wf.per_node.output_interface.inputs, out_name,
                    node_out)
            self._connected.add(out_name)

    def add(self, task):
        if task.name in self.wf.name2obj:
            raise ValueError(
                "Another task named {} is already added to the pipeline"
                .format(task.name))        
        if task.name in dir(self):
            raise ValueError(
                "Cannot use names of pipeline attributes or methods "
                f"({task.name}) as task name")        
        self.wf.per_node.add(task)
        # Favour setting a proper attribute instead of using __getattr__ to
        # redirect to name2obj
        setattr(self, task.name, task)

    @property
    def nodes(self):
        return self.wf.nodes

    @property
    def dataset(self):
        return self.wf.per_node.sink.inputs.dataset

    def __call__(self, *args, **kwargs):
        self.check_connections()
        parameterisation = self.get_parameterisation(kwargs)
        self.wf.to_process.inputs.parameterisation = parameterisation
        self.wf.per_node.source.inputs.parameterisation = parameterisation
        result = self.wf(*args, **kwargs)
        # Set derivatives as existing
        for node in self.dataset.nodes(self.frequency):
            for output in self.output_names:
                node[output].get(assume_exists=True)
        return result

    def check_connections(self):
        missing = set(self.output_names) - self._connected
        if missing:
            raise Exception(
                f"The following outputs haven't been connected: {missing}")

    @property
    def input_names(self):
        return (n for n, _ in self.inputs)

    @property
    def output_names(self):
        return (n for n, _ in self.outputs)

    def get_parameterisation(self, additional_args):
        """
        Generates provenance information for the pipeline

        Returns
        -------
        prov : dict[str, *]s
            A dictionary containing the provenance information to record
            for the pipeline
        """
        prov = {
            '__prov_version__': self.PROVENANCE_VERSION}
        return prov

    @classmethod
    def factory(cls, name, dataset, inputs, outputs, frequency=None,
                overwrite=False, **kwargs):
        """Generate a new pipeline connected with its inputs and outputs
        connected to sources/sinks in the dataset

        Parameters
        ----------
        name : str
            Name of the pipeline
        dataset : Dataset
            The dataset to connect the pipeline to
        inputs : Sequence[ty.Union[str, ty.Tuple[str, FileFormat]]]
            List of column names (i.e. either data sources or sinks) to be
            connected to the inputs of the pipeline. If the pipelines requires
            the input to be in a format to the source, then it can be specified
            in a tuple (NAME, FORMAT)
        outputs : Sequence[ty.Union[str, ty.Tuple[str, FileFormat]]]
            List of sink names to be connected to the outputs of the pipeline
            If the input to be in a specific format, then it can be provided in
            a tuple (NAME, FORMAT)
        frequency : DataSpace, optional
            The frequency of the pipeline, i.e. the frequency of the
            derivatvies within the dataset, e.g. per-session, per-subject, etc,
            by default None
        overwrite : bool, optional
            Whether to overwrite existing connections to sinks, by default False
        **kwargs
            Passed to Pydra.Workflow init

        Returns
        -------
        Pipeline
            The newly created pipeline ready for analysis nodes to be added to
            it

        Raises
        ------
        ArcanaUsageError
            If the new pipeline will overwrite an existing pipeline connection
            with overwrite == False.
        """
        if frequency is None:
            frequency = max(dataset.space)
        else:
            frequency = dataset._parse_freq(frequency)

        inputs = list(inputs)
        outputs = list(outputs)

        if not inputs:
            raise ArcanaUsageError(f"No inputs provided to {name} pipeline")

        if not outputs:
            raise ArcanaUsageError(f"No outputs provided to {name} pipeline")

        # Separate required formats and input names
        input_types = dict(i for i in inputs if not isinstance(i, str))
        input_names = [i if isinstance(i, str) else i[0] for i in inputs]

        # Separate produced formats and output names
        output_types = dict(o for o in outputs if not isinstance(o, str))
        output_names = [o if isinstance(o, str) else o[0] for o in outputs]

        # Create the outer workflow to link the analysis workflow with the
        # data node iteration and store connection nodes
        wf = Workflow(name=name, input_spec=['ids'], **kwargs)

        pipeline = Pipeline(wf, frequency=frequency)

        # Add sinks for the output of the workflow
        sources = {}
        for input_name in input_names:
            try:
                source = dataset.column_specs[input_name]
            except KeyError as e:
                raise ArcanaNameError(
                    input_name,
                    f"{input_name} is not the name of a source in {dataset}") from e
            sources[input_name] = source
            try:
                required_format = input_types[input_name]
            except KeyError:
                input_types[input_name] = required_format = source.datatype
            pipeline.inputs.append((input_name, required_format))

        # Add sinks for the output of the workflow
        sinks = {}
        for output_name in output_names:
            try:
                sink = dataset.column_specs[output_name]
            except KeyError as e:
                raise ArcanaNameError(
                    output_name,
                    f"{output_name} is not the name of a sink in {dataset}") from e
            if sink.pipeline is not None:
                if overwrite:
                    logger.info(
                        f"Overwriting pipeline of sink '{output_name}' "
                        f"{sink.pipeline} with {pipeline}")
                else:
                    raise ArcanaUsageError(
                        f"Attempting to overwrite pipeline of '{output_name}' "
                        f"sink ({sink.pipeline}). Use 'overwrite' option if "
                        "this is desired")
            sink.pipeline = pipeline
            sinks[output_name] = sink
            try:
                produced_format = output_types[output_name]
            except KeyError:
                output_types[output_name] = produced_format = sink.datatype
            pipeline.outputs.append((output_name, produced_format))

        # Generate list of nodes to process checking existing outputs
        wf.add(to_process(
            dataset=dataset,
            frequency=frequency,
            outputs=pipeline.outputs,
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
                if dataset.column_specs[s].frequency.is_parent(
                    frequency, if_match=True)
                else ty.Sequence[DataItem])
            for s in input_names}
        source_out_dct['provenance_'] = ty.Dict[str, ty.Any]

        wf.per_node.add(func_task(
            source_items,
            in_fields=source_in,
            out_fields=list(source_out_dct.items()),
            name='source',
            dataset=dataset,
            frequency=frequency,
            inputs=input_names,
            id=wf.per_node.lzin.id))

        # Set the inputs
        sourced = {i: getattr(wf.per_node.source.lzout, i) for i in input_names}

        # Do input format conversions if required
        for input_name, required_format in pipeline.inputs:
            stored_format = dataset.column_specs[input_name].datatype
            if required_format != stored_format:
                logger.info("Adding implicit conversion for input '%s' "
                            "from %s to %s", input_name, stored_format,
                            required_format)
                cname = f"{input_name}_input_converter"                
                converter_task = required_format.converter(stored_format)(
                    name=cname,
                    to_convert=sourced[input_name])
                if source_out_dct[input_name] == ty.Sequence[DataItem]:
                    # Iterate over all items in the sequence and convert them
                    converter_task.split('to_convert')
                # Insert converter
                wf.per_node.add(converter_task)
                # Map converter output to input_interface
                sourced[input_name] = getattr(wf.per_node, cname).lzout.converted

        # Create identity node to accept connections from user-defined nodes
        # via `set_output` method
        wf.per_node.add(func_task(
            extract_paths_and_values,
            in_fields=[(i, DataItem) for i in input_names],
            out_fields=[(i, ty.Any) for i in input_names],
            name='input_interface',
            **sourced))        

        # Creates a node to accept values from user-defined nodes and
        # encapsulate them into DataItems
        wf.per_node.add(func_task(
            encapsulate_paths_and_values,
            in_fields=[('outputs', ty.Dict[str, type])] + [
                (o, ty.Any) for o in output_names],
            out_fields=[(o, DataItem) for o in output_names],
            name='output_interface',
            outputs=output_types))

        # Set format converters where required
        to_sink = {o: getattr(wf.per_node.output_interface.lzout, o)
                   for o in output_names}

        # Do output format conversions if required
        for output_name, produced_format in pipeline.outputs:
            stored_format = dataset.column_specs[output_name].datatype
            if produced_format != stored_format:
                logger.info("Adding implicit conversion for output '%s' "
                    "from %s to %s", output_name, produced_format,
                    stored_format)
                cname = f"{output_name}_output_converter"
                # Insert converter
                wf.per_node.add(stored_format.converter(produced_format)(
                    name=cname, to_convert=to_sink[output_name]))
                # Map converter output to workflow output
                to_sink[output_name] = getattr(wf.per_node,
                                               cname).lzout.converted

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
            dataset=dataset,
            frequency=frequency,
            id=wf.per_node.lzin.id,
            provenance=wf.per_node.source.lzout.provenance_,
            **to_sink))

        wf.per_node.set_output(
            [('id', wf.per_node.sink.lzout.id)])

        wf.set_output(
            [('processed', wf.per_node.lzout.id),
             ('couldnt_process', wf.to_process.lzout.cant_process)])

        return pipeline


    PROVENANCE_VERSION = '1.0'


# def identity(**kwargs):
#     "Returns the keyword arguments as a tuple"
#     to_return = tuple(kwargs.values())
#     if len(to_return) == 1:
#         to_return = to_return[0]
#     return to_return


# def extract_paths(**data_items):
#     paths = tuple(i.value for i in kwargs.values())
#     return paths if len(paths) > 1 else paths[0]


# def encapsulate_paths(outputs, **kwargs):
#     items = [v.datatype(kwargs[k]) for k, v in outputs]
#     return items if len(items) > 1 else items[0]


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


def extract_paths_and_values(**data_items):
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
    for out_name, out_type in outputs.items():
        if isinstance(out_type, FileFormat):
            items.append(out_type.from_path(kwargs[out_name]))
        else:
            items.append(out_type(kwargs[out_name]))
    return tuple(items) if len(items) > 1 else items[0]
