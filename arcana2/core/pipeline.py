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
from arcana2.exceptions import ArcanaNameError, ArcanaUsageError
from .data.item import DataItem
from .data.set import Dataset
from .data.datatype import FileFormat
from .data.enum import DataSpace

logger = logging.getLogger('arcana')


@attr.s
class Pipeline():
    """A thin wrapper around a Pydra workflow to link it to sources and sinks
    within a dataset

    Parameters
    ----------
    workflow : Workflow
        The pydra workflow
    """

    workflow: Workflow = attr.ib()
    # dataset: Dataset = attr.ib()
    frequency: DataSpace = attr.ib()
    inputs: list[tuple[str, FileFormat]] = attr.ib(factory=list)
    outputs: list[tuple[str, FileFormat]] = attr.ib(factory=list)
    _connected: set[str] = attr.ib(factory=set, repr=False)

    @property
    def lzin(self):
        """
        Treat the 'lzout' of the source node as the 'lzin' of the pipeline to
        allow pipelines to be treated the same as normal Pydra workflow
        """
        return self.workflow.source.lzout

    def set_output(self, connections):
        """Connect the output using the same syntax as used for a Pydra workflow

        Parameters
        ----------
        connections : list[tuple(str, ?)] or tuple(str, ?) or dict[str, ?]
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
            setattr(self.workflow.sink, out_name, node_out)
            self._connected.add(out_name)

    def __getattr__(self, varname):
        """
        Delegate any missing attributes to nested workflow that operates per
        node"""
        return getattr(self.workflow.per_node, varname)

    def __call__(self, *args, **kwargs):
        self.check_connections()
        return self.workflow(*args, **kwargs)

    def check_connections(self):
        if missing:= set(self.output_names) - self._connected:
            raise Exception(
                f"The following outputs haven't been connected: {missing}")

    @property
    def input_names(self):
        return (n for n, _ in self.inputs)

    @property
    def output_names(self):
        return (n for n, _ in self.outputs)

    @classmethod
    def factory(cls, name, dataset, inputs, outputs, frequency=None,
                overwrite=False):
        """Generate a new pipeline connected with its inputs and outputs
        connected to sources/sinks in the dataset

        Parameters
        ----------
        name : str
            Name of the pipeline
        dataset : Dataset
            The dataset to connect the pipeline to
        inputs : Sequence[str or tuple[str, FileFormat]]
            List of column names (i.e. either data sources or sinks) to be
            connected to the inputs of the pipeline. If the pipelines requires
            the input to be in a format to the source, then it can be specified
            in a tuple (NAME, FORMAT)
        outputs : Sequence[str or tuple[str, FileFormat]]
            List of sink names to be connected to the outputs of the pipeline
            If teh the input to be in a specific format, then it can be provided in
            a tuple (NAME, FORMAT)
        frequency : DataSpace, optional
            The frequency of the pipeline, i.e. the frequency of the
            derivatvies within the dataset, e.g. per-session, per-subject, etc,
            by default None
        overwrite : bool, optional
            Whether to overwrite existing connections to sinks, by default False

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

        # Separate required formats and input names
        input_formats = dict(i for i in inputs if not isinstance(i, str))
        input_names = [i if isinstance(i, str) else i[0] for i in inputs]

        # Separate produced formats and output names
        output_formats = dict(o for o in outputs if not isinstance(o, str))
        output_names = [o if isinstance(o, str) else o[0] for o in outputs]

        # Create the outer workflow to link the analysis workflow with the
        # data node iteration and repository connection nodes
        wf = Workflow(name=name, input_spec=['ids'])

        wf.add(test_func(
            a=wf.lzin.ids,
            b=2,
            dataset=dataset,
            name='test_func'))

        wf.set_output(('c', wf.test_func.lzout.c))

        pipeline = Pipeline(wf,
                            # dataset,
                            frequency=frequency)

        # Add sinks for the output of the workflow
        sources = {}
        for input_name in input_names:
            try:
                source = dataset.column_specs[input_name]
            except KeyError:
                raise ArcanaNameError(
                    input_name,
                    f"{input_name} is not the name of a source in {dataset}")
            sources[input_name] = source
            try:
                required_format = input_formats[input_name]
            except KeyError:
                required_format = source.datatype
            pipeline.inputs.append((input_name, required_format))

        # Add sinks for the output of the workflow
        sinks = {}
        for output_name in output_names:
            try:
                sink = dataset.column_specs[output_name]
            except KeyError:
                raise ArcanaNameError(
                    output_name,
                    f"{output_name} is not the name of a sink in {dataset}")
            if sink.pipeline is not None:
                if overwrite:
                    logger.info(
                        f"Overwriting pipeline of sink '{output_name}' "
                        f"{sink.pipeline} with {pipeline}")
                else:
                    raise ArcanaUsageError(
                        f"Attempting to overwrite pipeline of '{output_name}' "
                        f"sink ({sink.pipeline}) . Use 'overwrite' option if "
                        "this is desired")
            sink.pipeline = pipeline
            sinks[output_name] = sink
            try:
                produced_format = output_formats[output_name]
            except KeyError:
                produced_format = sink.datatype
            pipeline.outputs.append((output_name, produced_format))

        # Generate list of nodes to process checking existing outputs
        wf.add(to_process(
            dataset=dataset,
            frequency=frequency,
            outputs=outputs,
            requested_ids=wf.lzin.ids,
            name='to_process'))

        # Create the workflow that will be split across all nodes for the 
        # given data frequency
        wf.add(Workflow(
            name='per_node',
            input_spec=['id'],
            id=wf.to_process.lzout.ids).split('id'))

        source_output_spec = {
            s: (DataItem
                if dataset.column_specs[s].frequency.is_parent(frequency,
                                                               if_match=True)
                else ty.Sequence[DataItem])
            for s in input_names}

        @mark.task
        @mark.annotate(
            {'dataset': Dataset,
             'frequency': DataSpace,
             'id': str,
             'inputs': ty.Sequence[str],
             'return': source_output_spec})
        def source(dataset, frequency, id, inputs):
            """Selects the items from the dataset corresponding to the input 
            sources and retrieves them from the repository to a cache on 
            the host"""
            outputs = []
            data_node = dataset.node(frequency, id)
            with dataset.repository:
                for inpt_name in inputs:
                    item = data_node[inpt_name]
                    item.get()  # download to host if required
                    outputs.append(item)
            return tuple(outputs)

        wf.per_node.add(source(
            name='source', dataset=dataset, frequency=frequency,
            inputs=input_names, id=wf.per_node.lzin.id))

        # Set the inputs
        for input_name in input_names:
            setattr(pipeline.lzin, input_name,
                    getattr(wf.per_node.source.lzout, input_name))

        # Do input format conversions if required
        for input_name, required_format in pipeline.inputs:
            stored_format = dataset.column_specs[input_name].datatype
            if required_format != stored_format:
                cname = f"{input_name}_input_converter"
                converter_task = required_format.converter(stored_format)(
                    name=cname,
                    to_convert=getattr(pipeline.lzin, input_name))
                if source_output_spec[input_name] == ty.Sequence[DataItem]:
                    # Iterate over all items in the sequence and convert them
                    converter_task.split('to_convert')
                # Insert converter
                wf.per_node.add(converter_task)
                # Map converter output to workflow output
                setattr(pipeline.lzin, input_name,
                        getattr(wf.per_node, cname).lzout.converted)

        # Create identity node to accept connections from user-
        wf.per_node.add(
            FunctionTask(
                identity,
                input_spec=SpecInfo(
                    name=f'{name}Inputs', bases=(BaseSpec,),
                    fields=[(o, ty.Any) for o in output_names]),
                output_spec=SpecInfo(
                    name=f'{name}Outputs', bases=(BaseSpec,),
                    fields=[(o, ty.Any) for o in output_names]),
                name='sink'))

        # Set format converters where required
        to_sink = {o: getattr(wf.per_node.sink.lzout, o)
                   for o in output_names}

        # Do output format conversions if required
        for output_name, produced_format in pipeline.outputs:
            stored_format = dataset.column_specs[output_name].datatype
            if produced_format != stored_format:
                cname = f"{output_name}_output_converter"
                # Insert converter
                wf.per_node.add(stored_format.converter(produced_format)(
                    name=cname, to_convert=to_sink[output_name]))
                # Map converter output to workflow output
                to_sink[output_name] = getattr(wf.per_node,
                                               cname).lzout.converted

        def store(dataset, frequency, id, **to_sink):
            data_node = dataset.node(frequency, id)
            with dataset.repository:
                for outpt_name, outpt_value in to_sink.items():
                    node_item = data_node[outpt_name]
                    node_item.value = outpt_value
                    node_item.put() # Store value/path in repository
            return id

        # Can't use a decorated function as we need to allow for dynamic
        # arguments
        wf.per_node.add(
            FunctionTask(
                store,
                input_spec=SpecInfo(
                    name='UploadInputs', bases=(BaseSpec,), fields=(
                        [('dataset', Dataset),
                         ('frequency', DataSpace),
                         ('id', str)]
                        + [(s, DataItem) for s in to_sink])),
                output_spec=SpecInfo(
                    name='UploadOutputs', bases=(BaseSpec,), fields=[
                        ('id', str)]),
                name='upload',
                dataset=dataset,
                frequency=frequency,
                id=wf.per_node.lzin.id,
                **to_sink))

        wf.per_node.set_output(
            [('id', wf.per_node.upload.lzout.id)])

        wf.set_output(
            [
             #('processed', wf.per_node.lzout.id),
             ('couldnt_process', wf.to_process.lzout.cant_process)
             ])

        return pipeline


def identity(**kwargs):
    "Returns the keyword arguments as a tuple"
    return tuple(kwargs.values())


@mark.task
@mark.annotate({
   'a': int,
   'b': int,
   'dataset': Dataset,
   'return':{
       'c': int}})
def test_func(a, b, dataset):
   return a + b


@mark.task
@mark.annotate({
    'dataset': Dataset,
    'frequency': DataSpace,
    'outputs': ty.Sequence[str],
    'requested_ids': ty.Sequence[str] or None,
    'return': {
        'ids': list[str],
        'cant_process': list[str]}})
def to_process(dataset, frequency, outputs, requested_ids):
    if requested_ids is None:
        requested_ids = dataset.node_ids(frequency)
    ids = []
    cant_process = []
    for data_node in dataset.nodes(frequency, ids=requested_ids):
        # TODO: Should check provenance of existing nodes to see if it matches
        not_exist = [not data_node[o].exists for o in outputs]
        if all(not_exist):
            ids.append(data_node.id)
        elif any(not_exist):
            cant_process.append(data_node.id)
    return ids, cant_process
