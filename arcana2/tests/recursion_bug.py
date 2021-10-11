import typing as ty
import logging
import cloudpickle as cp
from pydra import mark, Workflow
from pydra.engine.task import FunctionTask
from pydra.engine.specs import BaseSpec, SpecInfo
from pydra.tasks.dcm2niix import Dcm2Niix
from arcana2.core.data.enum import DataDimension
from arcana2.core.data.set import Dataset
from arcana2.core.data.item import DataItem
from arcana2.core.pipeline import Pipeline
from arcana2.repositories import FileSystem
from arcana2.dimensions.clinical import Clinical
from arcana2.datatypes import dicom, niftix_gz
from arcana2.exceptions import ArcanaNameError, ArcanaUsageError


def identity(**kwargs):
    "Returns the keyword arguments as a tuple"
    return tuple(kwargs.values())


@mark.task
@mark.annotate({
    'dataset': Dataset,
    'frequency': DataDimension,
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

dataset = FileSystem().dataset(
    name='/Users/tclose/git/workflows/arcana2/arcana2/tests/data/test-dataset',
    hierarchy=[Clinical.session])

dataset.add_source(
    name='in_dir',
    path='sample-dicom',
    format=dicom)

dataset.add_sink(
    name='out_file',
    path='output-nifti',
    format=niftix_gz)

frequency = Clinical.session
name='pydra_tasks_dcm2niix_Dcm2Niix'
inputs = ['in_dir']
outputs = ['out_file']
overwrite = False

logger = logging.getLogger('arcana')

# Separate required formats and input names
input_formats = dict(i for i in inputs if not isinstance(i, str))
input_names = [i if isinstance(i, str) else i[0] for i in inputs]

# Separate produced formats and output names
output_formats = dict(o for o in outputs if not isinstance(o, str))
output_names = [o if isinstance(o, str) else o[0] for o in outputs]

# Create the outer workflow to link the analysis workflow with the
# data node iteration and repository connection nodes
wf = Workflow(name=name, input_spec=['ids'])

# pipeline = Pipeline(wf, dataset, frequency=frequency)

# # Add sinks for the output of the workflow
# sources = {}
# for input_name in input_names:
#     try:
#         source = dataset.column_specs[input_name]
#     except KeyError:
#         raise ArcanaNameError(
#             input_name,
#             f"{input_name} is not the name of a source in {dataset}")
#     sources[input_name] = source
#     try:
#         required_format = input_formats[input_name]
#     except KeyError:
#         required_format = source.datatype
#     pipeline.inputs.append((input_name, required_format))

# # Add sinks for the output of the workflow
# sinks = {}
# for output_name in output_names:
#     try:
#         sink = dataset.column_specs[output_name]
#     except KeyError:
#         raise ArcanaNameError(
#             output_name,
#             f"{output_name} is not the name of a sink in {dataset}")
#     if sink.pipeline is not None:
#         if overwrite:
#             logger.info(
#                 f"Overwriting pipeline of sink '{output_name}' "
#                 f"{sink.pipeline} with {pipeline}")
#         else:
#             raise ArcanaUsageError(
#                 f"Attempting to overwrite pipeline of '{output_name}' "
#                 f"sink ({sink.pipeline}) . Use 'overwrite' option if "
#                 "this is desired")
#     sink.pipeline = pipeline
#     sinks[output_name] = sink
#     try:
#         produced_format = output_formats[output_name]
#     except KeyError:
#         produced_format = sink.datatype
#     pipeline.outputs.append((output_name, produced_format))


@mark.task
@mark.annotate(
    {'x': int,
     'y': int,
     'return': {'z': int}})
def f(x, y):
    return x + y


wf.add(f(name='test', x=wf.lzin.ids, y=2))

# # Generate list of nodes to process checking existing outputs
# wf.add(to_process(
#     dataset=dataset,
#     frequency=frequency,
#     outputs=outputs,
#     requested_ids=wf.lzin.ids,
#     name='to_process'))

# # Create the workflow that will be split across all nodes for the 
# # given data frequency
# wf.add(Workflow(
#     name='per_node',
#     input_spec=['id'],
#     id=wf.to_process.lzout.ids).split('id'))

# source_output_spec = {
#     s: (DataItem
#         if dataset.column_specs[s].frequency.is_parent(frequency,
#                                                         if_match=True)
#         else ty.Sequence[DataItem])
#     for s in input_names}

# @mark.task
# @mark.annotate(
#     {'dataset': Dataset,
#         'frequency': DataDimension,
#         'id': str,
#         'inputs': ty.Sequence[str],
#         'return': source_output_spec})
# def source(dataset, frequency, id, inputs):
#     """Selects the items from the dataset corresponding to the input 
#     sources and retrieves them from the repository to a cache on 
#     the host"""
#     outputs = []
#     data_node = dataset.node(frequency, id)
#     with dataset.repository:
#         for inpt_name in inputs:
#             item = data_node[inpt_name]
#             item.get()  # download to host if required
#             outputs.append(item)
#     return tuple(outputs)

# wf.per_node.add(source(
#     name='source', dataset=dataset, frequency=frequency,
#     inputs=input_names, id=wf.per_node.lzin.id))

# # Set the inputs
# for input_name in input_names:
#     setattr(pipeline.lzin, input_name,
#             getattr(wf.per_node.source.lzout, input_name))

# # Do input format conversions if required
# for input_name, required_format in pipeline.inputs:
#     stored_format = dataset.column_specs[input_name].datatype
#     if required_format != stored_format:
#         cname = f"{input_name}_input_converter"
#         converter_task = required_format.converter(stored_format)(
#             name=cname,
#             to_convert=getattr(pipeline.lzin, input_name))
#         if source_output_spec[input_name] == ty.Sequence[DataItem]:
#             # Iterate over all items in the sequence and convert them
#             converter_task.split('to_convert')
#         # Insert converter
#         wf.per_node.add(converter_task)
#         # Map converter output to workflow output
#         setattr(pipeline.lzin, input_name,
#                 getattr(wf.per_node, cname).lzout.converted)

# # Create identity node to accept connections from user-
# wf.per_node.add(
#     FunctionTask(
#         identity,
#         input_spec=SpecInfo(
#             name=f'{name}Inputs', bases=(BaseSpec,),
#             fields=[(o, ty.Any) for o in output_names]),
#         output_spec=SpecInfo(
#             name=f'{name}Outputs', bases=(BaseSpec,),
#             fields=[(o, ty.Any) for o in output_names]),
#         name='outputs'))

# # Set format converters where required
# to_sink = {o: getattr(wf.per_node.outputs.lzout, o)
#             for o in output_names}

# # Do output format conversions if required
# for output_name, produced_format in pipeline.outputs:
#     stored_format = dataset.column_specs[output_name].datatype
#     if produced_format != stored_format:
#         cname = f"{output_name}_output_converter"
#         # Insert converter
#         wf.per_node.add(stored_format.converter(produced_format)(
#             name=cname, to_convert=to_sink[output_name]))
#         # Map converter output to workflow output
#         to_sink[output_name] = getattr(wf.per_node,
#                                         cname).lzout.converted        

# def store(dataset, frequency, id, **to_sink):
#     data_node = dataset.node(frequency, id)
#     with dataset.repository:
#         for outpt_name, outpt_value in to_sink.items():
#             node_item = data_node[outpt_name]
#             node_item.value = outpt_value
#             node_item.put() # Store value/path in repository
#     return id

# # Can't use a decorated function as we need to allow for dynamic
# # arguments
# wf.per_node.add(
#     FunctionTask(
#         store,
#         input_spec=SpecInfo(
#             name='SinkInputs', bases=(BaseSpec,), fields=(
#                 [('dataset', Dataset),
#                     ('frequency', DataDimension),
#                     ('id', str)]
#                 + [(s, DataItem) for s in to_sink])),
#         output_spec=SpecInfo(
#             name='SinkOutputs', bases=(BaseSpec,), fields=[
#                 ('id', str)]),
#         name='store',
#         dataset=dataset,
#         frequency=frequency,
#         id=wf.per_node.lzin.id,
#         **to_sink))

# wf.per_node.set_output(
#     [('id', wf.per_node.store.lzout.id)])

wf.set_output([('z', wf.test.lzout.z)])

# Add the app task
# pipeline.add(Dcm2Niix(name='app'))

# Connect inputs
# for input in pipeline.input_names:
#     setattr(pipeline.app, input, getattr(pipeline.lzin, input))

# # Connect outputs
# for output in pipeline.output_names:
#     pipeline.set_output((output, getattr(pipeline.app.lzout, output)))

wf()

# cp.dumps(frequency)