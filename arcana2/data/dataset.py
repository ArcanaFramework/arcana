from operator import itemgetter
import logging
import typing as ty
import re
from copy import copy
from collections import defaultdict
from itertools import chain
from collections import OrderedDict
from pydra import Workflow, mark
from pydra.engine.task import FunctionTask
from pydra.engine.specs import BaseSpec, SpecInfo
from arcana2.exceptions import (
    ArcanaError, ArcanaSelectionError, ArcanaNameError,
    ArcanaDataTreeConstructionError, ArcanaUsageError,
    ArcanaBadlyFormattedIDError)
from .item import UnresolvedFileGroup, UnresolvedField, DataItem
from .frequency import DataFrequency
from .file_format import FileFormat
from .selector import DataSelector
from .spec import FileGroupSpec, FieldSpec


logger = logging.getLogger('arcana')

class Dataset():
    """
    A representation of a "dataset", the complete collection of data
    (file-sets and fields) to be used in an analysis.

    Parameters
    ----------
    name : str
        The name/path that uniquely identifies the datset within the
        repository it is stored (e.g. FS directory path or project name)
    repository : Repository
        The repository the dataset is stored into. Can be the local file
        system by providing a FileSystemDir repo.
    selectors : Dict[str, Selector]
        A dictionary that maps "name-paths" of input "columns" in the dataset
        to criteria in a Selector object that select the corresponding
        items in the dataset
    derivatives : Dict[str, Spec]
        A dictionary that maps "name-paths" of derivatives analysis workflows
        to be stored in the dataset
    included : Dict[DataFrequency, List[str]]
        The IDs to be included in the dataset for each frequency. E.g. can be
        used to limit the subject IDs in a project to the sub-set that passed
        QC. If a frequency is omitted or its value is None, then all available
        will be used
    frequency_enum : Enum
        The DataFrequency enum that defines the frequencies (e.g. per-session,
        per-subject,...) present in the dataset.
    id_inference : Sequence[(DataFrequency, str)] or Callable
        Specifies how IDs of primary data frequencies that not explicitly
        provided are inferred from the IDs that are. For example, given a set
        of subject IDs that combination of the ID of the group that they belong
        to and their member IDs (i.e. matched test/controls have same member ID)

            CONTROL01, CONTROL02, CONTROL03, ... and TEST01, TEST02, TEST03

        the group ID can be extracted by providing the a list of tuples
        containing ID to source the inferred IDs from coupled with a regular
        expression with named groups

            id_inference=[(ClinicalTrial.subject,
                           r'(?P<group>[A-Z]+)(?P<member>[0-9]+)')}

        Alternatively, a general function with signature `f(ids)` that returns
        a dictionary with the mapped IDs can be provided instead.
    **populate_kwargs : Dict[str, Any]
        Keyword arguments passed on to the `populate_tree` method of the
        repository class when it is called
    """

    def __init__(self, name, repository, selectors, frequency_enum,
                 derivatives=None, included=None, excluded=None,
                 id_inference=None, **populate_kwargs):
        self.name = name
        self.frequency_enum = frequency_enum
        self.repository = repository
        if wrong_freq := [m for m in selectors.values()
                          if not isinstance(m.frequency, self.frequency_enum)]:
            raise ArcanaUsageError(
                f"Data frequencies of {wrong_freq} selectors does not match "
                f"that of repository {self.frequency_enum}")
        self.selectors = selectors
        self.derivatives = derivatives if derivatives else {}
        if overlapping := (set(self.selectors) & set(self.derivatives)):
            raise ArcanaUsageError(
                "Name-path clashes between selectors and derivatives ("
                "', '".join(overlapping) + "')")

        def parse_id_filters(arg):
            dct = {f: None for f in self.frequency_enum}
            if arg:
                for freq, ids in arg:
                    try:
                        dct[self.frequency_enum[freq]] = list(ids)
                    except KeyError:
                        raise ArcanaUsageError(
                            f"Unrecognised data frequency '{freq}' (valid "
                            f"{', '.join(self.frequency_enum)})")
            return dct

        self.included = parse_id_filters(included)
        self.excluded = parse_id_filters(excluded)
        if both:= [f for f in self.included
                   if (self.included[f] is not None
                       and self.excluded[f] is not None)]:
            raise ArcanaUsageError(
                    "Cannot provide both 'included' and 'excluded' arguments "
                    "for frequencies ('{}') to Dataset __init__".format(
                        "', '".join(both)))
        self._root_node = None  # Lazy loading of data tree info from repo
        self._populate_kwargs = populate_kwargs
        self.id_inference = id_inference

    def __repr__(self):
        return (f"Dataset(name='{self.name}', repository={self.repository}, "
                f"included={self.included})")

    def __enter__(self):
        self.repository.__enter__()
        return self

    def __exit__(self):
        self.repository.__exit__()

    def __eq__(self, other):
        return (self.name == other.name
                and self.repository == other.repository
                and self.included == other.included
                and self.root_node == other.root_node
                and self.frequency_enum == other.frequency_enum)

    def __hash__(self):
        return (hash(self.name)
                ^ hash(self.repository)
                ^ hash(self.included)
                ^ hash(self.root_node)
                ^ hash(self.frequency_enum))

    def __getitem__(self, key):
        if key == self.frequency_enum(0):
            return self.root_node
        else:
            return self.root_node.subnodes[key]

    @property
    def prov(self):
        return {
            'name': self.name,
            'repository': self.repository.prov,
            'ids': {str(freq): tuple(ids) for freq, ids in self.nodes.items()}}

    @property
    def root_node(self):
        """Lazily loads the data tree from the repository on demand

        Returns
        -------
        DataNode
            The root node of the data tree
        """
        if self._root_node is None:
            self._root_node = DataNode(self.root_frequency, {}, self)
            self.repository.populate_tree(self, **self._populate_kwargs)
        return self._root_node

    @property
    def file_selectors(self):
        return (s for s in self.selectors if s.is_file_group)

    @property
    def field_selectors(self):
        return (s for s in self.selectors if s.is_field)

    def __ne__(self, other):
        return not (self == other)

    def column_spec(self, name_path):
        try:
            return self.selectors[name_path]
        except KeyError:
            try:
                return self.derivatives[name_path]
            except KeyError:
                raise ArcanaNameError(
                    f"No column with the name path '{name_path}' "
                    "(available {})".format("', '".join(
                        list(self.selectors) + list(self.derivatives))))

    @property
    def column_names(self):
        return list(self.selectors) + list(self.derivatives)

    def add_derivative(self, name, frequency, format, path=None,
                       **kwargs):
        """Add a placeholder for a derivative in the dataset. This can
        then be referenced when connecting workflow outputs

        Parameters
        ----------
        name : str
            The name used to reference the dataset "column" for the
            derivative
        frequency : [type]
            The frequency of the derivative within the dataset
        format : FileFormat or type
            The file-format (for file-groups) or datatype (for fields)
            that the derivative will be stored in within the dataset
        path : str, default `name`
            The location of the derivative within the dataset

        Raises
        ------
        ArcanaUsageError
            [description]
        """
        try:
            self.frequency_enum[str(frequency)]
        except KeyError:
            raise ArcanaUsageError(
                f"Frequency '{frequency} does not match frequency_enum of dataset "
                f"({', '.join(str(f) for f in self.frequency_enum)})")
        if path is None:
            path = name
        if hasattr(format, 'file_group_cls'):
            spec = FileGroupSpec(path, format, frequency, **kwargs)
        else:
            spec = FieldSpec(path, format, frequency)
        self.derivatives[name] = spec

    def node(self, frequency, ids=None, **id_kwargs):
        """Returns the node associated with the given frequency and ids dict

        Parameters
        ----------
        frequency : DataFrequency or str
            The frequency of the node
        ids : Dict[DataFrequency, str], optional
            The IDs corresponding to the node to return
        **id_kwargs : Dict[str, str]
            Additional IDs corresponding to the node to return passed as
            kwargs

        Returns
        -------
        DataNode
            The selected data node

        Raises
        ------
        ArcanaUsageError
            Raised when attemtping to use IDs with the frequency associated
            with the root node
        ArcanaNameError
            If there is no node corresponding to the given ids
        """
        if ids is None:
            ids = {}
        else:
            ids = copy(ids)
        ids.update = {self.frequency_enum(f): i for f, i in id_kwargs.items()}
        # Parse str to frequency enums
        frequency = self.frequency_enum[str(frequency)]
        if frequency == self.root_freq:
            if ids:
                raise ArcanaUsageError(
                    f"Root nodes don't have any IDs ({ids})")
            return self.root_node
        ids_tuple = self._ids_tuple(ids)
        try:
            return self.root_node.subnodes[frequency][ids_tuple]
        except KeyError:
            raise ArcanaNameError(
                ids_tuple,
                f"{ids_tuple} not present in data tree "
                "({})".format(
                    str(i) for i in self.root_node.subnodes[frequency]))

    def nodes(self, frequency):
        return self.root_node.subnodes[frequency].values()
        
    def node_ids(self, frequency):
        return self.root_node.subnodes[frequency].keys()

    def add_node(self, frequency, ids):
        """Adds a node to the dataset, creating references to upper and lower
        layers in the data tree.

        Parameters
        ----------
        frequency : DataFrequency
            The frequency of the data_node
        ids : Dict[DataFrequency, str]
            The IDs of the node and all branching points the data tree
            above it. The keys should match the Enum used provided for the
            'frequency

        Raises
        ------
        ArcanaDataTreeConstructionError
            If frequency is not of self.frequency.cls
        ArcanaDataTreeConstructionError
            If inserting a multiple IDs of the same class within the tree if
            one of their ids is None
        """
        if not isinstance(frequency, self.frequency_enum):
            raise ArcanaDataTreeConstructionError(
                f"Provided frequency {frequency} is not of "
                f"{self.frequency_enum} type")
        # Check conversion to frequency cls
        ids = {self.frequency_enum[str(f)]: i for f, i in ids.items()}
        # Create new data node
        node = DataNode(frequency, ids, self)
        basis_ids = {ids[f] for f in frequency.layers if f in ids}
        ids_tuple = tuple(basis_ids.items())
        node_dict = self.root_node.subnodes[frequency]
        if node_dict:
            if ids_tuple in node_dict:
                raise ArcanaDataTreeConstructionError(
                    f"ID clash ({ids_tuple}) between nodes inserted into data "
                    "tree")
            existing_tuple = next(iter(node_dict))
            if not ids_tuple or not existing_tuple:
                raise ArcanaDataTreeConstructionError(
                    f"IDs provided for some {frequency} nodes but not others"
                    f"in data tree ({ids_tuple} and {existing_tuple})")
            new_freqs = tuple(zip(ids_tuple))[0]
            exist_freqs = tuple(zip(existing_tuple))[0]
            if new_freqs != exist_freqs:
                raise ArcanaDataTreeConstructionError(
                    f"Inconsistent IDs provided for nodes in {frequency} "
                    f"in data tree ({ids_tuple} and {existing_tuple})")
        node_dict[ids_tuple] = node
        node.supranodes[self.frequency_enum(0)] = self.root_node
        # Insert nodes for basis layers if not already present and link them
        # with inserted node
        for supra_freq in frequency.layers:
            # Select relevant IDs from those provided
            supra_ids = {
                str(f): ids[f] for f in supra_freq.layers if f in ids}
            sub_ids = tuple((f, i) for f, i in ids_tuple
                            if f not in supra_freq.layers)
            try:
                supranode = self.node(supra_freq, **supra_ids)
            except ArcanaNameError:
                supranode = self.add_node(supra_freq, **supra_ids)
            # Set reference to level node in new node
            node.supranodes[supra_freq] = supranode
            supranode.subnodes[frequency][sub_ids] = node
        return node

    def ids_tuple(self, ids):
        """Generates a tuple in consistent order from the passed ids that can
        be used as a key in a dictionary

        Parameters
        ----------
        ids : Dict[DataFrequency | str, str]
            A dictionary with IDs for each frequency that specifies the
            nodes position within the data tree

        Returns
        -------
        Tuple[(DataFrequency, str)]
            A tuple sorted in order of provided frequencies
        """
        try:
            return tuple((self.frequency_enum[str(f)], i)
                         for f, i in sorted(ids.items(), key=itemgetter(1)))
        except KeyError:
            raise ArcanaUsageError(
                f"Unrecognised data frequencies in ID dict '{ids}' (valid "
                f"{', '.join(self.frequency_enum)})")

    @property
    def root_frequency(self):
        return self.frequency_enum(0)

    def connect(self, workflow, input_map=None, output_map=None, formats=None,
                skip_missing=False):
        """
        Connects a workflow to the dataset by prepending nodes to select,
        download and convert (if necessary) data and connect it to the input
        of the workflow, then stores the output of the workflow back in the
        dataset.

        Parameters
        ----------
        workflow : pydra.Workflow
            The workflow to connect to the dataset.
        input_map : Dict[str, str]
            Mapping from a column names (values) to inputs of the workflow
            (keys). If an auxiliary file is required the name of the
            aux file is appended to the column name after a '.', e.g.
            magnitude.json
        output_map : Dict[str, str]
            Mapping from outputs of the workflow (values) to column names
            (keys). If an auxiliary file is generated in a non-standard path,
            (i.e. exactly the same path as the primary file except for file
            extension), the path to the aux file can be specified explicitly by
            appending name of the aux file to the column name after a '.',
            e.g. magnitude.json
        formats : Dict[str, FileFormat or type]
            Specify columns that need to be converted to/from the format
            that is stored in the dataset to interoperate with the workflow
        skip_missing : bool
            Whether to quietly skip data nodes which don't have matches for the
            requested input columns (otherwise raise an error)

        Returns
        -------
        pydra.Workflow
            An outer workflow that wraps the given workflow and connects it to
            the dataset
        """

        if input_map is not None:
            input_names = [c.split('.')[0] for c in input_map.values()]
        else:
            input_names = workflow.input_names

        if output_map is not None:
            output_names = [c.split('.')[0] for c in output_map]
        else:
            output_names = workflow.output_names

        if invalid_inputs := set(input_names) - set(self.column_names):
            raise ArcanaUsageError(
                "Workflow inputs '" + "', '".join(invalid_inputs)
                + "' are not present in the dataset ('"
                + "', '".join(self.column_names) + "')")

        if invalid_outputs := set(output_names) - set(self.derivatvies):
            raise ArcanaUsageError(
                "Workflow outputs '"
                + "', '".join(invalid_outputs)
                + "' are not present in the dataset derivatives ('"
                + "', '".join(self.derivatives) + "')")

        frequencies = set(self.column(o).frequency for o in workflow.outputs)
        if len(frequencies) > 1:
            raise ArcanaUsageError(
                "Inconsitent frequencies found in workflow outputs: "
                + ", ".join(frequencies))
        frequency = next(iter(frequencies))

        if unrecognised_formats := set(formats) - (set(input_names) |
                                                   set(output_names)):
            raise ArcanaUsageError(
                "Unrecognised formats '"
                + "', '".join(unrecognised_formats)
                + "'that are not referenced by the workflow ("
                + "', '".join(set(input_names) | set(output_names)) + ')')

        # We create an outer workflow that splits over data nodes at the
        # of the specified frequency and includes nodes to source inputs,
        # convert formats and sink outputs of the inner workflow
        outer_workflow = Workflow(
            name=f'{workflow.name}_connected_to_{self.name}')

        @mark.task
        @mark.annotate(
            {'dataset': Dataset,
             'frequency': DataFrequency,
             'input_names': ty.Sequence[str],
             'skip_missing': bool,
             'return': {
                 'data_nodes': ty.Sequence[DataNode]}})
        def select(dataset, frequency, input_names, skip_missing):
            "Selects inputs from the dataset using the provided `selectors`"
            selected_nodes = []
            for node in dataset.nodes(frequency):
                try:
                    for inpt in input_names:
                        dataset.column(inpt).match(node)
                except ArcanaSelectionError:
                    if not skip_missing:
                        raise
                selected_nodes.append(node)
            return selected_nodes

        outer_workflow.add(select(name='select',
                                  dataset=self,
                                  input_names=list(workflow.inputs),
                                  skip_missing=skip_missing,
                                  frequency=frequency))

        # Create a workflow that sits between the outer workflow
        # but outside the "inner" workflow provided to the method. This
        # workflow will be split over the data-nodes returned by the select
        # task
        split_workflow = Workflow(input_spec=['data_node'])

        @mark.task
        @mark.annotate(
            {'data_node': DataNode,
             'column_names': ty.Sequence[str],
             'return': {i: DataItem for i in input_names}})
        def source(data_node, column_names):
            outputs = []
            with data_node.dataset:
                for col_name in column_names:
                    item = data_node.item(col_name)
                    item.get()
                    outputs.append(item)
            return tuple(outputs)

        split_workflow.add(source(
            name='source', data_node=workflow.lzin.data_node,
            column_names=input_names))

        # Do format conversions if required
        wf_source = {}
        for col_name in input_names:
            sourced = getattr(workflow.download.lzout, col_name)
            col_format = self.column(col_name).format
            try:
                required_format = formats[col_name]
            except KeyError:
                required_format = col_format
            if required_format != col_format:
                # Get converter node
                converter = required_format.converter(col_format)
                converter_name = f"{col_name}_input_converter"
                # Insert converter
                split_workflow.add(converter(
                    name=converter_name,
                    in_file=sourced))
                # Map converter output to workflow output
                converted = getattr(split_workflow,
                                    converter_name).lzout.out_file
            else:
                converted = sourced
            if input_map is not None:
                pass
                # Map download directly to output (i.e. without conversion)
            wf_source[col_name] = sourced

        split_workflow.add(workflow(**wf_source))

        # Do format conversions if required
        wf_sink = {}
        for col_name in output_names:
            wf_output = getattr(workflow.lzout, col_name)
            col_format = self.column(col_name).format
            try:
                produced_format = formats[col_name]
            except KeyError:
                produced_format = col_format
            if produced_format != col_format:
                # Get converter node
                converter = col_format.converter(produced_format)
                converter_name = f"{col_name}_output_converter"
                # Insert converter
                split_workflow.add(converter(
                    name=converter_name,
                    in_file=wf_output))
                # Map converter output to workflow output
                wf_sink[col_name] = getattr(split_workflow,
                                            converter_name).lzout.out_file
            else:
                # Map download directly to output (i.e. without conversion)
                wf_sink[col_name] = wf_output

        def sink(data_node, **to_sink):
            with data_node.dataset:
                for col_name, item in to_sink.items():
                    node_item = data_node.set_item(col_name, item)
                    node_item.put()
            return data_node

        # Can't use a decorated function as we need to allow for dynamic
        # arguments
        sink_task = FunctionTask(
            sink,
            input_spec=SpecInfo(
                name='SinkInputs', bases=(BaseSpec,), fields=(
                    [('dataset', Dataset)]
                    + [(s, DataItem) for s in wf_sink])),
            output_spec=SpecInfo(
                name='SinkOutputs', bases=(BaseSpec,), fields=[
                    ('data_node', DataNode)]))

        split_workflow.add(sink_task(
            name='sink', data_node=split_workflow.lzin.data_node, **wf_sink))

        split_workflow.set_output(('data_node',
                                   split_workflow.sink.lzout.data_node))

        split_workflow_name = f'{workflow.name}_per_{frequency}'

        outer_workflow.add(
            split_workflow(
                name=split_workflow_name,
                data_node=outer_workflow.select.lzout.data_nodes)
            .split('data_node').combine('data_node'))

        outer_workflow.set_output(
            ('data_nodes',
             getattr(outer_workflow, split_workflow_name).lzout.data_node))

        return outer_workflow

    def workflow(self, name, inputs, outputs, frequency, ids,
                 required_formats=None, produced_formats=None):
        """Generate a Pydra task that sources the specified inputs from the
        dataset

        Parameters
        ----------
        name : str
            A name for the workflow (must be globally unique)
        inputs : Sequence[str]
            The inputs to be sourced from the dataset
        outputs : Sequence[str]
            The outputs to be sinked into the dataset
        frequency : DataFrequency
            The frequency of the nodes to draw the inputs from
        ids : Sequence[str]
            The sequence of IDs of the data nodes to include in the workflow
        required_formats : Dict[str, FileFormat]
            The required file formats for any inputs that need to be converted
            before they are used by the workflow
        produced_formats : Dict[str, FileFormat]
            The produced file formats for any outputs that need to be converted
            before storing in the dataset
        """

        if ids is None:
            ids = list(self.data_nodes(frequency))
            
        workflow = Workflow(name=name, input_spec=['id'],
                            inputs=[(ids,)]).split('ids')

        inputs_spec = {
            i: (DataItem if (frequency == i.frequency
                             or frequency.is_child(i.frequency))
                else ty.Sequence[DataItem])
            for i in inputs}

        outputs_spec = {o: DataItem for o in outputs}

        @mark.task
        @mark.annotate(
            {'dataset': Dataset,
             'frequency': DataFrequency,
             'id': str,
             'inputs': ty.Sequence[DataSelector],
             'return': inputs_spec})
        def retrieve(dataset, id, frequency, inputs):
            """Selects the items from the dataset corresponding to the input 
            selectors and retrieves them from the repository to a cache on 
            the host"""
            outputs = []
            data_node = dataset.node(frequency, id)
            with dataset.repository:
                for inpt in inputs:
                    item = inpt.match(data_node)
                    item.get()  # download to host if required
                    outputs.append(item)
            return tuple(outputs)

        workflow.add(retrieve(
            name='retrieve', frequency=frequency, inputs=inputs,
            id=workflow.lzin.id))

        selected = {i: getattr(workflow.source.lzout, i) for i in inputs}

        # Do format conversions if required
        for inpt_name, required_format in required_formats.items():
            inpt_format = inputs[inpt_name].format
            if required_format != inpt_format:
                cname = f"{inpt_name}_input_converter"
                converter_task = required_format.converter(inpt_format)(
                    name=cname, to_convert=selected[inpt_name])
                if inputs_spec[inpt_name] == ty.Sequence[DataItem]:
                    # Iterate over all items in the sequence and convert them
                    converter_task.split('to_convert')
                # Insert converter
                workflow.add(converter_task)
                # Map converter output to workflow output
                selected[inpt_name] = getattr(workflow, cname).lzout.converted

        

        # Can't use a decorated function as we need to allow for dynamic
        # arguments
        workflow.add(
            FunctionTask(
                identity,
                input_spec=SpecInfo(
                    name='SourceInputs', bases=(BaseSpec,),
                    fields=list(inputs_spec.items())),
                output_spec=SpecInfo(
                    name='SourceOutputs', bases=(BaseSpec,),
                    fields=list(inputs_spec.items())))(name='source',
                                                       **selected))

        # Can't use a decorated function as we need to allow for dynamic
        # arguments
        workflow.add(
            FunctionTask(
                identity,
                input_spec=SpecInfo(
                    name='SinkInputs', bases=(BaseSpec,),
                    fields=list(outputs_spec.items())),
                output_spec=SpecInfo(
                    name='SinkOutputs', bases=(BaseSpec,),
                    fields=list(outputs_spec.items()))))(name='sink')

        sinked = {o: getattr(workflow.sink, o) for o in outputs}

        # Do format conversions if required
        for outpt_name, produced_format in produced_formats.items():
            outpt_format = outputs[outpt_name].format
            if produced_format != outpt_format:
                cname = f"{outpt_name}_input_converter"
                # Insert converter
                workflow.add(outpt_format.converter(produced_format)(
                    name=cname, to_convert=sinked[outpt_name]))
                # Map converter output to workflow output
                sinked[outpt_name] = getattr(workflow, cname).lzout.converted        

        def store(data_node, **to_sink):
            with data_node.dataset:
                for col_name, item in to_sink.items():
                    node_item = data_node.set_item(col_name, item)
                    node_item.put()
            return data_node

        # Can't use a decorated function as we need to allow for dynamic
        # arguments
        workflow.add(
            FunctionTask(
                store,
                input_spec=SpecInfo(
                    name='SinkInputs', bases=(BaseSpec,), fields=(
                        [('dataset', Dataset)] + list(outputs_spec.items()))),
                output_spec=SpecInfo(
                    name='SinkOutputs', bases=(BaseSpec,), fields=[
                        ('data_node', DataNode)]))(name='store', **sinked))

        workflow.set_output(('data_nodes', store.lzout.data_node))

        return workflow


class DataNode():
    """A "node" in a data tree where file-groups and fields can be placed, e.g.
    a session or subject.

    Parameters
    ----------
    frequency : DataFrequency
        The frequency of the node
    ids : Dict[DataFrequency, str]
        The ids for each provided frequency need to specify the data node
        within the tree
    root : DataNode
        A reference to the root of the data tree
    """

    def __init__(self, frequency, ids, dataset):
        self.ids = ids
        self.frequency = frequency
        self._file_groups = OrderedDict()
        self._fields = OrderedDict()
        self._provenances = OrderedDict()
        self.subnodes = defaultdict(dict)
        self.supranodes = {}
        self._dataset = dataset

    def __eq__(self, other):
        if not (isinstance(other, type(self))
                or isinstance(self, type(other))):
            return False
        return (tuple(self._file_groups) == tuple(other._file_groups)
                and tuple(self._fields) == tuple(other._fields)
                and tuple(self._provenances) == tuple(other._provenances))

    def __hash__(self):
        return (hash(tuple(self._file_groups)) ^ hash(tuple(self._fields))
                ^ hash(tuple(self._provenances)))

    def add_file_group(self, name_path, *args, **kwargs):
        if name_path in self._file_groups:
            raise ArcanaNameError(
                f"{name_path} conflicts with existing file-group")
        self._file_groups[name_path] = UnresolvedFileGroup(
            name_path, *args, data_node=self, **kwargs)

    def add_field(self, name_path, *args, **kwargs):
        if name_path in self._fields:
            raise ArcanaNameError(
                f"{name_path} conflicts with existing field")
        self._fields[name_path] = UnresolvedField(
            name_path, *args, data_node=self, **kwargs)

    def __getitem__(self, name):
        """Get's the item that matches the dataset's selector

        Parameters
        ----------
        name : str
            Name of the selector in the parent Dataset that is used to select
            a file-group or field in the node
        """

        return self._dataset.selectors[name].match(self)

    def file_group(self, name_path: str, file_format=None):
        """
        Gets the file_group at the name_path, different from the mapped path
        corresponding to the dataset's selectors

        Parameters
        ----------
        name_path : str
            The name_path to the file_group within the tree node, e.g. anat/T1w
        file_format : FileFormat | Sequence[FileFormat] | None
            A file format, or sequence of file formats, which are used to
            resolve the format of the file-group

        Returns
        -------
        FileGroup or UnresolvedFileGroup
            The file-group corresponding to the given name_path. If a, or
            multiple, candidate file formats are provided then the format of
            the file-group is resolved and a FileGroup object is returned.
            Otherwise, an UnresolvedFileGroup is returned instead.
        """
        try:
            file_group = self._file_groups[name_path]
        except KeyError:
            raise ArcanaNameError(
                name_path,
                f"{self} doesn't have a file_group at the name_path "
                f"{name_path} (available '"
                + "', '".join(self.file_groups) + "')")
        else:
            if file_format is not None:
                file_group = file_group.resolve_format(file_format)
        return file_group

    def field(self, name_path):
        """
        Gets the field named 'name_path'

        Parameters
        ----------
        name_path : str
            The name_path of the field within the node
        """
        # if isinstance(name, FieldMixin):
        #     if namespace is None and name.derived:
        #         namespace = name.analysis.name
        #     name = name.name
        try:
            return self._fields[name_path]
        except KeyError:
            raise ArcanaNameError(
                name_path, ("{} doesn't have a field named '{}' "
                            "(available '{}')").format(
                    self, name_path, "', '".join(self._fields)))

    @property
    def file_groups(self):
        return self._file_groups.values()

    @property
    def fields(self):
        return self._fields.values()

    @property
    def items(self):
        return chain(self.file_groups, self.fields)

    def __ne__(self, other):
        return not (self == other)

    @property
    def ids_tuple(self):
        return self.dataset.ids_tuple(self.ids)

    def find_mismatch(self, other, indent=''):
        """
        Highlights where two nodes differ in a human-readable form

        Parameters
        ----------
        other : TreeNode
            The node to compare
        indent : str
            The white-space with which to indent output string

        Returns
        -------
        mismatch : str
            The human-readable mismatch string
        """
        if self != other:
            mismatch = "\n{}{}".format(indent, type(self).__name__)
        else:
            mismatch = ''
        sub_indent = indent + '  '
        if len(list(self.file_groups)) != len(list(other.file_groups)):
            mismatch += ('\n{indent}mismatching summary file_group lengths '
                         '(self={} vs other={}): '
                         '\n{indent}  self={}\n{indent}  other={}'
                         .format(len(list(self.file_groups)),
                                 len(list(other.file_groups)),
                                 list(self.file_groups),
                                 list(other.file_groups),
                                 indent=sub_indent))
        else:
            for s, o in zip(self.file_groups, other.file_groups):
                mismatch += s.find_mismatch(o, indent=sub_indent)
        if len(list(self.fields)) != len(list(other.fields)):
            mismatch += ('\n{indent}mismatching summary field lengths '
                         '(self={} vs other={}): '
                         '\n{indent}  self={}\n{indent}  other={}'
                         .format(len(list(self.fields)),
                                 len(list(other.fields)),
                                 list(self.fields),
                                 list(other.fields),
                                 indent=sub_indent))
        else:
            for s, o in zip(self.fields, other.fields):
                mismatch += s.find_mismatch(o, indent=sub_indent)
        if len(list(self.provenances)) != len(list(other.provenances)):
            mismatch += ('\n{indent}mismatching summary provenance lengths '
                         '(self={} vs other={}): '
                         '\n{indent}  self={}\n{indent}  other={}'
                         .format(len(list(self.provenances)),
                                 len(list(other.provenances)),
                                 list(self.provenances),
                                 list(other.provenances),
                                 indent=sub_indent))
        else:
            for s, o in zip(self.provenances, other.provenances):
                mismatch += s.find_mismatch(o, indent=sub_indent)
        return mismatch

    def infer_ids(self, ids):
        """Infers IDs of primary data frequencies from those are provided from
        the `id_inference` dictionary passed to the dataset init.

        Parameters
        ----------
        ids : Dict[DataFrequency, str]
            Set of IDs specifying a data-node

        Returns
        -------
        Dict[DataFrequency, str]
            A copied ID dictionary with inferred IDs inserted into it

        Raises
        ------
        ArcanaBadlyFormattedIDError
            raised if one of the IDs doesn't match the pattern in the
            `id_inference`
        """
        ids = copy(ids)
        if callable(self.id_inference):
            return self.id_inference(ids)
        for source, regex in self.id_inference:
            match = re.match(regex, ids[source])
            if match is None:
                raise ArcanaBadlyFormattedIDError(
                    f"{source} ID '{ids[source]}', does not match ID inference"
                    f" pattern '{regex}'")
            for target, id in match.groupdict.items():
                ids[self.frequency_enum[target]] = id
        return ids


class MultiDataset(Dataset):
    """A dataset created by combining multiple datasets into a conglomerate

    Parameters
    ----------
    """

    def __init__(self):
        raise NotImplemented


def identity(**kwargs):
    "Returns the keyword arguments as a tuple"
    return tuple(kwargs.values())
