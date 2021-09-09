from __future__ import annotations
from operator import itemgetter, __or__
from itertools import combinations
import logging
import typing as ty
from enum import EnumMeta
import re
from copy import copy
import attr
from attr.converters import default_if_none
from pydra import Workflow, mark
from pydra.engine.task import FunctionTask
from pydra.engine.specs import BaseSpec, SpecInfo
from arcana2.exceptions import (
    ArcanaNameError, ArcanaDataTreeConstructionError, ArcanaUsageError,
    ArcanaBadlyFormattedIDError)
from .item import DataItem
from .enum import DataDimension
from .spec import DataSink, DataSource
from .. import repository
from .node import DataNode


logger = logging.getLogger('arcana')


@attr.s
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
        system by providing a FileSystem repo.
    hierarchy : Sequence[DataDimension]
        The data frequencies that are explicitly present in the data tree.
        For example, if a FileSystem dataset (i.e. directory) has
        two layer hierarchy of sub-directories, the first layer of
        sub-directories labelled by unique subject ID, and the second directory
        layer labelled by study time-point then the hierarchy would be

            [Clinical.subject, Clinical.timepoint]

        Alternatively, in some repositories (e.g. XNAT) the second layer in the
        hierarchy may be named with session ID that is unique across the project,
        in which case the layer dimensions would instead be

            [Clinical.subject, Clinical.session]
        
        In such cases, if there are multiple timepoints, the timepoint ID of the
        session will need to be extracted using the `id_inference` argument.

        Alternatively, the hierarchy could be organised such that the tree
        first splits on longitudinal time-points, then a second directory layer
        labelled by member ID, with the final layer containing sessions of
        matched members labelled by their groups (e.g. test & control):

            [Clinical.timepoint, Clinical.member, Clinical.group]

        Note that the combination of layers in the hierarchy must span the
        space defined in the DataDimension enum, i.e. the "bitwise or" of the
        layer values of the hierarchy must be 1 across all bits
        (e.g. Clinical.session: 0b111).
    sources : Dict[str, DataSource]
        The DataSources to add to the dataset and the names to refer to them
        by.
    sinks : Dict[str, DataSink]
        The DataSinks to add to the dataset and with the name to refer to them
        by.
    id_inference : Dict[DataDimension, str]
        Not all IDs will appear explicitly within the hierarchy of the data
        tree, and some will need to be inferred by extracting components of
        more specific lables.

        For example, given a set of subject IDs that combination of the ID of
        the group that they belong to and the member ID within that group
        (i.e. matched test & control would have same member ID)

            CONTROL01, CONTROL02, CONTROL03, ... and TEST01, TEST02, TEST03

        the group ID can be extracted by providing the a list of tuples
        containing ID to source the inferred IDs from coupled with a regular
        expression with named groups

            id_inference=[(Clinical.subject,
                           r'(?P<group>[A-Z]+)(?P<member>[0-9]+)')}

        Alternatively, a general function with signature `f(ids)` that returns
        a dictionary with the mapped IDs can be provided instead.
    included : Dict[DataDimension, List[str]]
        The IDs to be included in the dataset per frequency. E.g. can be
        used to limit the subject IDs in a project to the sub-set that passed
        QC. If a frequency is omitted or its value is None, then all available
        will be used
    excluded : Dict[DataDimension, List[str]]
        The IDs to be excluded in the dataset per frequency. E.g. can be
        used to exclude specific subjects that failed QC. If a frequency is
        omitted or its value is None, then all available will be used
    """

    name: str = attr.ib()
    repository: repository.Repository = attr.ib()
    hierarchy: list[DataDimension] = attr.ib()
    sources: dict[str, DataSource] or None = attr.ib(
        factory=dict, converter=default_if_none(factory=dict))
    sinks: dict[str, DataSink] or None = attr.ib(
        factory=dict, converter=default_if_none(factory=dict))
    id_inference: (list[tuple[DataDimension, str]]
                   or ty.Callable) = attr.ib(
                       factory=list, converter=default_if_none(factory=list))
    included: dict[DataDimension, ty.List[str]] = attr.ib(
        factory=dict, converter=default_if_none(factory=dict))
    excluded: dict[DataDimension, ty.List[str]] = attr.ib(
        factory=dict, converter=default_if_none(factory=dict))
    workflows: dict[str, Workflow] = attr.ib(factory=dict)
    _root_node: DataNode = attr.ib(default=None, init=False)  

    @sources.validator
    def sources_validator(self, _, sources):
        if wrong_freq := [m for m in sources.values()
                          if not isinstance(m.frequency, self.dimensions)]:
            raise ArcanaUsageError(
                f"Data hierarchy of {wrong_freq} sources does not match "
                f"that of repository {self.dimensions}")

    @sinks.validator
    def sinks_validator(self, _, sinks):
        if overlapping := (set(self.sources) & set(sinks)):
            raise ArcanaUsageError(
                "Name-path clashes between sources and sinks ("
                "', '".join(overlapping) + "')")

    @excluded.validator
    def excluded_validator(self, _, excluded):
        if both:= [f for f in self.included
                   if (self.included[f] is not None
                       and excluded[f] is not None)]:
            raise ArcanaUsageError(
                    "Cannot provide both 'included' and 'excluded' arguments "
                    "for frequencies ('{}') to Dataset".format(
                        "', '".join(both)))

    @hierarchy.validator
    def hierarchy_validator(self, _, hierarchy):
        if not hierarchy:
            raise ArcanaUsageError(
                f"hierarchy provided to {self} cannot be empty")            
        if not_valid := [f for f in hierarchy
                         if not isinstance(f, self.dimensions)]:
            raise ArcanaUsageError(
                "{} are not part of the {} data dimensions"
                .format(', '.join(not_valid), self.dimensions))
        # Check that all data frequencies are "covered" by the hierarchy and
        # each subsequent
        covered = self.dimensions(0)
        for i, layer in enumerate(hierarchy):
            diff = layer - covered
            if not diff:
                raise ArcanaUsageError(
                    f"{layer} does not add any additional basis layers to "
                    f"previous layers {hierarchy[i:]}")
            covered |= layer
        if covered != max(self.dimensions):
            raise ArcanaUsageError(
                f"The data hierarchy {hierarchy} does not cover the following "
                f"basis frequencies "
                + ', '.join(str(m) for m in (~covered).basis()) +
                f"f the {self.dimensions} data dimensions")

    def __getitem__(self, key):
        if key == self.dimensions(0):
            return self.root_node
        else:
            return self.root_node.children[key]

    @property
    def dimensions(self):
        return type(self.hierarchy[0])

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
            self._root_node = DataNode(self.dimensions(0), {}, self)
            self.repository.construct_tree(self)
        return self._root_node

    def column_spec(self, name):
        try:
            return self.sources[name]
        except KeyError:
            try:
                return self.sinks[name]
            except KeyError:
                raise ArcanaNameError(
                    f"No column with the name path '{name}' "
                    "(available {})".format("', '".join(
                        list(self.sources) + list(self.sinks))))

    @property
    def column_names(self):
        return list(self.sources) + list(self.sinks)

    def add_sink(self, name, frequency, format, path=None,
                       **kwargs):
        """Add a placeholder for a sink in the dataset. This can
        then be referenced when connecting workflow outputs

        Parameters
        ----------
        name : str
            The name used to reference the dataset "column" for the
            sink
        frequency : [type]
            The frequency of the sink within the dataset
        format : FileFormat or type
            The file-format (for file-groups) or datatype (for fields)
            that the sink will be stored in within the dataset
        path : str, default `name`
            The location of the sink within the dataset

        Raises
        ------
        ArcanaUsageError
            [description]
        """
        try:
            self.dimensions[str(frequency)]
        except KeyError:
            raise ArcanaUsageError(
                f"Frequency '{frequency} does not match dimensions of "
                "dataset ({})".format(
                    ', '.join(str(f) for f in self.dimensions)))
        if path is None:
            path = name
        self.sinks[name] = DataSink(path, format, frequency, **kwargs)

    def node(self, frequency=None, id=None, **id_kwargs):
        """Returns the node associated with the given frequency and ids dict

        Parameters
        ----------
        frequency : DataDimension or str
            The frequency of the node
        id : str or Tuple[str], optional
            The ID of the node to 
        **id_kwargs : Dict[str, str]
            Alternatively to providing `id`, ID corresponding to the node to
            return passed as kwargs

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
        node = self.root_node
        # Parse str to frequency enums
        if not frequency.value:
            if id is not None:
                raise ArcanaUsageError(
                    f"Root nodes don't have any IDs ({id})")
            return self.root_node
        if id_kwargs:
            if id is not None:
                raise ArcanaUsageError(
                    f"ID ({id}) and id_kwargs ({id_kwargs}) cannot be both "
                    f"provided to `node` method of {self}")
            # Convert to the DataDimension of the dataset
            node = self.root_node
            for freq, id in id_kwargs.items():
                try:
                    children_dict = node.children[self.dimensions[freq]]
                except KeyError:
                    raise ArcanaNameError(
                        freq, f"{freq} is not a child frequency of {node}")
                try:
                    node = children_dict[id]
                except KeyError:
                    raise ArcanaNameError(
                        id, f"{id} ({freq}) not a child node of {node}")
        else:
            try:
                return self.root_node.children[frequency][id]
            except KeyError:
                raise ArcanaNameError(
                    id, f"{id} not present in data tree "
                    f"({list(self.node_ids)})")

    def nodes(self, frequency):
        return self.root_node.children[frequency].values()
        
    def node_ids(self, frequency):
        return self.root_node.children[frequency].keys()

    def add_leaf_node(self, tree_path):
        """Creates a new node at a the path down the tree of the dataset as
        well as all "parent" nodes upstream in the data tree

        Parameters
        ----------
        tree_path : Sequence[str]
            The sequence of labels for each layer in the hierarchy of the
            dataset leading to the current node.

        Raises
        ------
        ArcanaBadlyFormattedIDError
            raised if one of the IDs doesn't match the pattern in the
            `id_inference`
        ArcanaDataTreeConstructionError
            raised if one of the groups specified in the ID inference reg-ex
            doesn't match a valid frequency in the data dimensions
        """
        # Get basis frequencies covered at the given depth of the
        if len(tree_path) != len(self.hierarchy):
            raise ArcanaDataTreeConstructionError(
                f"Tree path ({tree_path}) should have the same length as "
                f"the hierarchy ({self.hierarchy}) of {self}")
        # Set a default ID of None for all parent frequencies that could be
        # inferred from a node at this depth
        ids = {f: None for f in self.dimensions}
        # Calculate the combined freqs after each layer is added
        frequency = self.dimensions(0)
        for layer, label in zip(self.hierarchy, tree_path):
            ids[layer] = label
            try:
                regex = self.id_inference[layer]
            except KeyError:
                # If the layer introduces completely new bases then the basis
                # with the least significant bit (the order of the bits in the
                # DataDimension class should be arranged to account for this)
                # can be considered be considered to be equivalent to the label.
                # E.g. Given a hierarchy of [Clinical.subject, Clinical.session]
                # no groups are assumed to be present by default (although this
                # can be overridden by the `id_inference` attr) and the `member`
                # ID is assumed to be equivalent to the `subject` ID. Conversely,
                # the timepoint can't be inferred from the `session` ID, since
                # the session ID could be expected to contain the `member` and
                # `group` ID in it, and should be explicitly extracted by
                # providing a regex to `id_inference`, e.g. 
                #
                #       session ID: MRH010_CONTROL03_MR02
                #
                # with the '02' part representing as the timepoint can be
                # extracted with the
                #
                #       id_inference={
                #           Clinical.session: r'.*(?P<timepoint>0-9+)$'}   
                if not (layer & frequency):
                    ids[layer.basis()[-1]] = label
            else:
                match = re.match(regex, label)
                if match is None:
                    raise ArcanaBadlyFormattedIDError(
                        f"{layer} label '{label}', does not match ID inference"
                        f" pattern '{regex}'")
                new_freqs = layer - (layer & frequency)
                for target_freq, target_id in match.groupdict.items():
                    if (target_freq & new_freqs) != target_freq:
                        raise ArcanaUsageError(
                            f"Inferred ID target, {target_freq}, is not a "
                            f"data frequency added by layer {layer}")
                    if ids[target_freq] is not None:
                        raise ArcanaUsageError(
                            f"ID '{target_freq}' is specified twice in the ID "
                            f"inference of {tree_path} ({ids[target_freq]} "
                            f"and {target_id} from {regex}")
                    ids[target_freq] = target_id
            frequency |= layer
        assert(frequency == max(self.dimensions))
        # Create composite IDs for non-basis frequencies if they are not
        # explicitly in the layer dimensions
        for freq in (set(self.dimensions) - set(frequency.basis())):
            if ids[freq] is None:
                ids[freq] = tuple(ids[b] for b in freq.basis())
        self.add_node(self, ids, frequency)

    def add_node(self, ids, frequency):
        """Adds a node to the dataset, creating all parent "aggregate" nodes
        (e.g. for each subject, group or timepoint) where required

        Parameters
        ----------
        node: DataNode
            The node to add into the data tree

        Raises
        ------
        ArcanaDataTreeConstructionError
            If inserting a multiple IDs of the same class within the tree if
            one of their ids is None
        """
        node = DataNode(self, ids, frequency)
        # Create new data node
        node_dict = self.root_node.children[node.frequency]
        if node.id in node_dict:
            raise ArcanaDataTreeConstructionError(
                f"ID clash ({node.id}) between nodes inserted into data "
                "tree")
        node_dict[node.id] = node
        # Insert root node
        node.parents[self.dimensions(0)] = self.root_node
        # Insert parent nodes if not already present and link them with
        # inserted node
        for parent_freq, parent_id in node.ids.items():
            diff_freq = node.frequency - parent_freq
            if diff_freq:
                try:
                    parent_node = self.node(parent_freq, parent_id)
                except ArcanaNameError:
                    parent_ids = {f: i for f, i in node.ids.items()
                                  if f.is_parent(parent_freq)}
                    parent_node = self.add_node(DataNode(self, parent_ids,
                                                         parent_freq))
                # Set reference to level node in new node
                node.parents[parent_freq] = parent_node
                diff_id = node.ids[diff_freq]
                children_dict = parent_node.children[diff_freq]
                if diff_id in children_dict:
                    raise ArcanaDataTreeConstructionError(
                        f"ID clash ({diff_id}) between nodes inserted into "
                        f"data tree in {diff_freq} children of {parent_node} "
                        f"({children_dict[diff_id]} and {node})")
                children_dict[diff_id] = node
        return node

    def workflow(self, name, inputs, outputs, frequency=None,
                 workflow_formats=None, overwrite=False):
        """Generate a Pydra task that sources the specified inputs from the
        dataset

        Parameters
        ----------
        name : str
            A name for the workflow (must be globally unique)
        inputs : Dict[str, DataSource]
            The inputs to be sourced from the dataset
        outputs : Dict[str, DataSink]
            The outputs to be sinked into the dataset
        frequency : DataDimension or None
            The frequency of the nodes to draw the inputs from. Defaults to the
            the lowest level of the tree (i.e. max(self.dimensions))
        ids : Sequence[str]
            The sequence of IDs of the data nodes to include in the workflow
        workflow_formats : Dict[str, FileFormat]
            The required/produced file formats for any inputs/outputs that need
            to be converted before they are used by the workflow or after they
            are generated by the workflow respectively
        """
        if frequency is None:
            frequency = max(self.dimensions)
        if workflow_formats is None:
            workflow_formats = {}
            
        workflow = Workflow(name=name, input_spec=['id']).split('id')

        inputs_spec = {
            i: (DataItem if (frequency == s.frequency
                             or frequency.is_child(s.frequency))
                else ty.Sequence[DataItem])
            for i, s in inputs.items()}

        for inpt_name, source in inputs.items():
            if inpt_name in self.sources:
                if overwrite:
                    logger.info(
                        f"Overwriting source '{inpt_name}'")
                else:
                    raise ArcanaUsageError(
                        f"Attempting to overwriting '{inpt_name}' source "
                        f"with output from workflow '{name}'. Use 'overwrite' "
                        "option if this is desired")
            self.sources[inpt_name] = source

        outputs_spec = {o: DataItem for o in outputs}

        for out_name, out_spec in outputs.items():
            if out_name in self.sinks:
                if overwrite:
                    logger.info(
                        f"Overwriting sink '{out_name}' with workflow "
                        f"'{name}'")
                else:
                    raise ArcanaUsageError(
                        f"Attempting to overwriting '{out_name}' sink "
                        f"with workflow '{name}'. Use 'overwrite' option "
                        "if this is desired")
            self.sinks[out_name] = out_spec
            self.workflows[out_name] = workflow

        @mark.task
        @mark.annotate(
            {'dataset': Dataset,
             'frequency': DataDimension,
             'id': str,
             'input_names': ty.Sequence[str],
             'return': inputs_spec})
        def retrieve(dataset, frequency, id, input_names):
            """Selects the items from the dataset corresponding to the input 
            sources and retrieves them from the repository to a cache on 
            the host"""
            outputs = []
            data_node = dataset.node(frequency, id)
            with dataset.repository:
                for inpt_name in input_names:
                    item = data_node[inpt_name]
                    item.get()  # download to host if required
                    outputs.append(item)
            return tuple(outputs)

        workflow.add(retrieve(
            name='retrieve', dataset=self, frequency=frequency,
            inputs=list(inputs), id=workflow.lzin.id))

        retrieved = {i: getattr(workflow.retrieve.lzout, i) for i in inputs}

        # Do format conversions if required
        for inpt_name, input in inputs.items():
            inpt_format = input.data_format
            try:
                required_format = workflow_formats[inpt_name]
            except KeyError:
                required_format = inpt_format
            if required_format != inpt_format:
                cname = f"{inpt_name}_input_converter"
                converter_task = required_format.converter(inpt_format)(
                    name=cname, to_convert=retrieved[inpt_name])
                if inputs_spec[inpt_name] == ty.Sequence[DataItem]:
                    # Iterate over all items in the sequence and convert them
                    converter_task.split('to_convert')
                # Insert converter
                workflow.add(converter_task)
                # Map converter output to workflow output
                retrieved[inpt_name] = getattr(workflow, cname).lzout.converted

        # Can't use a decorated function as we need to allow for dynamic
        # arguments

        workflow.add(
            FunctionTask(
                name='source',
                func=identity,
                input_spec=SpecInfo(
                    name='SourceInputs', bases=(BaseSpec,),
                    fields=list(inputs_spec.items())),
                output_spec=SpecInfo(
                    name='SourceOutputs', bases=(BaseSpec,),
                    fields=list(inputs_spec.items())),
                **retrieved))

        # Can't use a decorated function as we need to allow for dynamic
        # arguments 
        workflow.add(FunctionTask(
            name='sink',
            func=identity,
            input_spec=SpecInfo(
                name='SinkInputs', bases=(BaseSpec,),
                fields=list(outputs_spec.items())),
            output_spec=SpecInfo(
                name='SinkOutputs', bases=(BaseSpec,),
                fields=list(outputs_spec.items()))))

        sinked = {o: getattr(workflow.sink.lzout, o) for o in outputs}

        # Do format conversions if required
        for outpt_name, output in outputs.items():
            outpt_format = output.data_format
            try:
                produced_format = workflow_formats[outpt_format]
            except KeyError:
                produced_format = outpt_format
            if produced_format != outpt_format:
                cname = f"{outpt_name}_input_converter"
                # Insert converter
                workflow.add(outpt_format.converter(produced_format)(
                    name=cname, to_convert=sinked[outpt_name]))
                # Map converter output to workflow output
                sinked[outpt_name] = getattr(workflow, cname).lzout.converted        

        def store(dataset, frequency, id, **to_sink):
            data_node = dataset.node(frequency, id)
            with dataset.repository:
                for outpt_name, outpt_value in to_sink.items():
                    node_item = data_node[outpt_name]
                    node_item.value = outpt_value
                    node_item.put() # Store value/path in repository
            return data_node

        # Can't use a decorated function as we need to allow for dynamic
        # arguments
        workflow.add(
            FunctionTask(
                store,
                input_spec=SpecInfo(
                    name='SinkInputs', bases=(BaseSpec,), fields=(
                        [('data_node', DataNode),
                         ('frequency', DataDimension),
                         ('id', str),
                         ('outputs', ty.Dict[str, DataSink])]
                        + list(outputs_spec.items()))),
                output_spec=SpecInfo(
                    name='SinkOutputs', bases=(BaseSpec,), fields=[
                        ('data_node', DataNode)]),
                name='store',
                dataset=self,
                frequency=frequency,
                id=workflow.lzin.id,
                **sinked))

        workflow.set_output(('data_nodes', workflow.store.lzout.data_node))
        return workflow

@attr.s
class SplitDataset():
    """A dataset created by combining multiple datasets into a conglomerate

    Parameters
    ----------
    """

    source_dataset: Dataset = attr.ib()
    sink_dataset: Dataset = attr.ib()



def identity(**kwargs):
    "Returns the keyword arguments as a tuple"
    return tuple(kwargs.values())
