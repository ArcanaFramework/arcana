from __future__ import annotations
from operator import itemgetter
import logging
import typing as ty
from enum import EnumMeta
import re
from copy import copy
import attr
from pydra import Workflow, mark
from pydra.engine.task import FunctionTask
from pydra.engine.specs import BaseSpec, SpecInfo
from arcana2.exceptions import (
    ArcanaNameError, ArcanaDataTreeConstructionError, ArcanaUsageError,
    ArcanaBadlyFormattedIDError)
from arcana2.core.utils import to_list, to_dict
from .item import DataItem
from .enum import DataHierarchy
from .spec import DataSpec
from .selector import DataSelector
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
    selectors : Dict[str, Selector]
        A dictionary that maps "name-paths" of input "columns" in the dataset
        to criteria in a Selector object that select the corresponding
        items in the dataset
    derivatives : Dict[str, Spec]
        A dictionary that maps "name-paths" of derivatives analysis workflows
        to be stored in the dataset
    included : Dict[DataHierarchy, List[str]]
        The IDs to be included in the dataset for each frequency. E.g. can be
        used to limit the subject IDs in a project to the sub-set that passed
        QC. If a frequency is omitted or its value is None, then all available
        will be used
    excluded : Dict[DataHierarchy, List[str]]
        The IDs to be excluded in the dataset for each frequency. E.g. can be
        used to exclude specific subjects that failed QC. If a frequency is
        omitted or its value is None, then all available will be used
    data_structure : EnumMeta
        The DataHierarchy enum that defines the frequencies (e.g. per-session,
        per-subject,...) present in the dataset.
    layers : list[DataHierarchy] or None
        The data frequencies from the data structure that are explicitly in the
        data tree. Only relevant for repositories with flexible tree structures
        (e.g. FileSystem). E.g. if a file-system dataset (i.e. directory) has
        two layers, corresponding to subjects and sessions it would be
        [Clinical.subject, Clinical.session]
    id_inference : Sequence[(DataHierarchy, str)] or Callable
        Specifies how IDs of primary data frequencies that not explicitly
        provided are inferred from the IDs that are. For example, given a set
        of subject IDs that combination of the ID of the group that they belong
        to and their member IDs (i.e. matched test/controls have same member ID)

            CONTROL01, CONTROL02, CONTROL03, ... and TEST01, TEST02, TEST03

        the group ID can be extracted by providing the a list of tuples
        containing ID to source the inferred IDs from coupled with a regular
        expression with named groups

            id_inference=[(Clinical.subject,
                           r'(?P<group>[A-Z]+)(?P<member>[0-9]+)')}

        Alternatively, a general function with signature `f(ids)` that returns
        a dictionary with the mapped IDs can be provided instead.
    """

    name: str = attr.ib()
    repository: repository.Repository = attr.ib()
    data_structure: EnumMeta  = attr.ib()
    selectors: list[DataSelector] or None = attr.ib(
        factory=list, converter=to_list)
    derivatives: list[DataSpec] or None = attr.ib(
        factory=list, converter=to_list)
    included: dict[DataHierarchy, ty.List[str]] = attr.ib(
        factory=dict, converter=to_dict)
    excluded: dict[DataHierarchy, ty.List[str]] = attr.ib(
        factory=dict, converter=to_dict)
    layers: list[DataHierarchy] = attr.ib()
    id_inference: (list[tuple[DataHierarchy, str]]
                   or ty.Callable) = attr.ib(factory=list, converter=to_list)
    _root_node: DataNode = attr.ib(default=None, init=False)

    @selectors.validator
    def selectors_validator(self, _, selectors):
        if wrong_freq := [m for m in selectors.values()
                          if not isinstance(m.frequency, self.data_structure)]:
            raise ArcanaUsageError(
                f"Data layers of {wrong_freq} selectors does not match "
                f"that of repository {self.data_structure}")

    @derivatives.validator
    def derivatives_validator(self, _, derivatives):
        if overlapping := (set(self.selectors) & set(derivatives)):
            raise ArcanaUsageError(
                "Name-path clashes between selectors and derivatives ("
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

    @layers.default
    def layers_default(self):
        """Default to a single layer that includes all the basis frequencies
        e.g. 'session' for Clinical data structure (which includes the 'group'
        'member' and 'timepoint' "basis" frequencies)
        """
        return [max(self.data_structure)]

    @layers.validator
    def layers_validator(self, _, layers):
        if not_valid := [f for f in layers
                         if not isinstance(f, self.data_structure)]:
            raise ArcanaUsageError(
                "{} are not part of the {} data structure"
                .format(', '.join(not_valid), self.data_structure))
        upper_layers = 

    def __enter__(self):
        self.repository.__enter__()
        return self

    def __exit__(self):
        self.repository.__exit__()

    def __getitem__(self, key):
        if key == self.data_structure(0):
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
            self._root_node = DataNode(self.data_structure(0), {}, self)
            self.repository.construct_tree(self)
        return self._root_node

    def column_spec(self, name):
        try:
            return self.selectors[name]
        except KeyError:
            try:
                return self.derivatives[name]
            except KeyError:
                raise ArcanaNameError(
                    f"No column with the name path '{name}' "
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
            self.data_structure[str(frequency)]
        except KeyError:
            raise ArcanaUsageError(
                f"Frequency '{frequency} does not match data_structure of "
                "dataset ({})".format(
                    ', '.join(str(f) for f in self.data_structure)))
        if path is None:
            path = name
        self.derivatives[name] = DataSpec(path, format, frequency, **kwargs)

    def node(self, frequency, ids=None, **id_kwargs):
        """Returns the node associated with the given frequency and ids dict

        Parameters
        ----------
        frequency : DataHierarchy or str
            The frequency of the node
        ids : Dict[DataHierarchy, str], optional
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
        ids.update({self.data_structure(f): i for f, i in id_kwargs.items()})
        # Parse str to frequency enums
        frequency = self.data_structure[str(frequency)]
        if not frequency.value:
            if ids:
                raise ArcanaUsageError(
                    f"Root nodes don't have any IDs ({ids})")
            return self.root_node
        ids_tuple = self.make_ids_tuple(ids)
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
        frequency : DataHierarchy
            The frequency of the data_node
        ids : Dict[DataHierarchy, str]
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
        if not isinstance(frequency, self.data_structure):
            raise ArcanaDataTreeConstructionError(
                f"Provided frequency {frequency} is not of "
                f"{self.data_structure} type")
        # Check conversion to frequency cls
        ids = {self.data_structure[str(f)]: i for f, i in ids.items()}
        # Create new data node
        node = DataNode(frequency, ids, self)
        basis_ids = {f: ids[f] for f in frequency.layers() if f in ids}
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
        node.supranodes[self.data_structure(0)] = self.root_node
        # Insert nodes for basis layers if not already present and link them
        # with inserted node
        for supra_freq in frequency.layers():
            # Select relevant IDs from those provided
            supra_ids = {
                str(f): ids[f] for f in supra_freq.layers() if f in ids}
            sub_ids = tuple((f, i) for f, i in ids_tuple
                            if f not in supra_freq.layers())
            try:
                supranode = self.node(supra_freq, **supra_ids)
            except ArcanaNameError:
                supranode = self.add_node(supra_freq, supra_ids)
            # Set reference to level node in new node
            node.supranodes[supra_freq] = supranode
            supranode.subnodes[frequency][sub_ids] = node
        return node

    def make_ids_tuple(self, ids):
        """Generates a tuple in consistent order from the passed ids that can
        be used as a key in a dictionary

        Parameters
        ----------
        ids : Dict[DataHierarchy | str, str]
            A dictionary with IDs for each frequency that specifies the
            nodes position within the data tree

        Returns
        -------
        Tuple[(DataHierarchy, str)]
            A tuple sorted in order of provided frequencies
        """
        try:
            return tuple((self.data_structure[str(f)], i)
                         for f, i in sorted(ids.items(), key=itemgetter(1)))
        except KeyError:
            raise ArcanaUsageError(
                f"Unrecognised data frequencies in ID dict '{ids}' (valid "
                f"{', '.join(self.data_structure)})")

    def workflow(self, name, inputs, outputs, frequency, ids,
                 required_formats=None, produced_formats=None,
                 overwrite=False):
        """Generate a Pydra task that sources the specified inputs from the
        dataset

        Parameters
        ----------
        name : str
            A name for the workflow (must be globally unique)
        inputs : Sequence[DataSelector]
            The inputs to be sourced from the dataset
        outputs : Sequence[DataSpec]
            The outputs to be sinked into the dataset
        frequency : DataHierarchy
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
            ids = list(self.nodes(frequency))
            
        workflow = Workflow(name=name, input_spec=['id'],
                            inputs=[(ids,)]).split('ids')

        inputs_spec = {
            i: (DataItem if (frequency == i.frequency
                             or frequency.is_child(i.frequency))
                else ty.Sequence[DataItem])
            for i in inputs}

        self.selectors.update()

        for inpt_name, selector in inputs.items():
            if inpt_name in self.selectors:
                if overwrite:
                    logger.info(
                        f"Overwriting selector '{inpt_name}'")
                else:
                    raise ArcanaUsageError(
                        f"Attempting to overwriting '{inpt_name}' selector "
                        f"with output from workflow '{name}'. Use 'overwrite' "
                        "option if this is desired")
            self.selectors[inpt_name] = selector

        outputs_spec = {o: DataItem for o in outputs}

        for out_name, out_spec in outputs.items():
            if out_name in self.derivatives:
                if overwrite:
                    logger.info(
                        f"Overwriting derivative '{out_name}' with workflow "
                        f"'{name}'")
                else:
                    raise ArcanaUsageError(
                        f"Attempting to overwriting '{out_name}' derivative "
                        f"with workflow '{name}'. Use 'overwrite' option "
                        "if this is desired")
            self.derivatives[out_name] = out_spec
            self.workflows[out_name] = workflow

        @mark.task
        @mark.annotate(
            {'dataset': Dataset,
             'frequency': DataHierarchy,
             'id': str,
             'input_names': ty.Sequence[str],
             'return': inputs_spec})
        def retrieve(dataset, frequency, id, input_names):
            """Selects the items from the dataset corresponding to the input 
            selectors and retrieves them from the repository to a cache on 
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
        for inpt_name, required_format in required_formats.items():
            inpt_format = inputs[inpt_name].format
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
                identity,
                input_spec=SpecInfo(
                    name='SourceInputs', bases=(BaseSpec,),
                    fields=list(inputs_spec.items())),
                output_spec=SpecInfo(
                    name='SourceOutputs', bases=(BaseSpec,),
                    fields=list(inputs_spec.items())))(name='source',
                                                       **retrieved))

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
                         ('frequency', DataHierarchy),
                         ('id', str)
                         ('outputs', ty.Dict[str, DataSpec])]
                        + list(outputs_spec.items()))),
                output_spec=SpecInfo(
                    name='SinkOutputs', bases=(BaseSpec,), fields=[
                        ('data_node', DataNode)]))(
                            name='store',
                            dataset=self,
                            frequency=frequency,
                            id=workflow.lzin.id,
                            **sinked))

        workflow.set_output(('data_nodes', store.lzout.data_node))

        return workflow


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