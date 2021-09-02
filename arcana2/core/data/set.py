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
from arcana2.core.utils import split_extension
from .item import DataItem
from .enum import DataFrequency
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
    included : Dict[DataFrequency, List[str]]
        The IDs to be included in the dataset for each frequency. E.g. can be
        used to limit the subject IDs in a project to the sub-set that passed
        QC. If a frequency is omitted or its value is None, then all available
        will be used
    excluded : Dict[DataFrequency, List[str]]
        The IDs to be excluded in the dataset for each frequency. E.g. can be
        used to exclude specific subjects that failed QC. If a frequency is
        omitted or its value is None, then all available will be used
    data_structure : EnumMeta
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

    name: str = attr.ib()
    repository: repository.Repository = attr.ib()
    data_structure: EnumMeta  = attr.ib()
    selectors: ty.Sequence[DataSelector] = attr.ib(factory=list)
    derivatives: ty.Sequence[DataSpec] = attr.ib(factory=list)
    included: ty.Dict[DataFrequency, ty.List[str]] = attr.ib(factory=dict)
    excluded: ty.Dict[DataFrequency, ty.List[str]] = attr.ib(factory=dict)
    id_inference: (ty.Sequence[ty.Tuple(DataFrequency, str)]
                   or ty.Callable) = attr.ib(factory=list, converter=list)
    populate_kwargs: ty.Dict[str, ty.Any] = attr.ib(factory=dict)
    _root_node: DataNode = attr.ib(default=None, init=False)


    @selectors.validator
    def selectors_validator(self, _, selectors):
        if wrong_freq := [m for m in selectors.values()
                          if not isinstance(m.frequency, self.data_structure)]:
            raise ArcanaUsageError(
                f"Data frequencies of {wrong_freq} selectors does not match "
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
                and self.excluded == other.excluded
                and self.root_node == other.root_node
                and self.data_structure == other.data_structure)

    def __hash__(self):
        return (hash(self.name)
                ^ hash(self.repository)
                ^ hash(self.included)
                ^ hash(self.excluded)
                ^ hash(self.root_node)
                ^ hash(self.data_structure))

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
            self._root_node = DataNode(self.root_frequency, {}, self)
            self.repository.populate_tree(self, **self._populate_kwargs)
        return self._root_node

    def __ne__(self, other):
        return not (self == other)

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
        ids.update = {self.data_structure(f): i for f, i in id_kwargs.items()}
        # Parse str to frequency enums
        frequency = self.data_structure[str(frequency)]
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
        if not isinstance(frequency, self.data_structure):
            raise ArcanaDataTreeConstructionError(
                f"Provided frequency {frequency} is not of "
                f"{self.data_structure} type")
        # Check conversion to frequency cls
        ids = {self.data_structure[str(f)]: i for f, i in ids.items()}
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
        node.supranodes[self.data_structure(0)] = self.root_node
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
            return tuple((self.data_structure[str(f)], i)
                         for f, i in sorted(ids.items(), key=itemgetter(1)))
        except KeyError:
            raise ArcanaUsageError(
                f"Unrecognised data frequencies in ID dict '{ids}' (valid "
                f"{', '.join(self.data_structure)})")

    @property
    def root_frequency(self):
        return self.data_structure(0)

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
             'frequency': DataFrequency,
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
                         ('frequency', DataFrequency),
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
                ids[self.data_structure[target]] = id
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
