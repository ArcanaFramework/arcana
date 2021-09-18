from __future__ import annotations
import logging
import typing as ty
from itertools import chain
import re
from copy import copy
import attr
from attr.converters import default_if_none
from pydra import Workflow, mark
from pydra.engine.task import FunctionTask
from pydra.engine.specs import BaseSpec, SpecInfo
from arcana2.exceptions import (
    ArcanaNameError, ArcanaDataTreeConstructionError, ArcanaUsageError,
    ArcanaBadlyFormattedIDError, ArcanaWrongDataDimensionsError,
    ArcanaError)
from .item import DataItem
from .enum import DataDimension
from .spec import DataSink, DataSource
from .. import repository
from ..pipeline import Pipeline
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
    column_specs : Dict[str, DataSource or DataSink]
        The sources and sinks to be initially added to the dataset (columns are
        explicitly added when workflows are applied to the dataset).
    included : Dict[DataDimension, List[str]]
        The IDs to be included in the dataset per frequency. E.g. can be
        used to limit the subject IDs in a project to the sub-set that passed
        QC. If a frequency is omitted or its value is None, then all available
        will be used
    excluded : Dict[DataDimension, List[str]]
        The IDs to be excluded in the dataset per frequency. E.g. can be
        used to exclude specific subjects that failed QC. If a frequency is
        omitted or its value is None, then all available will be used
    workflows : Dict[str, pydra.Workflow]
        Workflows that have been applied to the dataset to generate sink
    """

    name: str = attr.ib()
    repository: repository.Repository = attr.ib()
    hierarchy: list[DataDimension] = attr.ib()
    id_inference: (dict[DataDimension, str] or ty.Callable) = attr.ib(
        factory=dict, converter=default_if_none(factory=dict))
    column_specs: dict[str, DataSource or DataSink] or None = attr.ib(
        factory=dict, converter=default_if_none(factory=dict), repr=False)
    included: dict[DataDimension, ty.List[str]] = attr.ib(
        factory=dict, converter=default_if_none(factory=dict), repr=False)
    excluded: dict[DataDimension, ty.List[str]] = attr.ib(
        factory=dict, converter=default_if_none(factory=dict), repr=False)
    workflows: dict[str, Workflow] = attr.ib(factory=dict, repr=False)
    _root_node: DataNode = attr.ib(default=None, init=False, repr=False)  

    @column_specs.validator
    def column_specs_validator(self, _, column_specs):
        if wrong_freq := [m for m in column_specs.values()
                          if not isinstance(m.frequency, self.dimensions)]:
            raise ArcanaUsageError(
                f"Data hierarchy of {wrong_freq} column specs do(es) not match"
                f" that of dataset {self.dimensions}")

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
            raise ArcanaWrongDataDimensionsError(
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
                + ', '.join(str(m) for m in (~covered).nonzero_basis()) +
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
    def root_freq(self):
        return self.dimensions(0)

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
            self._root_node = DataNode({self.root_freq: None}, self.root_freq,
                                       self)
            self.repository.construct_tree(self)
        return self._root_node

    def add_source(self, name, path, frequency, format, overwrite=False,
                   **kwargs):
        """Specify a data source in the dataset, which can then be referenced
        when connecting workflow inputs.

        Parameters
        ----------
        name : str
            The name used to reference the dataset "column" for the
            source
        path : str, default `name`
            The location of the source within the dataset
        frequency : [type]
            The frequency of the source within the dataset
        format : FileFormat or type
            The file-format (for file-groups) or datatype (for fields)
            that the source will be stored in within the dataset
        overwrite : bool
            Whether to overwrite existing columns
        **kwargs : dict[str, Any]
            Additional kwargs to pass to DataSource.__init__
        """
        frequency = self._parse_freq(frequency)
        self._add_spec(name, DataSource(path, format, frequency, **kwargs),
                       overwrite)

    def add_sink(self, name, path, frequency, format, overwrite=False,
                 **kwargs):
        """Specify a data source in the dataset, which can then be referenced
        when connecting workflow inputs.

        Parameters
        ----------
        name : str
            The name used to reference the dataset "column" for the
            sink
        path : str, default `name`
            The location of the sink within the dataset
        frequency : [type]
            The frequency of the sink within the dataset
        format : FileFormat or type
            The file-format (for file-groups) or datatype (for fields)
            that the sink will be stored in within the dataset
        """
        frequency = self._parse_freq(frequency)
        self._add_spec(name, DataSink(path, format, frequency, **kwargs),
                       overwrite)

    def _add_spec(self, name, spec, overwrite):
        if name in self.column_specs:
            if overwrite:
                logger.info(
                    f"Overwriting {self.column_specs[name]} with {spec} in "
                    f"{self}")
            else:
                raise ArcanaNameError(
                    name,
                    f"Name clash attempting to add {spec} to {self} "
                    f"with {self.column_specs[name]}. Use 'overwrite' option "
                    "if this is desired")
        self.column_specs[name] = spec

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
        if not frequency:
            if id is not None:
                raise ArcanaUsageError(
                    f"Root nodes don't have any IDs ({id})")
            return self.root_node
        frequency = self._parse_freq(frequency)
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
                    f"({list(self.node_ids(frequency))})")

    def nodes(self, frequency=None, ids=None):
        """Return all the IDs in the dataset for a given frequency

        Parameters
        ----------
        frequency : DataDimension or None
            The "frequency" of the nodes, e.g. per-session, per-subject. If
            None then all nodes are returned
        ids : Sequence[str or Tuple[str]]
            The i

        Returns
        -------
        Sequence[DataNode]
            The sequence of the data node within the dataset
        """
        if frequency is None:
            return chain(
                *(d.values() for d in self.root_node.children.values()))
        frequency = self._parse_freq(frequency)
        if frequency == self.root_freq:
            return [self.root_node]
        nodes = self.root_node.children[frequency].values()
        if ids is not None:
            nodes = (n for n in nodes if n in set(ids))
        return nodes
        
    def node_ids(self, frequency):
        """Return all the IDs in the dataset for a given frequency

        Parameters
        ----------
        frequency : DataDimension
            The "frequency" of the nodes, e.g. per-session, per-subject...

        Returns
        -------
        Sequence[str]
            The IDs of the nodes
        """
        frequency = self._parse_freq(frequency)
        if frequency == self.root_freq:
            return [None]
        return self.root_node.children[frequency].keys()

    def column(self, name):
        """Return all data items across the dataset for a given source or sink

        Parameters
        ----------
        name : str
            Name of the source/sink to select

        Returns
        -------
        Sequence[DataItem]
            All data items in the column
        """
        spec = self.column_specs[name]
        return (n[name] for n in self.nodes(spec.frequency))

    def columns(self, *names):
        """Iterate over all columns in the dataset

        Returns
        -------
        Sequence[List[DataItem]]
            All columns in the dataset
        """
        if not names:
            names = self.column_specs
        return (list(self.column(n)) for n in names)

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
                    ids[layer.nonzero_basis()[-1]] = label
            else:
                match = re.match(regex, label)
                if match is None:
                    raise ArcanaBadlyFormattedIDError(
                        f"{layer} label '{label}', does not match ID inference"
                        f" pattern '{regex}'")
                new_freqs = layer - (layer & frequency)
                for target_freq, target_id in match.groupdict().items():
                    target_freq = self.dimensions[target_freq]
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
        for freq in (set(self.dimensions) - set(frequency.nonzero_basis())):
            if ids[freq] is None:
                id = tuple(ids[b] for b in freq.nonzero_basis() if ids[b] is not None)
                if id:
                    if len(id) == 1:
                        id = id[0]
                    ids[freq] = id
        self.add_node(ids, frequency)

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
        logger.info(f'Adding new {str(frequency)} node to {self.name} dataset: '
                    + ', '.join(f'{str(f)}={i}' for f, i in ids.items()))
        frequency = self._parse_freq(frequency)
        node = DataNode(ids, frequency, self)
        # Create new data node
        node_dict = self.root_node.children[node.frequency]
        if node.id in node_dict:
            raise ArcanaDataTreeConstructionError(
                f"ID clash ({node.id}) between nodes inserted into data "
                "tree")
        node_dict[node.id] = node
        # Insert root node
        # Insert parent nodes if not already present and link them with
        # inserted node
        for parent_freq, parent_id in node.ids.items():
            diff_freq = node.frequency - (parent_freq & node.frequency)
            if diff_freq and parent_freq:  # Don't need to insert root node again
                logger.debug(f'Linking parent {parent_freq}: {parent_id}')
                try:
                    parent_node = self.node(parent_freq, parent_id)
                except ArcanaNameError:
                    logger.debug(
                        f'Parent {parent_freq}:{parent_id} not found, adding')
                    parent_ids = {f: i for f, i in node.ids.items()
                                  if (f.is_parent(parent_freq)
                                      or f == parent_freq)}
                    parent_node = self.add_node(parent_ids, parent_freq)
                # Set reference to level node in new node
                diff_id = node.ids[diff_freq]
                children_dict = parent_node.children[frequency]
                if diff_id in children_dict:
                    raise ArcanaDataTreeConstructionError(
                        f"ID clash ({diff_id}) between nodes inserted into "
                        f"data tree in {diff_freq} children of {parent_node} "
                        f"({children_dict[diff_id]} and {node})")
                children_dict[diff_id] = node
        return node

    def new_pipeline(self, name, inputs=None, outputs=None, frequency=None,
                     **kwargs):
        """Generate a Pydra task that sources the specified inputs from the
        dataset

        Parameters
        ----------
        name : str
            A name for the workflow (must be globally unique)
        workflow : pydra.Workflow
            The Pydra workflow to add to the repository
        inputs : Dict[str, str]
            A dictionary that maps the workflow inputs (keys) to a column spec
            in the dataset (can be either a source or a sink)
        formats : Dict[str, FileFormat]
            The required and produced data formats for the inputs and outputs
            of the workflow, respectively. Inputs in the same format as they
            are stored in the repository can be omitted.
        frequency : DataDimension or None
            The frequency of the nodes to draw the inputs from. Defaults to the
            the lowest level of the tree (i.e. max(self.dimensions))
        outputs : Dict[str, DataSink]
            Explicitly set outputs of the workflow to a specific path in the
            dataset. Otherwise they will be stored at WORKFLOW_NAME/OUTPUT_NAME
        overwrite : bool
            Whether to overwrite existing sinks in the dataset
        """
        frequency = self._parse_freq(frequency)
        return Pipeline.factory()
        

    def derive(self, *names, ids=None):
        """Generate derivatives from the workflows

        Parameters
        ----------
        *names : Sequence[str]
            Names of the columns corresponding to the items to derive
        ids : Sequence[str]
            The IDs of the data nodes in each column to derive

        Returns
        -------
        Sequence[List[DataItem]]
            The derived columns
        """
        # TODO: Should construct full stack of required workflows
        for workflow in set(self.column_spec[n].workflow for n in names):
            workflow(ids=ids)
        return self.columns(*names)

    def _parse_freq(self, freq):
        """Parses the data frequency, converting from string if necessary and
        checks it matches the dimensions of the dataset"""
        try:
            if isinstance(freq, str):
                freq = self.dimensions[freq]
            elif not isinstance(freq, self.dimensions):
                raise KeyError
        except KeyError:
            raise ArcanaWrongDataDimensionsError(
                f"{freq} is not a valid dimension for {self} "
                f"({self.dimensions})")
        return freq

    @classmethod
    def _sink_path(cls, workflow_name, sink_name):
        return f'{workflow_name}/{sink_name}'


@attr.s
class SplitDataset():
    """A dataset created by combining multiple datasets into a conglomerate

    Parameters
    ----------
    """

    source_dataset: Dataset = attr.ib()
    sink_dataset: Dataset = attr.ib()
