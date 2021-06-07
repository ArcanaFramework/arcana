import weakref
from itertools import itemgetter
import logging
from copy import copy
from collections import defaultdict
from itertools import chain
from collections import OrderedDict
from pydra import mark, Workflow
from .item import UnresolvedFileGroup, UnresolvedField
from arcana2.exceptions import (
    ArcanaError, ArcanaNameError, ArcanaDataTreeConstructionError,
    ArcanaUsageError)

logger = logging.getLogger('arcana')


class Dataset():
    """
    A representation of a "dataset", the complete collection of data
    (file-sets and fields) to be used in an analysis.

    Parameters
    ----------
    name : str
        The name-name_path that uniquely identifies the datset within the
        repository it is stored
    repository : Repository
        The repository the dataset is stored into. Can be the local file
        system by providing a FileSystemDir repo.
    selectors : Dict[str, Selector]
        A dictionary that maps the name-paths of "columns" in the dataset
        to criteria in a Selector object that select the corresponding
        items in the dataset
    include_ids : Dict[str, List[str]]
        The IDs to be included in the dataset for each frequency. E.g. can be
        used to limit the subject IDs in a project to the sub-set that passed
        QC. If a frequency is omitted or its value is None, then all available
        will be used
    **populate_kwargs : Dict[str, Any]
        Keyword arguments passed on to the `populate_tree` method of the
        repository class when it is called
    """

    def __init__(self, name, repository, selectors, include_ids=None,
                 **populate_kwargs):
        self.name = name
        self.repository = repository
        if wrong_freq:= [m for m in selectors
                         if not isinstance(m.frequency, self.frequency_enum)]:
            raise ArcanaUsageError(
                f"Data frequencies of {wrong_freq} selectors does not match "
                f"that of repository {self.frequency_enum}")
        self.selectors = selectors
        self._columns = {}  # Populated on demand from selector objects
        self.include_ids = {f: None for f in self.frequency_enum}
        for freq, ids in include_ids:
            try:
                self.include_ids[self.frequency_enum[freq]] = list(ids)
            except KeyError:
                raise ArcanaUsageError(
                    f"Unrecognised data frequency '{freq}' (valid "
                    f"{', '.join(self.frequency_enum)})")
        self._root_node = None  # Lazy loading of data tree info from repo
        self._populate_kwargs = populate_kwargs

    def __repr__(self):
        return (f"Dataset(name='{self.name}', repository={self.repository}, "
                f"include_ids={self.include_ids})")

    def __eq__(self, other):
        return (self.name == other.name
                and self.repository == other.repository
                and self.include_ids == other.include_ids
                and self.root_node == other.root_node
                and self.frequency_enum == other.frequency_enum)

    def __hash__(self):
        return (hash(self.name)
                ^ hash(self.repository)
                ^ hash(self.include_ids)
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

    def __ne__(self, other):
        return not (self == other)

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
        node._supranodes[self.frequency_enum(0)] = weakref.ref(self.root_node)
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
            node.__supranodes[supra_freq] = weakref.ref(supranode)
            supranode.subnodes[frequency][sub_ids] = node
        return node

    def _ids_tuple(self, ids):
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
    def frequency_enum(self):
        return self.repository.frequency_enum

    @property
    def root_frequency(self):
        return self.frequency_enum(0)

    def source(frequency, inputs):
        """
        Returns a Pydra task that downloads/extracts data from the
        repository to be passed to a workflow

        Parameters
        ----------
        frequency : DataFrequency
            The frequency of the outputs to sink, i.e. which level in the
            data tree the outputs are to be placed
        inputs : Sequence[str or Tuple[str, FileFormat or dtype]]
            The name of the columns to source from the dataset. If the
            file format or data type needs to be converted from what it is
            in the dataset a Tuple[str, FileFormat or dtype] can be provided
            instead of the name, consisting of the name and the desired
            file-format/datatype.

        Returns
        -------
        pydra.task
            A Pydra task to that downloads/extracts the requested data
        """
        workflow = Workflow(name='source')
        
        
        raise NotImplemented
        # Protect against iterators
        columns = {
            n: self.column(m) for n, v in inputs.items()}
        # Check for consistent frequencies in columns
        frequencies = set(c.tree_level for c in columns)
        if len(frequencies) > 1:
            raise ArcanaError(
                "Attempting to sink multiple frequencies in {}"
                .format(', '.join(str(c) for c in columns)))
        elif frequencies:
            # NB: Exclude very rare case where pipeline doesn't have inputs,
            #     would only really happen in unittests
            self._tree_level = next(iter(frequencies))
        # Extract set of datasets used to source/sink from/to
        self.datasets = set(chain(*(
            (i.dataset for i in c if i.dataset is not None)
            for c in columns)))
        self.repositories = set(d.repository for d in self.datasets)
        # Segregate into file_group and field columns
        self.file_group_columns = [c for c in columns if c.is_file_group]
        self.field_columns = [c for c in columns if c.is_field]

          # Directory that holds session-specific
        outputs = self.output_spec().get()
        subject_id = (self.inputs.subject_id
                      if isdefined(self.inputs.subject_id) else None)
        timepoint_id = (self.inputs.timepoint_id
                    if isdefined(self.inputs.timepoint_id) else None)
        outputs['subject_id'] = self.inputs.subject_id
        outputs['timepoint_id'] = self.inputs.timepoint_id
        # Source file_groups
        with ExitStack() as stack:
            # Connect to set of repositories that the columns come from
            for repository in self.repositories:
                stack.enter_context(repository)
            for file_group_column in self.file_group_columns:
                file_group = file_group_column.item(subject_id, timepoint_id)
                file_group.get()
                outputs[file_group_column.name + PATH_SUFFIX] = file_group.path
                outputs[file_group_column.name
                        + CHECKSUM_SUFFIX] = file_group.checksums
            for field_column in self.field_columns:
                field = field_column.item(subject_id, timepoint_id)
                field.get()
                outputs[field_column.name + FIELD_SUFFIX] = field.value
        return outputs


    def sink(frequency, outputs):
        """
        Returns a Pydra task that uploads/moves data to the repository

        Parameters
        ----------
        frequency : DataFrequency
            The frequency of the outputs to sink, i.e. which level in the
            data tree the outputs are to be placed
        outputs : Sequence[Tuple[str, FileFormat or dtype]]
            A list of tuples specifying the output name path and the format/
            dtype the output will be expected/saved in

        Returns
        -------
        pydra.task
            A Pydra task to that uploads/moves data into 
        """
        raise NotImplemented
        super(RepositorySink, self).__init__(columns)
        # Add traits for file_groups to sink
        for file_group_column in self.file_group_columns:
            self._add_trait(self.inputs,
                            file_group_column.name + PATH_SUFFIX,
                            PATH_TRAIT)
        # Add traits for fields to sink
        for field_column in self.field_columns:
            self._add_trait(self.inputs,
                            field_column.name + FIELD_SUFFIX,
                            self.field_trait(field_column))
        # Add traits for checksums/values of pipeline inputs
        self._pipeline_input_file_groups = []
        self._pipeline_input_fields = []
        for inpt in pipeline.inputs:
            if inpt.is_file_group:
                trait_t = JOINED_CHECKSUM_TRAIT
            else:
                trait_t = self.field_trait(inpt)
                trait_t = traits.Either(trait_t, traits.List(trait_t),
                                        traits.List(traits.List(trait_t)))
            self._add_trait(self.inputs, inpt.checksum_suffixed_name, trait_t)
            if inpt.is_file_group:
                self._pipeline_input_file_groups.append(inpt.name)
            elif inpt.is_field:
                self._pipeline_input_fields.append(inpt.name)
            else:
                assert False
        self._prov = pipeline.prov
        self._pipeline_name = pipeline.name
        self._namespace = pipeline.analysis.name
        self._required = required
        outputs = self.output_spec().get()
        # Connect iterables (i.e. subject_id and timepoint_id)
        subject_id = (self.inputs.subject_id
                      if isdefined(self.inputs.subject_id) else None)
        timepoint_id = (self.inputs.timepoint_id
                    if isdefined(self.inputs.timepoint_id) else None)
        missing_inputs = []
        # Collate input checksums into a dictionary
        input_checksums = {n: getattr(self.inputs, n + CHECKSUM_SUFFIX)
                           for n in self._pipeline_input_file_groups}
        input_checksums.update({n: getattr(self.inputs, n + FIELD_SUFFIX)
                                for n in self._pipeline_input_fields})
        output_checksums = {}
        with ExitStack() as stack:
            # Connect to set of repositories that the columns come from
            for repository in self.repositories:
                stack.enter_context(repository)
            for file_group_column in self.file_group_columns:
                file_group = file_group_column.item(subject_id, timepoint_id)
                path = getattr(self.inputs, file_group_column.name + PATH_SUFFIX)
                if not isdefined(path):
                    if file_group.name in self._required:
                        missing_inputs.append(file_group.name)
                    continue  # skip the upload for this file_group
                file_group.path = path  # Push to repository
                output_checksums[file_group.name] = file_group.checksums
            for field_column in self.field_columns:
                field = field_column.item(
                    subject_id,
                    timepoint_id)
                value = getattr(self.inputs,
                                field_column.name + FIELD_SUFFIX)
                if not isdefined(value):
                    if field.name in self._required:
                        missing_inputs.append(field.name)
                    continue  # skip the upload for this field
                field.value = value  # Push to repository
                output_checksums[field.name] = field.value
            # Add input and output checksums to provenance provenance and sink to
            # all repositories that have received data (typically only one)
            prov = copy(self._prov)
            prov['inputs'] = input_checksums
            prov['outputs'] = output_checksums
            provenance = Provenance(self._pipeline_name, self.tree_level, subject_id,
                            timepoint_id, self._namespace, prov)
            for dataset in self.datasets:
                dataset.put_provenance(provenance)
        if missing_inputs:
            raise ArcanaDesignError(
                "Required derivatives '{}' to were not created by upstream "
                "nodes connected to sink {}".format(
                    "', '".join(missing_inputs), self))
        # Return cache file paths
        outputs['checksums'] = output_checksums
        return outputs


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
        self._supranodes = {}  # Refs to level (e.g. session -> subject)
        self._dataset = weakref.ref(dataset)
        

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

    def file_group(self, name_path: str, file_format=None):
        """
        Gets the file_group with the ID 'id' produced by the Analysis named
        'analysis' if provided. If a spec is passed instead of a str to the
        name argument, then the analysis will be set from the spec iff it is
        derived

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
                (f"{self} doesn't have a file_group at the name_path {name_path} "
                 "(available '{}')".format("', '".join(self.file_groups))))
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

    def supranode(self, frequency):
        node = self.__supranodes[frequency]()
        if node is None:
            raise ArcanaError(
                f"Node referenced by {self} for {frequency} no longer exists")
        return node

    @property
    def file_groups(self):
        return self._file_groups.values()
    
    @property
    def fields(self):
        return self._fields.values()

    @property
    def items(self):
        return chain(self.file_groups, self.fields)

    @property
    def dataset(self):
        dataset = self._dataset()
        if dataset is None:
            raise ArcanaError(
                "Dataset referenced by data node no longer exists")
        return dataset

    def __ne__(self, other):
        return not (self == other)

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


class MultiDataset(Dataset):
    """A dataset created by combining multiple datasets into a conglomerate

    Parameters
    ----------
    """

    def __init__(self):
        raise NotImplemented
