from itertools import itemgetter
import logging
import typing as ty
from copy import copy
from collections import defaultdict
from itertools import chain
from collections import OrderedDict
from pydra import Workflow, mark
from .item import UnresolvedFileGroup, UnresolvedField, DataItem
from .frequency import DataFrequency
from .selector import DataSelector
from .file_format import FileFormat
from arcana2.exceptions import (
    ArcanaError, ArcanaInputError, ArcanaNameError, ArcanaDataTreeConstructionError,
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

    def __enter__(self):
        self.repository.__enter__()
        return self

    def __exit__(self):
        self.repository.__exit__()

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

    @property
    def file_selectors(self):
        return (s for s in self.selectors if s.is_file_group)

    @property
    def field_selectors(self):
        return (s for s in self.selectors if s.is_field)

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

    def nodes(self, frequency):
        return self.root_node.subnodes[frequency]

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

    def connect(self, workflow, frequency, inputs, outputs, skip_missing=False):
        """
        Connects a workflow to the dataset by prepending nodes to select,
        download and convert (if necessary) downloads/extracts data from the
        repository to be passed to a workflow, and convert (if necessary) and
        upload data back into the dataset.

        Parameters
        ----------
        workflow : pydra.Workflow
            The workflow to connect to the dataset
        frequency : DataFrequency
            The frequency of the output columns to generate within the dataset,
            i.e. are they to be present, per session, subject, timepoint or
            singular within the dataset.
        inputs : Dict[str, str or Tuple[str, FileFormat or dtype]]
            A mapping from the name the columns in the dataset to the inputs
            of the workflow. If the input needs to be converted before it
            can be passed to the workflow, the requiref file format can be
            passed with the workflow input name in a tuple
        outputs : Dict[str, Tuple[str, FileFormat or dtype]]
            A mapping from the name the outputs of the workflow to the name
            of the dataset column and file format to store it in.
        skip_missing : bool
            Whether to quietly skip data nodes which don't have matches for the
            requested input columns (otherwise raise an error)

        Returns
        -------
        pydra.task
            A Pydra task to that downloads/extracts the requested data
        """

        if invalid_inputs:= [i for i in inputs if i not in self.selectors]:
            raise ArcanaUsageError(
                f"Inputs '{'\', \''.join(invalid_inputs)}' are not present in "
                f"the dataset ('{'\', \''.join(self.selectors)}'")
        # Fill out inputs dictionary so that all inputs have a specified
        # file format
        for name, inpt in list(inputs.items()):
            if not isinstance(inpt, tuple):
                selector = self.selector[inpt]
                inputs[name] = (
                    inpt, (selector.format
                           if selector.is_file_group else selector.dtype))

        # We implement the source in multiple nodes of a nested workflow to
        # handle the selection, download and format conversions in separate
        # nodes
        outer_workflow = Workflow(name='source')

        @mark.task
        @mark.annotate(
            {'dataset': Dataset,
             'frequency': DataFrequency,
             'column_names': ty.Dict[str, ty.Union(FileFormat, type)],
             'skip_missing': bool,
             'return': {
                'items': ty.Sequence[ty.Dict[str, DataItem]]}})
        def select(dataset, frequency, input_names, skip_missing):
            items = []
            for node in dataset.nodes(frequency):
                node_items = []
                items.append(node_items)
                try:
                    for inpt in input_names:
                        node_items.append(dataset.selectors[inpt].match(node))
                except ArcanaInputError:
                    if not skip_missing:
                        raise                    
            return items
        
        outer_workflow.add(select(name='select',
                                  dataset=self,
                                  input_names=list(inputs),
                                  skip_missing=skip_missing,
                                  frequency=frequency).split('items'))

        # Generate return spec for `download` task including file-groups and
        # their auxiliary files
        return_spec = {}
        for col_name in column_names:
            selector = self.selectors[col_name]
            if selector.is_file_group:
                return_spec[col_name] = str  # The path to the primary file/dir
                col_format = selector.format
                for aux in col_format.aux_files:
                    return_spec[
                        col_format.aux_interface_name(col_name, aux)] = str
            else:
                return_spec[col_name] = selector.dtypeinptinpt

        @mark.task
        @mark.annotate(
            {'dataset': Dataset,
             'node_items': ty.Sequence[DataItem],
             'return': return_spec})
        def download(dataset, node_items):
            paths_and_values = []
            with dataset:
                for item in node_items:
                    item.get()
                    if item.is_file_group:
                        paths_and_values.append(item.local_path)
                        paths_and_values.extend(item.aux_files.values())
                    else:
                        paths_and_values.append(item.value)
            return tuple(paths_and_values)

        workflow.add(download(name='download',
                              node_items=workflow.select_items.lzout.items))

        # Do format conversions if required
        for col_name, required_format in zip(column_names, column_formats):
            selector = self.selectors[col_name]            
            current_format = self.selectors[col_name].format
            if required_format != current_format:
                # Get converter node
                converter = required_format.converter(current_format)
                converter_name = f"{col_name}_converter"
                # Map auxiliary files to converter interface
                aux_conns = {}
                for aux_name in current_format.aux_files:
                    aux_conns[aux_name] = getattr(
                        workflow.download.lzout,
                        current_format.aux_interface_name(col_name, aux_name))
                # Insert converter
                workflow.add(converter(
                    name=converter_name,
                    in_file=getattr(workflow.download.lzout, col_name)
                    **aux_conns))
                # Map converter output to workflow output
                converter_task = getattr(workflow, converter_name)
                workflow.set_output(
                    (col_name, converter_task.lzout.out_file))
                # Map auxiliary files to workflow output
                for aux_name in required_format.aux_files:
                    workflow.set_output((
                        col_name,
                        getattr(converter_task.lzout,
                                required_format.aux_interface_name(col_name,
                                                                   aux_name))))
            else:
                # Map download directly to output (i.e. without conversion)
                workflow.set_output(
                    (col_name, getattr(workflow.download.lzout, col_name)))

        return workflow

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
        self.supranodes = {}  # Refs to level (e.g. session -> subject)
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
                f"{name_path} (available '{'\', \''.join(self.file_groups)}'")
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
        node = self.supranodes[frequency]
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

