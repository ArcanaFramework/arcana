from abc import ABCMeta, abstractmethod
import logging
from nipype.interfaces.base import (
    traits, DynamicTraitedSpec, Undefined, File, Directory,
    BaseInterface, isdefined)
from itertools import chain
from typing import Sequence
from copy import copy
from arcana2.utils import ExitStack, PATH_SUFFIX, FIELD_SUFFIX, CHECKSUM_SUFFIX
from arcana2.exceptions import ArcanaError, ArcanaDesignError
from ..provenance import Record
from ..set import Dataset

import logging


logger = logging.getLogger('arcana')


class Repository(metaclass=ABCMeta):
    """
    Abstract base class for all Repository systems, DaRIS, XNAT and
    local file system. Sets out the interface that all Repository
    classes should implement.
    """

    def __init__(self):
        self._connection_depth = 0

    def __enter__(self):
        # This allows the repository to be used within nested contexts
        # but still only use one connection. This is useful for calling
        # methods that need connections, and therefore control their
        # own connection, in batches using the same connection by
        # placing the batch calls within an outer context.
        if self._connection_depth == 0:
            self.connect()
        self._connection_depth += 1
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._connection_depth -= 1
        if self._connection_depth == 0:
            self.disconnect()

    def standardise_name(self, name):
        return name

    def connect(self):
        """
        If a connection session is required to the repository,
        manage it here
        """

    def disconnect(self):
        """
        If a connection session is required to the repository,
        manage it here
        """

    def dataset(self, name, **kwargs):
        """
        Returns a dataset from the XNAT repository

        Parameters
        ----------
        name : str
            The name, path or ID of the dataset within the repository
        subject_ids : list[str]
            The list of subjects to include in the dataset
        timepoint_ids : list[str]
            The list of timepoints to include in the dataset
        """
        return Dataset(name, repository=self, **kwargs)

    @abstractmethod
    def find_data(self, dataset, subject_ids=None, timepoint_ids=None, **kwargs):
        """
        Find all data within a repository, registering file_groups, fields and
        provenance with the found_file_group, found_field and found_provenance
        methods, respectively

        Parameters
        ----------
        dataset : Dataset
            The dataset to return the data for
        subject_ids : list(str)
            List of subject IDs with which to filter the tree with. If
            None all are returned
        timepoint_ids : list(str)
            List of timepoint IDs with which to filter the tree with. If
            None all are returned

        Returns
        -------
        file_groups : list[FileGroup]
            All the file_groups found in the repository
        fields : list[Field]
            All the fields found in the repository
        records : list[Record]
            The provenance records found in the repository
        """

    @abstractmethod
    def get_file_group(self, file_group):
        """
        Cache the file_group locally if required

        Parameters
        ----------
        file_group : FileGroup
            The file_group to cache locally

        Returns
        -------
        path : str
            The file-system path to the cached file
        """

    @abstractmethod
    def get_field(self, field):
        """
        Extract the value of the field from the repository

        Parameters
        ----------
        field : Field
            The field to retrieve the value for

        Returns
        -------
        value : int | float | str | list[int] | list[float] | list[str]
            The value of the Field
        """

    def get_checksums(self, file_group):
        """
        Returns the checksums for the files in the file_group that are stored in
        the repository. If no checksums are stored in the repository then this
        method should be left to return None and the checksums will be
        calculated by downloading the files and taking calculating the digests

        Parameters
        ----------
        file_group : FileGroup
            The file_group to return the checksums for

        Returns
        -------
        checksums : dct[str, str]
            A dictionary with keys corresponding to the relative paths of all
            files in the file_group from the base path and values equal to the MD5
            hex digest. The primary file in the file-set (i.e. the one that the
            path points to) should be specified by '.'.
        """

    @abstractmethod
    def put_file_group(self, file_group):
        """
        Inserts or updates the file_group into the repository

        Parameters
        ----------
        file_group : FileGroup
            The file_group to insert into the repository
        """

    @abstractmethod
    def put_field(self, field):
        """
        Inserts or updates the fields into the repository

        Parameters
        ----------
        field : Field
            The field to insert into the repository
        """

    @abstractmethod
    def put_record(self, record, dataset):
        """
        Inserts a provenance record into a session or subject|timepoint|analysis
        summary

        Parameters
        ----------
        record : prov.Record
            The record to insert into the repository
        dataset : Dataset
            The dataset to put the record into
        """

    def source(dataset_name, data_columns, tree_level):
        """
        Returns a Pydra task that downloads/extracts data from the
        repository to be passed to a workflow

        Parameters
        ----------
        dataset_name : str
            The name of the dataset within the repository (e.g. project name)
        data_columns : DataColumn
            A sequence of Matcher of Column objects that specify which data
            to pull from the repository
        tree_level : str, optional
            The tree_level of the data to extract (i.e. per_session,
            per_subject, etc...), by default 'per_session'

        Returns
        -------
        pydra.task
            A Pydra task to that downloads/extracts the requested data
        """
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


    def sink(dataset_name, outputs, tree_level='per_session'):
        """
        Returns a Pydra task that uploads/moves data to the
        repository to be passed to a workflow

        Parameters
        ----------
        dataset_name : str
            The name of the dataset within the repository (e.g. project name)
        outputs : Spec
            A sequence Spec objects that specify where to put the data
            in the repository
        tree_level : str, optional
            The tree_level of the data to put (i.e. per_session,
            per_subject, etc...), by default 'per_session'

        Returns
        -------
        pydra.task
            A Pydra task to that downloads/extracts the requested data
        """
        raise NotImplementedError
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
            # Add input and output checksums to provenance record and sink to
            # all repositories that have received data (typically only one)
            prov = copy(self._prov)
            prov['inputs'] = input_checksums
            prov['outputs'] = output_checksums
            record = Record(self._pipeline_name, self.tree_level, subject_id,
                            timepoint_id, self._namespace, prov)
            for dataset in self.datasets:
                dataset.put_record(record)
        if missing_inputs:
            raise ArcanaDesignError(
                "Required derivatives '{}' to were not created by upstream "
                "nodes connected to sink {}".format(
                    "', '".join(missing_inputs), self))
        # Return cache file paths
        outputs['checksums'] = output_checksums
        return outputs


PATH_TRAIT = traits.Either(File(exists=True), Directory(exists=True))
FIELD_TRAIT = traits.Either(traits.Int, traits.Float, traits.Str,
                            traits.List(traits.Int), traits.List(traits.Float),
                            traits.List(traits.Str))
CHECKSUM_TRAIT = traits.Dict(traits.Str(), traits.Str())
# Trait for checksums that may be joined over iterators
JOINED_CHECKSUM_TRAIT = traits.Either(
    CHECKSUM_TRAIT, traits.List(CHECKSUM_TRAIT),
    traits.List(traits.List(CHECKSUM_TRAIT)))


class RepositoryInterface(BaseInterface):
    """
    Parameters
    ----------
    columns : FileGroupColumn | FieldColumn
        The file-group and field columns to add extract from the repository
    """

    def __init__(self, columns):
        super(RepositoryInterface, self).__init__()
        # Protect against iterators
        columns = list(columns)
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

    def __eq__(self, other):
        try:
            return (
                self.file_group_columns == other.file_group_columns
                and self.field_columns == other.field_columns)
        except AttributeError:
            return False

    def __repr__(self):
        return "{}(file_groups={}, fields={})".format(
            type(self).__name__, self.file_group_columns,
            self.field_columns)

    def __ne__(self, other):
        return not self == other

    def _run_interface(self, runtime, *args, **kwargs):
        return runtime

    @property
    def columns(self):
        return chain(self.file_group_columns, self.field_columns)

    @property
    def tree_level(self):
        return self._tree_level

    @classmethod
    def _add_trait(cls, spec, name, trait_type):
        spec.add_trait(name, trait_type)
        spec.trait_set(trait_change_notify=False, **{name: Undefined})
        # Access the trait (not sure why but this is done in add_traits
        # so I have also done it here
        getattr(spec, name)

    @classmethod
    def field_trait(cls, field):
        if field.array:
            trait = traits.List(field.dtype)
        else:
            trait = field.dtype
        return trait


class RepositorySpec(DynamicTraitedSpec):
    """
    Base class for input and output specifications for repository source
    and sink interfaces
    """
    subject_id = traits.Str(desc="The subject ID")
    timepoint_id = traits.Str(desc="The timepoint ID")


class RepositorySourceInputSpec(RepositorySpec):
    """
    Input specification for repository source interfaces.
    """
    prereqs = traits.List(
        desc=("A list of lists of iterator IDs used in prerequisite pipelines."
              " Only passed here to ensure that prerequisites are processed "
              "before this source is run (so that their outputs exist in the "
              "repository)"))


class RepositorySource(RepositoryInterface):
    """
    Parameters
    ----------
    file_groups: list
        List of all file_groups to be extracted from the repository
    fields: list
        List of all the fields that are to be extracted from the repository
    """

    input_spec = RepositorySourceInputSpec
    output_spec = RepositorySpec
    _always_run = True

    def _outputs(self):
        outputs = super(RepositorySource, self)._outputs()
        # Add traits for file_groups to source and their checksums
        for file_group_column in self.file_group_columns:
            self._add_trait(outputs,
                            file_group_column.name + PATH_SUFFIX, PATH_TRAIT)
            self._add_trait(outputs,
                            file_group_column.name + CHECKSUM_SUFFIX,
                            CHECKSUM_TRAIT)
        # Add traits for fields to source
        for field_column in self.field_columns:
            self._add_trait(outputs,
                            field_column.name + FIELD_SUFFIX,
                            self.field_trait(field_column))
        return outputs

    def _list_outputs(self):
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


class RepositorySinkOutputSpec(DynamicTraitedSpec):

    checksums = traits.Either(
        traits.Dict, FIELD_TRAIT,
        desc=("Provenance information sinked with files and fields. Note that"
              "at this stage it is only used as something to connect to the "
              "\"deiterators\" and eventually the \"final\" node after the "
              "pipeline outputs have been sunk"))


class RepositorySink(RepositoryInterface):
    """
    Interface used to sink derivatives into the output repository

    Parameters
    ----------
    columns : *Slice
        The columns of Field and FileGroup objects to insert into the
        outputs repositor(y|ies)
    pipeline : arcana2.pipeline.Pipeline
        The pipeline that has produced the outputs to sink
    required : list[str]
        Names of derivatives that are required by downstream nodes. Any
        undefined required derivatives that are undefined will raise an error.
    """

    input_spec = RepositorySpec
    output_spec = RepositorySinkOutputSpec

    def __init__(self, columns, pipeline, required=()):
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

    def _list_outputs(self):
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
            # Add input and output checksums to provenance record and sink to
            # all repositories that have received data (typically only one)
            prov = copy(self._prov)
            prov['inputs'] = input_checksums
            prov['outputs'] = output_checksums
            record = Record(self._pipeline_name, self.tree_level, subject_id,
                            timepoint_id, self._namespace, prov)
            for dataset in self.datasets:
                dataset.put_record(record)
        if missing_inputs:
            raise ArcanaDesignError(
                "Required derivatives '{}' to were not created by upstream "
                "nodes connected to sink {}".format(
                    "', '".join(missing_inputs), self))
        # Return cache file paths
        outputs['checksums'] = output_checksums
        return outputs
