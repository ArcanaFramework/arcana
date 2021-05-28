import os
import os.path as op
import weakref
from itertools import chain
import hashlib
from collections.abc import Iterable
from arcana2.utils import split_extension, parse_value
from arcana2.exceptions import (
    ArcanaError, ArcanaFileFormatError, ArcanaUsageError, ArcanaNameError,
    ArcanaDataNotDerivedYetError, ArcanaUriAlreadySetException,
    ArcanaNoMatchingFileFormatError)
# from .file_format import FileFormat
from .base import FileGroupMixin, FieldMixin, DataMixin
from arcana2.enum import DataFreq

HASH_CHUNK_SIZE = 2 ** 20  # 1MB


class DataItem():

    is_spec = False

    def __init__(self, data_node, exists, record):
        self._data_node = (
            weakref.ref(data_node) if data_node is not None else None)
        self.exists = exists
        self._record = record

    def __eq__(self, other):
        return (self.data_node == other.data_node
                and self.exists == other.exists
                and self._record == other._record)

    def __hash__(self):
        return (hash(self.data_node)
                ^ hash(self.exists)
                ^ hash(self._record))

    def find_mismatch(self, other, indent=''):
        sub_indent = indent + '  '
        mismatch = ''
        if self.data_node != other.data_node:
            mismatch += ('\n{}data_node: self={} v other={}'
                         .format(sub_indent, self.data_node,
                                 other.data_node))
        if self.exists != other.exists:
            mismatch += ('\n{}exists: self={} v other={}'
                         .format(sub_indent, self.exists,
                                 other.exists))
        if self._record != other._record:
            mismatch += ('\n{}_record: self={} v other={}'
                         .format(sub_indent, self._record,
                                 other._record))
        return mismatch

    # @property
    # def derived(self):
    #     return self.namespace is not None

    @property
    def record(self):
        return self._record

    @property
    def data_node(self):
        if self.data_node is None:
            data_node = None
        else:
            data_node = self.data_node()
            if data_node is None:
                raise ArcanaError("Referenced data_node no longer exists")
        return data_node
        

    @record.setter
    def record(self, record):
        if self.path not in record.outputs:
            raise ArcanaNameError(
                self.path,
                "{} was not found in outputs {} of provenance record {}"
                .format(self.path, record.outputs.keys(), record))
        self._record = record

    @property
    def recorded_checksums(self):
        if self.record is None:
            return None
        else:
            return self._record.outputs[self.path]

    def initkwargs(self):
        dct = super().initkwargs()
        dct['data_node'] = self.data_node
        dct['exists'] = self.exists
        return dct


class FileGroup(DataItem, FileGroupMixin):
    """
    A representation of a file_group within the dataset.

    Parameters
    ----------
    path : str
        The path to the relative location of the file group, i.e. excluding
        information about which node in the data tree it belongs to
    format : FileFormat
        The file format used to store the file_group.
    aux_files : dict[str, str] | None
        Additional files in the file_group. Keys should match corresponding
        aux_files dictionary in format.
    order : int | None
        The order in which the file-group appears in the node it belongs to.
        Typically corresponds to the acquisition order for scans within an
        imaging session. Can be used to distinguish between scans with the
        same series description (e.g. multiple BOLD or T1w scans) in the same
        imaging sessions.
    data_node : DataNode
        The data node within a dataset that the file-group belongs to
    exists : bool
        Whether the file_group exists or is just a placeholder for a derivative
    checksums : dict[str, str]
        A checksums of all files within the file_group in a dictionary sorted by
        relative file paths
    record : ..provenance.Record | None
        The provenance record for the pipeline that generated the file-group,
        if applicable
    resource_name : str | None
        For repositories where the name of the file format is saved with the
        data (i.e. XNAT), the name of the resource enables straightforward
        format identification
    quality : str
        The quality label assigned to the file_group (e.g. as is saved on XNAT)
    local_path : str | None
        Path to the file-group on the local file system (i.e. cache for remote
        repositories)
    """

    def __init__(self, path, format=None, aux_files=None, order=None, uri=None,
                 exists=True, checksums=None, record=None, resource_name=None,
                 quality=None, local_path=None, data_node=None):
        FileGroupMixin.__init__(self, path=path, format=format)
        DataItem.__init__(self, data_node, exists, record)
        if aux_files is not None:
            if local_path is None:
                raise ArcanaUsageError(
                    "Side cars can only be provided to a FileGroup __init__ "
                    "of '{}' ({}) if the local cache path is also".format(
                        self.path, aux_files))
            if format is None:
                raise ArcanaUsageError(
                    "Side cars provided to '{}' file_group ({}) but format is "
                    "not specified".format(self.path, aux_files))
        if local_path is not None:
            local_path = op.abslocal_path(op.realpath(local_path))
            if aux_files is None:
                aux_files = {}
            elif set(aux_files.keys()) != set(self.format.aux_files.keys()):
                raise ArcanaUsageError(
                    "Provided side cars for '{}' but expected '{}'"
                    .format("', '".join(aux_files.keys()),
                            "', '".join(self.format.aux_files.keys())))
        self._local_path = local_path
        self.aux_files = aux_files if aux_files is not None else {}
        self.uri = uri
        self._order = order
        self.checksums = checksums
        self.resource_name = resource_name
        self.quality = quality

    # def __getattr__(self, attr):
    #     """
    #     For the convenience of being able to make calls on a file_group that are
    #     dependent on its format, e.g.

    #         >>> file_group = FileGroup('a_name', format=AnImageFormat())
    #         >>> file_group.get_header()

    #     we capture missing attributes and attempt to redirect them to methods
    #     of the format class that take the file_group as the first argument
    #     """
    #     try:
    #         frmt = self.__dict__['_format']
    #     except KeyError:
    #         frmt = None
    #     else:
    #         try:
    #             format_attr = getattr(frmt, attr)
    #         except AttributeError:
    #             pass
    #         else:
    #             if callable(format_attr):
    #                 return lambda *args, **kwargs: format_attr(self, *args,
    #                                                            **kwargs)
    #     raise AttributeError("FileGroups of '{}' format don't have a '{}' "
    #                          "attribute".format(frmt, attr))

    def __eq__(self, other):
        eq = (FileGroupMixin.__eq__(self, other)
              and DataItem.__eq__(self, other)
              and self.aux_files == other.aux_files
              and self._order == other._order
              and self._checksums == other._checksums
              and self.resource_name == other.resource_name
              and self.quality == other.quality)
        # Avoid having to cache file_group in order to test equality unless they
        # are already both cached
        try:
            if self._local_path is not None and other._local_path is not None:
                eq &= (self._local_path == other._local_path)
        except AttributeError:
            return False
        return eq

    def __hash__(self):
        return (FileGroupMixin.__hash__(self)
                ^ DataItem.__hash__(self)
                ^ hash(self._order)
                ^ hash(tuple(sorted(self.aux_files.items())))
                ^ hash(self._checksums)
                ^ hash(self.resource_name)
                ^ hash(self.quality))

    def __lt__(self, other):
        if isinstance(self.order, int) and isinstance(other.order, str):
            return True
        elif isinstance(self.order, str) and isinstance(other.order, int):
            return False
        else:
            if self.order == other.order:
                return self.path < other.path
            else:
                return self.order < other.order

    def __repr__(self):
        return ("{}('{}', {}, '{}', subj={}, vis={}, exists={}, "
                "quality={}{})"
                .format(
                    type(self).__name__, self.path, self.format,
                    self.frequency, self.subject_id,
                    self.visit_id, self.exists, self.quality,
                    (", resource_name='{}'".format(self._resource_name)
                     if self._resource_name is not None else '')))

    def find_mismatch(self, other, indent=''):
        mismatch = FileGroupMixin.find_mismatch(self, other, indent)
        mismatch += DataItem.find_mismatch(self, other, indent)
        sub_indent = indent + '  '
        if self.order != other.order:
            mismatch += ('\n{}order: self={} v other={}'
                         .format(sub_indent, self.order,
                                 other.order))
        if self._checksums != other._checksums:
            mismatch += ('\n{}checksum: self={} v other={}'
                         .format(sub_indent, self.checksums,
                                 other.checksums))
        if self.resource_name != other.resource_name:
            mismatch += ('\n{}format_name: self={} v other={}'
                         .format(sub_indent, self.resource_name,
                                 other.resource_name))
        if self.quality != other.quality:
            mismatch += ('\n{}quality: self={} v other={}'
                         .format(sub_indent, self.quality,
                                 other.quality))
        if self.resource_name != other.resource_name:
            mismatch += ('\n{}resource_name: self={} v other={}'
                         .format(sub_indent, self.resource_name,
                                 other.resource_name))
        return mismatch

    @property
    def local_path(self):
        if not self.exists:
            raise ArcanaDataNotDerivedYetError(
                self.path,
                "Cannot access cache-path of {} as it hasn't been derived yet"
                .format(self))
        if self._local_path is None:
            if self.dataset is not None:
                self.get()  # Retrieve from dataset
            else:
                raise ArcanaError(
                    "Neither cache-path nor dataset has been set for "
                    "FileGroup('{}')".format(self.path))
        return self._local_path

    def set_local_path(self, local_path, aux_files=None):
        if local_path is not None:
            local_path = op.abspath(op.realpath(local_path))
            self._exists = True
        self._local_path = local_path
        if aux_files is None:
            self._aux_files = dict(
                self.format.default_aux_file_paths(local_path))
        else:
            if set(self.format.aux_files.keys()) != set(aux_files.keys()):
                raise ArcanaUsageError(
                    "Keys of provided side cars ('{}') don't match format "
                    "('{}')".format("', '".join(aux_files.keys()),
                                    "', '".join(self.format.aux_files.keys())))
            self.aux_files = aux_files
        self._checksums = self.calculate_checksums()
        self.put()  # Push to dataset

    @local_path.setter
    def local_path(self, local_path):
        self.set_local_path(local_path, aux_files=None)

    @property
    def local_paths(self):
        "Iterates through all files in the group and returns their cache paths"

        if self.format is None:
            raise ArcanaFileFormatError(
                "Cannot get paths of file_group ({}) that hasn't had its format "
                "set".format(self))
        if self.format.directory:
            return chain(*((op.join(root, f) for f in files)
                           for root, _, files in os.walk(self.local_path)))
        else:
            return chain([self.local_path], self.aux_files.values())

    @property
    def fname(self):
        if self.format is None:
            raise ArcanaFileFormatError(
                "Need to provide format before accessing the filename of {}"
                .format(self))
        return self.path + self.format.ext_str

    # @property
    # def basename(self):
    #     return self.name

    @property
    def order(self):
        if self.order is None:
            return self.path
        else:
            return self._order

    @order.setter
    def order(self, order):
        if self._order is None:
            self._order = order
        elif order != self._order:
            raise ArcanaUsageError("Can't change value of ID for {} from {} "
                                   "to {}".format(self, self._order, order))

    @property
    def uri(self):
        return self._uri

    @uri.setter
    def uri(self, uri):
        if self._uri is None:
            # Use the resource label instead of the resource ID to avoid
            # duplication of resources
            self._uri = uri
        elif uri != self._uri:
            raise ArcanaUriAlreadySetException(
                "Can't change value of URI for {} from {} to {}"
                .format(self, self._uri, uri))

    def aux_file(self, name):
        return self.aux_files[name]

    @property
    def aux_file_fnames_and_paths(self):
        return ((self.basename + self.format.aux_files[sc_name], sc_path)
                for sc_name, sc_path in self.aux_files.items())

    @property
    def format_name(self):
        if self.format is None:
            name = self._resource_name
        else:
            name = self.format.name
        return name

    @property
    def checksums(self):
        if not self.exists:
            raise ArcanaDataNotDerivedYetError(
                self.path,
                "Cannot access checksums of {} as it hasn't been derived yet"
                .format(self))
        if self._checksums is None:
            if self.dataset is not None:
                self._checksums = self.dataset.get_checksums(self)
            if self._checksums is None:
                self._checksums = self.calculate_checksums()
        return self._checksums

    def calculate_checksums(self):
        checksums = {}
        for fpath in self.paths:
            fhash = hashlib.md5()
            with open(fpath, 'rb') as f:
                # Calculate hash in chunks so we don't run out of memory for
                # large files.
                for chunk in iter(lambda: f.read(HASH_CHUNK_SIZE), b''):
                    fhash.update(chunk)
            checksums[op.relpath(fpath, self.path)] = fhash.hexdigest()
        return checksums

    @classmethod
    def from_path(cls, local_path, **kwargs):
        if not op.exists(local_path):
            raise ArcanaUsageError(
                "Attempting to read FileGroup from path '{}' but it "
                "does not exist".format(local_path))
        if op.isdir(local_path):
            path = op.basename(local_path)
        else:
            path = split_extension(op.basename(local_path))[0]
        return cls(path, local_path=path, **kwargs)

    def initkwargs(self):
        dct = FileGroupMixin.initkwargs(self)
        dct.update(DataItem.initkwargs(self))
        dct['local_path'] = self.local_path
        dct['order'] = self.order
        dct['uri'] = self.uri
        dct['bids_attr'] = self.bids_attr
        dct['checksums'] = self.checksums
        dct['resource_name'] = self.resource_name
        dct['quality'] = self.quality
        return dct

    def get(self):
        if self.dataset is not None:
            self._exists = True
            self._local_path, self._aux_files = self.dataset.get_file_group(
                self)

    def put(self):
        if self.dataset is not None and self._local_path is not None:
            self.dataset.put_file_group(self)

    def contents_equal(self, other, **kwargs):
        """
        Test the equality of the file_group contents with another file_group. If the
        file_group's format implements a 'contents_equal' method than that is used
        to determine the equality, otherwise a straight comparison of the
        checksums is used.

        Parameters
        ----------
        other : FileGroup
            The other file_group to compare to
        """
        if hasattr(self.format, 'contents_equal'):
            equal = self.format.contents_equal(self, other, **kwargs)
        else:
            equal = (self.checksums == other.checksums)
        return equal


class Field(DataItem, FieldMixin):
    """
    A representation of a value field in the dataset.

    Parameters
    ----------
    path : str
        The path to the relative location of the field, i.e. excluding
        information about which node in the data tree it belongs to
    dtype : type
        The datatype of the value. Can be one of (float, int, str)
    derived : bool
        Whether or not the value belongs in the derived session or not
    data_node : DataNode
        The data node that the field belongs to
    exists : bool
        Whether the field exists or is just a placeholder for a derivative
    record : ..provenance.Record | None
        The provenance record for the pipeline that generated the field,
        if applicable
    """

    def __init__(self, path, value=None, dtype=None, array=None,
                 data_node=None, exists=True, record=None):
        # Try to determine dtype and array from value if they haven't
        # been provided.
        if value is None:
            if dtype is None:
                raise ArcanaUsageError(
                    "Either 'value' or 'dtype' must be provided to "
                    "Field init")
            array = bool(array)  # Convert to array is None to False
        else:
            value = parse_value(value)
            if isinstance(value, list):
                if array is False:
                    raise ArcanaUsageError(
                        "Array value passed to '{}', which is explicitly not "
                        "an array ({})".format(path, value))
                array = True
            else:
                if array:
                    raise ArcanaUsageError(
                        "Non-array value ({}) passed to '{}', which expects "
                        "array{}".format(value, path,
                                         ('of type {}'.format(dtype)
                                          if dtype is not None else '')))
                array = False
            if dtype is None:
                if array:
                    dtype = type(value[0])
                else:
                    dtype = type(value)
            else:
                # Ensure everything is cast to the correct type
                if array:
                    value = [dtype(v) for v in value]
                else:
                    value = dtype(value)
        FieldMixin.__init__(self, path, dtype, array)
        DataItem.__init__(self, data_node, exists, record)
        self._value = value

    def __eq__(self, other):
        return (FieldMixin.__eq__(self, other)
                and DataItem.__eq__(self, other)
                and self.value == other.value
                and self.array == other.array)

    def __hash__(self):
        return (FieldMixin.__hash__(self)
                ^ DataItem.__hash__(self)
                ^ hash(self.value)
                ^ hash(self.array))

    def find_mismatch(self, other, indent=''):
        mismatch = FieldMixin.find_mismatch(self, other, indent)
        mismatch += DataItem.find_mismatch(self, other, indent)
        sub_indent = indent + '  '
        if self.value != other.value:
            mismatch += ('\n{}value: self={} v other={}'
                         .format(sub_indent, self.value,
                                 other.value))
        if self.array != other.array:
            mismatch += ('\n{}array: self={} v other={}'
                         .format(sub_indent, self.array,
                                 other.array))
        return mismatch

    def __int__(self):
        return int(self.value)

    def __float__(self):
        return float(self.value)

    def __str__(self):
        if self.array:
            val = '[' + ','.join(self._to_str(v) for v in self.value) + ']'
        else:
            val = self._to_str(self.value)
        return val

    def _to_str(self, val):
        if self.dtype is str:
            val = '"{}"'.format(val)
        else:
            val = str(val)
        return val

    def __lt__(self, other):
        return self.path < other.path

    def __repr__(self):
        return ("{}('{}',{} '{}', subject={}, visit={}, exists={})"
                .format(
                    type(self).__name__, self.path,
                    (" {},".format(self._value)
                     if self._value is not None else ''),
                    self.frequency, self.subject_id,
                    self.visit_id, self.exists))

    @property
    def value(self):
        if not self.exists:
            raise ArcanaDataNotDerivedYetError(
                self.path,
                "Cannot access value of {} as it hasn't been "
                "derived yet".format(repr(self)))
        if self._value is None:
            if self.dataset is not None:
                self._value = self.dataset.get_field(self)
            else:
                raise ArcanaError(
                    "Neither value nor dataset has been set for Field("
                    "'{}')".format(self.path))
        return self._value

    @value.setter
    def value(self, value):
        if self.array:
            self._value = [self.dtype(v) for v in value]
        else:
            self._value = self.dtype(value)
        self._exists = True
        self.put()

    @property
    def checksums(self):
        """
        For duck-typing with file_groups in checksum management. Instead of a
        checksum, just the value of the field is used
        """
        return self.value

    def initkwargs(self):
        dct = FieldMixin.initkwargs(self)
        dct.update(DataItem.initkwargs(self))
        dct['value'] = self.value
        return dct

    def get(self):
        if self.dataset is not None:
            self._exists = True
            self._value = self.dataset.get_field(self)

    def put(self):
        if self.dataset is not None and self._value is not None:
            self.dataset.put_field(self)


class MultiFormatFileGroup(DataItem, DataMixin):
    """A file-group potentially stored in multiple file formats, and
    the file formats can't be determined until a list of possible candidates
    are provided (i.e. in the 'match' method)
    

    Parameters
    ----------
    path : str
        The path to the relative location of the file group, i.e. excluding
        information about which node in the data tree it belongs to
    frequency : DataFreq
        The frequency that the file group occurs in the dataset, i.e. 
        per 'session', 'subject', 'visit', 'group_visit', 'group' or 'dataset'
    files : Sequence[str] | None
        Files in the file-group in (potentially) multiple formats.
    order : int | None
        The ID of the file_group in the session. To be used to
        distinguish multiple file_groups with the same scan type in the
        same session, e.g. scans taken before and after a task. For
        datasets where this isn't stored (i.e. Local), id can be None
    data_node : DataNode
        The data node that the field belongs to
    record : ..provenance.Record | None
        The provenance record for the pipeline that generated the file-group,
        if applicable
    resource_uris : Dict[str, str] | None
        For repositories where the name of the file format is saved with the
        data (i.e. XNAT), the name of the resource enables straightforward
        format identification. It is stored here along with URIs corresponding
        to each resource
    quality : str
        The quality label assigned to the file_group (e.g. as is saved on XNAT)
    local_path : str | None
        Path to the file-group in the local cache
    """

    def __init__(self, path, order=None, resource_uris=None, quality=None,
                 local_paths=None, record=None, data_node=None):
        DataItem.__init__(self, data_node, True, record)
        DataMixin.__init__(self, path=path)
        if local_paths is not None:
            local_paths = [op.abspath(op.realpath(p)) for p in local_paths]
        self._local_paths = local_paths
        self.order = order
        self.resource_uris = resource_uris
        self.quality = quality
        self._matched = {}

    def match_format(self, candidates):
        """
        Detects the format of the file-group from a list of possible
        candidates and returns a corresponding FileGroup object. If multiple
        candidates match the potential files, e.g. NiFTI-X (see dcm2niix) and
        NiFTI, then the first matching candidate is selected.

        If 'resource_uris' were specified when the multi-format file-group was
        created then that is used to select between the candidates. Otherwise
        the file extensions of the local paths, and extensions of the files
        within the directory will be used instead.

        Parameters
        ----------
        candidates : FileFormat | Sequence[FileFormat]
            A list of file-formats to try to match. The first matching format
            in the sequence will be used to create a file-group

        Returns
        -------
        FileGroup
            The file-group in the first matching format
        """
        # Ensure candidates is a list of file formats
        if not isinstance(candidates, Iterable):
            candidates = [candidates]
        else:
            candidates = list(candidates)
        # If multiple formats are specified via resource names
        common_kwargs = {
            'path': self.path,
            'frequency': self.frequency,
            'order': self.order,
            'subject_id': self.subject_id,
            'visit_id': self.visit_id,
            'dataset': self.dataset,
            'exists': True,
            'quality': self.quality}
        if not (self.resource_uris or self._local_paths):
            raise ArcanaError(
                "Either resource_uris or local paths must be provided "
                f"to UnresolvedFormatFileGroup('{self.path}') in before "
                "attempting to resolve a file-groups format")
        for candidate in candidates:
            try:
                # Attempt to access previously saved
                return self._matched[candidate]
            except KeyError:
                # Perform matching based on resource names in multi-format
                # file-group
                if self.resource_uris is not None:   
                    for resource_name, uri in self.resource_uris.items():
                        if resource_name in candidate.resource_uris:
                            return FileGroup(resource_name=resource_name,
                                             uri=uri, **common_kwargs)
                # Perform matching based on file-extensions of local paths in
                # multi-format file-group
                else:
                    local_path = None
                    aux_files = []
                    if candidate.directory:
                        if (len(self._local_paths) == 1
                            and op.isdir(self._local_paths[0])
                            and (candidate.within_dir_exts is None
                                or (candidate.within_dir_exts == frozenset(
                                    split_extension(f)[1]
                                    for f in os.listdir(self.local_paths)
                                    if not f.startswith('.'))))):
                            local_path = self._local_paths[0]
                    else:
                        try:
                            local_path, aux_files = candidate.assort_files(
                                self.local_paths)[0]
                        except ArcanaFileFormatError:
                            pass
                    if local_path is not None:
                        return FileGroup(local_path=local_path,
                                        aux_files=aux_files, **common_kwargs)
        # If we get to here none of the candidate formats have matched and
        # we raise and error
        if self.resource_uris:
            error_msg = (
                "Could not find a matching resource in {} for any of the "
                "candidates ({}), found ('{}')".format(
                    self,
                    ', '.join(str(c)for c in candidates),
                    "', '".join(self.resource_uris)))
        else:
            error_msg = (
                "Paths in {} ({}) did not match the naming conventions "
                "expected by any of the candidates formats ({}), found ('{}')"
                .format(self,
                        ', '.join(self._local_paths),
                        ', '.join(str(c)for c in candidates),
                        "', '".join(self.resource_uris)))
        raise ArcanaNoMatchingFileFormatError(error_msg)
