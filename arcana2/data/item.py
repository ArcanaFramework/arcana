import os
import os.path as op
from itertools import chain
import hashlib
import json
import re
from copy import deepcopy
from pprint import pformat
from datetime import datetime
from deepdiff import DeepDiff
from arcana2.exceptions import ArcanaError, ArcanaUsageError
from collections.abc import Iterable
from arcana2.utils import split_extension, parse_value
from arcana2.exceptions import (
    ArcanaError, ArcanaFileFormatError, ArcanaUsageError, ArcanaNameError,
    ArcanaDataNotDerivedYetError, ArcanaUriAlreadySetException,
    ArcanaNoMatchingFileFormatError)
# from .file_format import FileFormat
from .base import FileGroupMixin, FieldMixin, DataMixin

HASH_CHUNK_SIZE = 2 ** 20  # 1MB


class DataItem():

    is_spec = False

    def __init__(self, data_node, exists, provenance):
        self.data_node = data_node
        self.exists = exists
        if not isinstance(provenance, Provenance):
            provenance = Provenance(provenance)
        self._provenance = provenance

    def __eq__(self, other):
        return (self.data_node == other.data_node
                and self.exists == other.exists
                and self._provenance == other._provenance)

    def __hash__(self):
        return (hash(self.data_node)
                ^ hash(self.exists)
                ^ hash(self._provenance))

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
        if self._provenance != other._provenance:
            mismatch += ('\n{}_provenance: self={} v other={}'
                         .format(sub_indent, self._provenance,
                                 other._provenance))
        return mismatch

    @property
    def provenance(self):
        return self._provenance

    @provenance.setter
    def provenance(self, provenance):
        if self.name_path not in provenance.outputs:
            raise ArcanaNameError(
                self.name_path,
                "{} was not found in outputs {} of provenance provenance {}"
                .format(self.name_path, provenance.outputs.keys(), provenance))
        self._provenance = provenance

    @property
    def recorded_checksums(self):
        if self.provenance is None:
            return None
        else:
            return self.provenance.outputs[self.name_path]

    def initkwargs(self):
        dct = super().initkwargs()
        dct['data_node'] = self.data_node
        dct['exists'] = self.exists
        dct['provenance'] = self.provenance
        return dct


class FileGroup(DataItem, FileGroupMixin):
    """
    A representation of a file_group within the dataset.

    Parameters
    ----------
    name_path : str
        The name_path to the relative location of the file group, i.e. excluding
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
    exists : bool
        Whether the file_group exists or is just a placeholder for a derivative
    checksums : dict[str, str]
        A checksums of all files within the file_group in a dictionary sorted
        bys relative file name_paths
    provenance : Provenance | None
        The provenance for the pipeline that generated the file-group,
        if applicable
    format_name : str | None
        For repositories where the name of the file format is saved with the
        data (i.e. XNAT), the name of the resource enables straightforward
        format identification
    quality : str
        The quality label assigned to the file_group (e.g. as is saved on XNAT)
    local_path : str | None
        Path to the file-group on the local file system (i.e. cache for remote
        repositories)
    data_node : DataNode
        The data node within a dataset that the file-group belongs to
    """

    def __init__(self, name_path, format=None, aux_files=None, order=None,
                 uri=None, exists=True, checksums=None, provenance=None,
                 format_name=None, quality=None, local_path=None,
                 data_node=None, derived=False):
        FileGroupMixin.__init__(self, name_path=name_path, format=format)
        DataItem.__init__(self, data_node, exists, provenance, derived)
        if aux_files is not None:
            if local_path is None:
                raise ArcanaUsageError(
                    "Side cars can only be provided to a FileGroup __init__ "
                    "of '{}' ({}) if the local cache name_path is also".format(
                        self.name_path, aux_files))
            if format is None:
                raise ArcanaUsageError(
                    "Side cars provided to '{}' file_group ({}) but format is "
                    "not specified".format(self.name_path, aux_files))
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
        self._uri = uri
        self._order = order
        self.checksums = checksums
        self.format_name = format_name
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
              and self.format_name == other.format_name
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
                ^ hash(self.format_name)
                ^ hash(self.quality))

    def __lt__(self, other):
        if isinstance(self.order, int) and isinstance(other.order, str):
            return True
        elif isinstance(self.order, str) and isinstance(other.order, int):
            return False
        else:
            if self.order == other.order:
                return self.name_path < other.name_path
            else:
                return self.order < other.order

    def __repr__(self):
        return ("{}('{}', {}, exists={}, quality={}{})"
                .format(
                    type(self).__name__, self.name_path, self.format,
                    self.exists, self.quality,
                    (", format_name='{}'".format(self.format_name)
                     if self.format_name is not None else '')))

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
        if self.quality != other.quality:
            mismatch += ('\n{}quality: self={} v other={}'
                         .format(sub_indent, self.quality,
                                 other.quality))
        if self.format_name != other.format_name:
            mismatch += ('\n{}format_name: self={} v other={}'
                         .format(sub_indent, self.format_name,
                                 other.format_name))
        return mismatch

    @property
    def local_path(self):
        if not self.exists:
            raise ArcanaDataNotDerivedYetError(
                self.name_path,
                "Cannot access cache-path of {} as it hasn't been derived yet"
                .format(self))
        if self._local_path is None:
            if self.dataset is not None:
                self.get()  # Retrieve from dataset
            else:
                raise ArcanaError(
                    "Neither cache-name_path nor dataset has been set for "
                    "FileGroup('{}')".format(self.name_path))
        return self._local_path

    def set_local_path(self, local_path, aux_files=None):
        if local_path is not None:
            local_path = op.abspath(op.realpath(local_path))
            self.exists = True
        self._local_path = local_path
        if aux_files is None:
            self.aux_files = dict(
                self.format.default_aux_file_paths(local_path))
            if self.exists:
                if dont_exist:= [
                        f'{n}: {p}' for n, p in self._aux_files.items()
                        if not op.exists(p)]:
                    raise ArcanaUsageError(
                        "Auxiliary files implicitly expected alongside "
                        f"primary path {local_path} (" + ', '.join(dont_exist)
                        + ") do not exist")
        else:
            if set(self.format.aux_files.keys()) != set(aux_files.keys()):
                raise ArcanaUsageError(
                    "Keys of provided auxiliary files ('{}') don't match format"
                    " ('{}')".format("', '".join(aux_files.keys()),
                                     "', '".join(self.format.aux_files.keys())))
            self.aux_files = aux_files
        self.checksums = self.calculate_checksums()
        self.put()  # Push to dataset

    @local_path.setter
    def local_path(self, local_path):
        self.set_local_path(local_path, aux_files=None)

    @property
    def local_paths(self):
        "Iterates through all files in the group and returns their cache name_paths"

        if self.format is None:
            raise ArcanaFileFormatError(
                "Cannot get name_paths of file_group ({}) that hasn't had its format "
                "set".format(self))
        if self.format.directory:
            return chain(*((op.join(root, f) for f in files)
                           for root, _, files in os.walk(self.local_path)))
        else:
            return chain([self.local_path], self.aux_files.values())

    @property
    def value(self):
        """For duck-typing with Field in source tasks"""
        return self.local_path

    @property
    def fname(self):
        if self.format is None:
            raise ArcanaFileFormatError(
                "Need to provide format before accessing the filename of {}"
                .format(self))
        return self.name_path + self.format.ext_str

    # @property
    # def basename(self):
    #     return self.name

    @property
    def order(self):
        if self.order is None:
            return self.name_path
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
    def checksums(self):
        if not self.exists:
            raise ArcanaDataNotDerivedYetError(
                self.name_path,
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
        for fpath in self.name_paths:
            fhash = hashlib.md5()
            with open(fpath, 'rb') as f:
                # Calculate hash in chunks so we don't run out of memory for
                # large files.
                for chunk in iter(lambda: f.read(HASH_CHUNK_SIZE), b''):
                    fhash.update(chunk)
            checksums[op.relpath(fpath, self.name_path)] = fhash.hexdigest()
        return checksums

    @classmethod
    def from_path(cls, local_path, **kwargs):
        if not op.exists(local_path):
            raise ArcanaUsageError(
                "Attempting to read FileGroup from name_path '{}' but it "
                "does not exist".format(local_path))
        if op.isdir(local_path):
            name_path = op.basename(local_path)
        else:
            name_path = split_extension(op.basename(local_path))[0]
        return cls(name_path, local_path=name_path, **kwargs)

    def initkwargs(self):
        dct = FileGroupMixin.initkwargs(self)
        dct.update(DataItem.initkwargs(self))
        dct['local_path'] = self.local_path
        dct['order'] = self.order
        dct['uri'] = self.uri
        dct['bids_attr'] = self.bids_attr
        dct['checksums'] = self.checksums
        dct['format_name'] = self.format_name
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
    name_path : str
        The name_path to the relative location of the field, i.e. excluding
        information about which node in the data tree it belongs to
    dtype : type
        The datatype of the value. Can be one of (float, int, str)
    derived : bool
        Whether or not the value belongs in the derived session or not
    data_node : DataNode
        The data node that the field belongs to
    exists : bool
        Whether the field exists or is just a placeholder for a derivative
    provenance : Provenance | None
        The provenance for the pipeline that generated the field,
        if applicable
    """

    def __init__(self, name_path, value=None, dtype=None, array=None,
                 data_node=None, exists=True, provenance=None,
                 derived=False):
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
                        "an array ({})".format(name_path, value))
                array = True
            else:
                if array:
                    raise ArcanaUsageError(
                        "Non-array value ({}) passed to '{}', which expects "
                        "array{}".format(value, name_path,
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
        FieldMixin.__init__(self, name_path, dtype, array)
        DataItem.__init__(self, data_node, exists, provenance, derived)
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
        return self.name_path < other.name_path

    def __repr__(self):
        return ("{}('{}',{} exists={})"
                .format(
                    type(self).__name__, self.name_path,
                    (" {},".format(self._value)
                     if self._value is not None else ''),
                    self.exists))

    @property
    def value(self):
        if not self.exists:
            raise ArcanaDataNotDerivedYetError(
                self.name_path,
                "Cannot access value of {} as it hasn't been "
                "derived yet".format(repr(self)))
        if self._value is None:
            if self.dataset is not None:
                self._value = self.dataset.get_field(self)
            else:
                raise ArcanaError(
                    "Neither value nor dataset has been set for Field("
                    "'{}')".format(self.name_path))
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


class UnresolvedFileGroup(DataItem, DataMixin):
    """A file-group stored in, potentially multiple, unknown file formats.
    File formats are resolved by providing a list of candidates to the
    'resolve' method

    Parameters
    ----------
    name_path : str
        The name_path to the relative location of the file group, i.e. excluding
        information about which node in the data tree it belongs to
    files : Sequence[str] | None
        Files in the file-group in (potentially) multiple formats.
    order : int | None
        The ID of the file_group in the session. To be used to
        distinguish multiple file_groups with the same scan type in the
        same session, e.g. scans taken before and after a task. For
        datasets where this isn't stored (i.e. Local), id can be None
    format_uris : Dict[str, str] | None
        For repositories where the name of the file format is saved with the
        data (i.e. XNAT), the name of the resource enables straightforward
        format identification. It is stored here along with URIs corresponding
        to each resource
    quality : str
        The quality label assigned to the file_group (e.g. as is saved on XNAT)
    local_paths : Sequence[str] | None
        Path to the file-group in the local cache
    data_node : DataNode
        The data node that the field belongs to
    provenance : Provenance | None
        The provenance for the pipeline that generated the file-group,
        if applicable
    """

    def __init__(self, name_path, order=None, format_uris=None, quality=None,
                 local_paths=None, provenance=None, data_node=None):
        DataItem.__init__(self, data_node, True, provenance)
        DataMixin.__init__(self, name_path=name_path)
        if local_paths is not None:
            local_paths = [op.abspath(op.realpath(p)) for p in local_paths]
        self.local_paths = local_paths
        self.order = order
        self.format_uris = format_uris
        self.quality = quality
        self._matched = {}

    def resolve(self, candidates):
        """
        Detects the format of the file-group from a list of possible
        candidates and returns a corresponding FileGroup object. If multiple
        candidates match the potential files, e.g. NiFTI-X (see dcm2niix) and
        NiFTI, then the first matching candidate is selected.

        If 'format_uris' were specified when the multi-format file-group was
        created then that is used to select between the candidates. Otherwise
        the file extensions of the local name_paths, and extensions of the files
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
            'name_path': self.name_path,
            'frequency': self.frequency,
            'order': self.order,
            'dataset': self.dataset,
            'exists': True,
            'quality': self.quality}
        if not (self.format_uris or self.local_paths):
            raise ArcanaError(
                "Either format_uris or local name_paths must be provided "
                f"to UnresolvedFileGroup('{self.name_path}') in before "
                "attempting to resolve a file-groups format")
        for candidate in candidates:
            try:
                # Attempt to access previously saved
                return self._matched[candidate]
            except KeyError:
                # Perform matching based on resource names in multi-format
                # file-group
                if self.format_uris is not None:   
                    for format_name, uri in self.format_uris.items():
                        if format_name in candidate.format_uris:
                            return candidate.file_group_cls(
                                format_name=format_name, uri=uri,
                                **common_kwargs)
                # Perform matching based on file-extensions of local name_paths in
                # multi-format file-group
                else:
                    local_path = None
                    aux_files = []
                    if candidate.directory:
                        if (len(self.local_paths) == 1
                            and op.isdir(self.local_paths[0])
                            and (candidate.within_dir_exts is None
                                or (candidate.within_dir_exts == frozenset(
                                    split_extension(f)[1]
                                    for f in os.listdir(self.local_paths)
                                    if not f.startswith('.'))))):
                            local_path = self.local_paths[0]
                    else:
                        try:
                            local_path, aux_files = candidate.assort_files(
                                self.local_paths)[0]
                        except ArcanaFileFormatError:
                            pass
                    if local_path is not None:
                        return candidate.file_group_cls(
                            local_path=local_path, aux_files=aux_files,
                            **common_kwargs)
        # If we get to here none of the candidate formats have matched and
        # we raise and error
        if self.format_uris:
            error_msg = (
                "Could not find a matching resource in {} for any of the "
                "candidates ({}), found ('{}')".format(
                    self,
                    ', '.join(str(c)for c in candidates),
                    "', '".join(self.format_uris)))
        else:
            error_msg = (
                "Paths in {} ({}) did not match the naming conventions "
                "expected by any of the candidates formats ({}), found ('{}')"
                .format(self,
                        ', '.join(self.local_paths),
                        ', '.join(str(c)for c in candidates),
                        "', '".join(self.format_uris)))
        raise ArcanaNoMatchingFileFormatError(error_msg)

class UnresolvedField():
    """A field stored in unknown datatype. Can create a Field
    object by the 'resolve' method.
    

    Parameters
    ----------
    name_path : str
        The name_path to the relative location of the file group, i.e. excluding
        information about which node in the data tree it belongs to
    value : str
        The value of the field (in a generic datatype)
    data_node : DataNode
        The data node that the field belongs to
    provenance : Provenance | None
        The provenance for the pipeline that generated the file-group,
        if applicable
    """

    def __init__(self, name_path, value=None, data_node=None, provenance=None):
        DataItem.__init__(self, data_node, True, provenance)
        DataMixin.__init__(self, name_path=name_path)
        self.value = value

    def resolve(self, dtype, array=False):
        """
        Detects the format of the file-group from a list of possible
        candidates and returns a corresponding FileGroup object. If multiple
        candidates match the potential files, e.g. NiFTI-X (see dcm2niix) and
        NiFTI, then the first matching candidate is selected.

        If 'format_uris' were specified when the multi-format file-group was
        created then that is used to select between the candidates. Otherwise
        the file extensions of the local name_paths, and extensions of the files
        within the directory will be used instead.

        Parameters
        ----------
        dtype : type
            The dtype of the field
        array : bool
            Whether the field should be treated as an array (i.e. split on ',')

        Returns
        -------
        Field
            The field with resolved datatype
        """
        if array:
            value = [dtype(v) for v in self.value[1:-1].split(',')]
        else:
            value = dtype(self.value)
        return Field(name_path=self.name_path, value=value, dtype=dtype, array=array,
                     data_node=self.data_node, provenance=self.provenance)
        

class Provenance():
    """
    A representation of the information required to describe the provenance of
    analysis derivatives. Provenances the provenance information relevant to a
    specific session, i.e. the general configuration of the pipeline and file
    checksums|field values of the pipeline inputs used to derive the outputs in
    a given session (or timepoint, subject, analysis summary). It also provenances
    the checksums|values of the outputs in order to detect if they have been
    altered outside of Arcana's management (e.g. manual QC/correction)

    Parameters
    ----------
    dct : dict[str, Any]
        A dictionary containing the provenance record
    """

    PROV_VERSION_KEY = '__prov_version__'
    PROV_VERSION = '1.0'
    DATETIME = 'datetime'

    def __init__(self, dct):
        self.dct = deepcopy(dct)
        if self.DATETIME not in self.dct:
            self.dct[self.DATETIME] = datetime.now().isoformat()
        if self.PROV_VERSION_KEY not in self.dct:
            self.dct[self.PROV_VERSION_KEY] = self.PROV_VERSION

    def __repr__(self):
        return repr(self.dct)

    def __eq__(self, other):
        return self.dct == other.dct

    def __getitem__(self, key):
        return self.dct[key]

    def __setitem__(self, key, value):
        self.dct[key] = value

    def items(self):
        return self.dct.items()

    @property
    def datetime(self):
        return self.dct[self.DATETIME]

    @property
    def version(self):
        return self.dct[self.PROV_VERSION_KEY]

    def save(self, local_path):
        """
        Saves the provenance object to a JSON file, optionally including
        checksums for inputs and outputs (which are initially produced mid-
        run) to insert during the write

        Parameters
        ----------
        name_path : str
            Path to save the generated JSON file
        inputs : dict[str, str | list[str] | list[list[str]]] | None
            Checksums of all pipeline inputs used by the pipeline. For inputs
            of matching frequency to the output derivative associated with the
            provenance object, the values of the dictionary will be single
            checksums. If the output is of lower frequency they will be lists
            of checksums or in the case of 'per_session' inputs to 'per_dataset'
            outputs, lists of lists of checksum. They need to be provided here
            if the provenance object was initialised without checksums
        outputs : dict[str, str] | None
            Checksums of all pipeline outputs. They need to be provided here
            if the provenance object was initialised without checksums
        """
        with open(local_path, 'w') as f:
            try:
                json.dump(self.dct, f, sort_keys=True, indent=2)
            except TypeError:
                raise ArcanaError(
                    "Could not serialise provenance provenance dictionary:\n{}"
                    .format(pformat(self.dct)))

    @classmethod
    def load(cls, local_path):
        """
        Loads a saved provenance object from a JSON file

        Parameters
        ----------
        name_path : str
            Path to the provenance file
        local_path : str
            The name_path to a local file containing the provenance JSON

        Returns
        -------
        provenance : Provenance
            The loaded provenance provenance
        """
        with open(local_path) as f:
            dct = json.load(f)
        return Provenance(dct)

    def mismatches(self, other, include=None, exclude=None):
        """
        Compares information stored within provenance objects with the
        exception of version information to see if they match. Matches are
        constrained to the name_paths passed to the 'include' kwarg, with the
        exception of sub-name_paths passed to the 'exclude' kwarg

        Parameters
        ----------
        other : Provenance
            The provenance object to compare against
        include : list[list[str]] | None
            Paths in the provenance to include in the match. If None all are
            incluced
        exclude : list[list[str]] | None
            Paths in the provenance to exclude from the match. In None all are
            excluded
        """
        if include is not None:
            include_res = [self._gen_prov_path_regex(p) for p in include]
        if exclude is not None:
            exclude_res = [self._gen_prov_path_regex(p) for p in exclude]
        diff = DeepDiff(self._prov, other._prov, ignore_order=True)
        # Create regular expresssions for the include and exclude name_paths in
        # the format that deepdiff uses for nested dictionary/lists

        def include_change(change):
            if include is None:
                included = True
            else:
                included = any(rx.match(change) for rx in include_res)
            if included and exclude is not None:
                included = not any(rx.match(change) for rx in exclude_res)
            return included

        filtered_diff = {}
        for change_type, changes in diff.items():
            if isinstance(changes, dict):
                filtered = dict((k, v) for k, v in changes.items()
                                if include_change(k))
            else:
                filtered = [c for c in changes if include_change(c)]
            if filtered:
                filtered_diff[change_type] = filtered
        return filtered_diff

    @classmethod
    def _gen_prov_path_regex(self, local_path):
        if isinstance(local_path, str):
            if local_path.startswith('/'):
                local_path = local_path[1:]
            regex = re.compile(r"root\['{}'\].*"
                               .format(r"'\]\['".join(local_path.split('/'))))
        elif not isinstance(local_path, re.Pattern):
            raise ArcanaUsageError(
                "Provenance in/exclude name_paths can either be name_path strings or "
                "regexes, not '{}'".format(local_path))
        return regex
