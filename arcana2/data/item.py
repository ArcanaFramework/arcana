import os
import os.path as op
from itertools import chain
import hashlib
from arcana2.utils import split_extension, parse_value
from arcana2.exceptions import (
    ArcanaError, ArcanaFileFormatError, ArcanaUsageError, ArcanaNameError,
    ArcanaDataNotDerivedYetError, ArcanaUriAlreadySetException)
# from .file_format import FileFormat
from .base import FileGroupMixin, FieldMixin
from .tree import DataFreq

HASH_CHUNK_SIZE = 2 ** 20  # 1MB


class DataItem(object):

    is_spec = False

    def __init__(self, subject_id, visit_id, dataset, exists, record):
        self.subject_id = subject_id
        self.visit_id = visit_id
        self.dataset = dataset
        self.exists = exists
        self._record = record

    def __eq__(self, other):
        return (self.subject_id == other.subject_id
                and self.visit_id == other.visit_id
                and self.exists == other.exists
                and self._record == other._record)

    def __hash__(self):
        return (hash(self.subject_id)
                ^ hash(self.visit_id)
                ^ hash(self.exists)
                ^ hash(self._record))

    def find_mismatch(self, other, indent=''):
        sub_indent = indent + '  '
        mismatch = ''
        if self.subject_id != other.subject_id:
            mismatch += ('\n{}subject_id: self={} v other={}'
                         .format(sub_indent, self.subject_id,
                                 other.subject_id))
        if self.visit_id != other.visit_id:
            mismatch += ('\n{}visit_id: self={} v other={}'
                         .format(sub_indent, self.visit_id,
                                 other.visit_id))
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
    def session_id(self):
        return (self.subject_id, self.visit_id)

    @property
    def record(self):
        return self._record

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
        dct['dataset'] = self.dataset
        dct['subject_id'] = self.subject_id
        dct['visit_id'] = self.visit_id
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
    frequency : DataFreq
        The frequency that the file group occurs in the dataset, i.e. 
        per 'session', 'subject', 'visit', 'group_visit', 'group' or 'dataset'
    derived : bool
        Whether the scan was generated or acquired. Depending on the dataset
        used to store the file_group this is used to determine the location of the
        file_group.
    path : str | None
        The path to the file_group (for repositories on the local system)
    aux_files : dict[str, str] | None
        Additional files in the file_group. Keys should match corresponding
        aux_files dictionary in format.
    scan_id : int | None
        The ID of the file_group in the session. To be used to
        distinguish multiple file_groups with the same scan type in the
        same session, e.g. scans taken before and after a task. For
        datasets where this isn't stored (i.e. Local), id can be None
    subject_id : int | str | None
        The id of the subject which the file_group belongs to
    visit_id : int | str | None
        The id of the visit which the file_group belongs to
    dataset : Dataset
        The dataset which the file-group is stored
    exists : bool
        Whether the file_group exists or is just a placeholder for a derivative
    checksums : dict[str, str]
        A checksums of all files within the file_group in a dictionary sorted by
        relative file paths
    record : ..provenance.Record | None
        The provenance record for the pipeline that generated the file set,
        if applicable
    resource_name : str | None
        For repositories where the name of the file format is saved with the
        data (i.e. XNAT), the name of the resource enables straightforward
        format identification
    quality : str
        The quality label assigned to the file_group (e.g. as is saved on XNAT)
    cache_path : str | None
        Path to the file-group in the local cache
    """

    def __init__(self, path, format=None, frequency=DataFreq.session,
                 aux_files=None, scan_id=None, uri=None,
                 subject_id=None, visit_id=None, dataset=None,
                 exists=True, checksums=None, record=None, resource_name=None,
                 quality=None, cache_path=None):
        FileGroupMixin.__init__(self, path=path, format=format,
                                frequency=frequency)
        DataItem.__init__(self, subject_id, visit_id, dataset, exists, record)
        if aux_files is not None:
            if path is None:
                raise ArcanaUsageError(
                    "Side cars provided to '{}' file_group ({}) but not primary "
                    "path".format(self.path, aux_files))
            if format is None:
                raise ArcanaUsageError(
                    "Side cars provided to '{}' file_group ({}) but format is "
                    "not specified".format(self.path, aux_files))
        if cache_path is not None:
            cache_path = op.abscache_path(op.realpath(cache_path))
            if aux_files is None:
                aux_files = {}
            elif set(aux_files.keys()) != set(self.format.aux_files.keys()):
                raise ArcanaUsageError(
                    "Provided side cars for '{}' but expected '{}'"
                    .format("', '".join(aux_files.keys()),
                            "', '".join(self.format.aux_files.keys())))
        self._cache_path = cache_path
        self.aux_files = aux_files if aux_files is not None else {}
        self.uri = uri
        self.scan_id = scan_id
        self.checksums = checksums
        self.resource_name = resource_name
        self.quality = quality
        # if potential_aux_files is not None and format is not None:
        #     raise ArcanaUsageError(
        #         "Potential paths should only be provided to FileGroup.__init__ "
        #         "({}) when the format of the file_group ({}) is not determined"
        #         .format(self.name, format))
        # if potential_aux_files is not None:
        #     potential_aux_files = list(potential_aux_files)
        # self._potential_aux_files = potential_aux_files

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
              and self.scan_id == other.scan_id
              and self._checksums == other._checksums
              and self.resource_name == other.resource_name
              and self.quality == other.quality)
        # Avoid having to cache file_group in order to test equality unless they
        # are already both cached
        # try:
        #     if self._path is not None and other._path is not None:
        #         eq &= (self._path == other._path)
        # except AttributeError:
        #     return False
        return eq

    def __hash__(self):
        return (FileGroupMixin.__hash__(self)
                ^ DataItem.__hash__(self)
                ^ hash(self.scan_id)
                ^ hash(tuple(sorted(self.aux_files.items())))
                ^ hash(self._checksums)
                ^ hash(self.resource_name)
                ^ hash(self.quality))

    def __lt__(self, other):
        if isinstance(self.scan_id, int) and isinstance(other.scan_id, str):
            return True
        elif isinstance(self.scan_id, str) and isinstance(other.scan_id, int):
            return False
        else:
            if self.scan_id == other.scan_id:
                return self.path < other.path
            else:
                return self.scan_id < other.scan_id

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
        if self.scan_id != other.scan_id:
            mismatch += ('\n{}scan_id: self={} v other={}'
                         .format(sub_indent, self.scan_id,
                                 other.scan_id))
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
    def cache_path(self):
        if not self.exists:
            raise ArcanaDataNotDerivedYetError(
                self.path,
                "Cannot access cache path of {} as it hasn't been derived yet"
                .format(self))
        if self._cache_path is None:
            if self.dataset is not None:
                self.get()  # Retrieve from dataset
            else:
                raise ArcanaError(
                    "Neither path nor dataset has been set for FileGroup("
                    "'{}')".format(self.path))
        return self._cache_path

    def set_cache_path(self, cache_path, aux_files=None):
        if cache_path is not None:
            cache_path = op.abspath(op.realpath(cache_path))
            self._exists = True
        self._cache_path = cache_path
        if aux_files is None:
            self._aux_files = dict(
                self.format.default_aux_file_paths(cache_path))
        else:
            if set(self.format.aux_files.keys()) != set(aux_files.keys()):
                raise ArcanaUsageError(
                    "Keys of provided side cars ('{}') don't match format "
                    "('{}')".format("', '".join(aux_files.keys()),
                                    "', '".join(self.format.aux_files.keys())))
            self.aux_files = aux_files
        self._checksums = self.calculate_checksums()
        self.put()  # Push to dataset

    @cache_path.setter
    def cache_path(self, path):
        self.set_cache_path(path, aux_files=None)

    @property
    def cache_paths(self):
        "Iterates through all files in the group and returns their cache paths"

        if self.format is None:
            raise ArcanaFileFormatError(
                "Cannot get paths of file_group ({}) that hasn't had its format "
                "set".format(self))
        if self.format.directory:
            return chain(*((op.join(root, f) for f in files)
                           for root, _, files in os.walk(self.cache_path)))
        else:
            return chain([self.cache_path], self.aux_files.values())

    # @property
    # def format(self):
    #     return self._format

    # @format.setter
    # def format(self, format):
    #     assert isinstance(format, FileFormat)
    #     self._format = format
    #     if format.aux_files and self._path is not None:
    #         self._aux_files = format.assort_files(
    #             [self._path] + list(self._potential_aux_files))[1]
    #     if self._id is None and hasattr(format, 'extract_id'):
    #         self._id = format.extract_id(self)
    #     # No longer need to retain potentials after we have assigned the real
    #     # auxiliaries
    #     self._potential_aux_files = None

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
    def id(self):
        if self.scan_id is None:
            return self.path
        else:
            return self._scan_id

    @id.setter
    def id(self, id):
        if self._id is None:
            self._id = id
        elif id != self._id:
            raise ArcanaUsageError("Can't change value of ID for {} from {} "
                                   "to {}".format(self, self._id, id))

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
        return cls(path, cache_path=path, **kwargs)

    # def detect_format(self, candidates):
    #     """
    #     Detects the format of the file_group from a list of possible
    #     candidates. If multiple candidates match the potential files, e.g.
    #     NiFTI-X (see dcm2niix) and NiFTI, then the first matching candidate is
    #     selected.

    #     If a 'format_name' was specified when the file_group was
    #     created then that is used to select between the candidates. Otherwise
    #     the file extensions of the primary path and potential auxiliary files,
    #     or extensions of the files within the directory for directories are
    #     matched against those specified for the file formats

    #     Parameters
    #     ----------
    #     candidates : FileFormat
    #         A list of file-formats to select from.
    #     """
    #     if self._format is not None:
    #         raise ArcanaFileFormatError(
    #             "Format has already been set for {}".format(self))
    #     matches = [c for c in candidates if c.matches(self)]
    #     if not matches:
    #         raise ArcanaFileFormatError(
    #             "None of the candidate file formats ({}) match {}"
    #             .format(', '.join(str(c) for c in candidates), self))
    #     return matches[0]

    def initkwargs(self):
        dct = FileGroupMixin.initkwargs(self)
        dct.update(DataItem.initkwargs(self))
        dct['cache_path'] = self.cache_path
        dct['scan_id'] = self.scan_id
        dct['uri'] = self.uri
        dct['bids_attr'] = self.bids_attr
        dct['checksums'] = self.checksums
        dct['resource_name'] = self.resource_name
        # dct['potential_aux_files'] = self._potential_aux_files
        dct['quality'] = self.quality
        return dct

    def get(self):
        if self.dataset is not None:
            self._exists = True
            self._cache_path, self._aux_files = self.dataset.get_file_group(
                self)

    def put(self):
        if self.dataset is not None and self._cache_path is not None:
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
    frequency : DataFreq
        The frequency that the items occur in the dataset, i.e. 
        per 'session', 'subject', 'visit', 'group_visit', 'group' or 'dataset'
    derived : bool
        Whether or not the value belongs in the derived session or not
    subject_id : int | str | None
        The id of the subject which the field belongs to
    visit_id : int | str | None
        The id of the visit which the field belongs to
    dataset : Dataset
        The dataset which the field is stored
    exists : bool
        Whether the field exists or is just a placeholder for a derivative
    record : ..provenance.Record | None
        The provenance record for the pipeline that generated the field,
        if applicable
    """

    def __init__(self, path, value=None, dtype=None,
                 frequency=DataFreq.session, array=None, subject_id=None,
                 visit_id=None, dataset=None, exists=True, record=None):
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
        FieldMixin.__init__(self, path, dtype, frequency, array)
        DataItem.__init__(self, subject_id, visit_id, dataset,
                          exists, record)
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
