import os
import os.path as op
from itertools import chain
import hashlib
from arcana2.utils import split_extension, parse_value
from arcana2.exceptions import (
    ArcanaError, ArcanaFileFormatError, ArcanaUsageError, ArcanaNameError,
    ArcanaDataNotDerivedYetError, ArcanaUriAlreadySetException)
from .file_format import FileFormat
from .base import FileGroupMixin, FieldMixin

HASH_CHUNK_SIZE = 2 ** 20  # 1MB


class DataItem(object):

    is_spec = False

    def __init__(self, subject_id, visit_id, dataset, from_analysis,
                 exists, record):
        self._subject_id = subject_id
        self._visit_id = visit_id
        self._dataset = dataset
        self._from_analysis = from_analysis
        self._exists = exists
        self._record = record

    def __eq__(self, other):
        return (self.subject_id == other.subject_id
                and self.visit_id == other.visit_id
                and self.from_analysis == other.from_analysis
                and self.exists == other.exists
                and self._record == other._record)

    def __hash__(self):
        return (hash(self.subject_id)
                ^ hash(self.visit_id)
                ^ hash(self.from_analysis)
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
        if self.from_analysis != other.from_analysis:
            mismatch += ('\n{}from_analysis: self={} v other={}'
                         .format(sub_indent, self.from_analysis,
                                 other.from_analysis))
        if self.exists != other.exists:
            mismatch += ('\n{}exists: self={} v other={}'
                         .format(sub_indent, self.exists,
                                 other.exists))
        if self._record != other._record:
            mismatch += ('\n{}_record: self={} v other={}'
                         .format(sub_indent, self._record,
                                 other._record))
        return mismatch

    @property
    def derived(self):
        return self.from_analysis is not None

    @property
    def dataset(self):
        return self._dataset

    @property
    def exists(self):
        return self._exists

    @property
    def subject_id(self):
        return self._subject_id

    @property
    def visit_id(self):
        return self._visit_id

    @property
    def session_id(self):
        return (self.subject_id, self.visit_id)

    @property
    def from_analysis(self):
        return self._from_analysis

    @property
    def record(self):
        return self._record

    @record.setter
    def record(self, record):
        if self.name not in record.outputs:
            raise ArcanaNameError(
                self.name,
                "{} was not found in outputs {} of provenance record {}"
                .format(self.name, record.outputs.keys(), record))
        self._record = record

    @property
    def recorded_checksums(self):
        if self.record is None:
            return None
        else:
            return self._record.outputs[self.name]

    def initkwargs(self):
        dct = super().initkwargs()
        dct['dataset'] = self.dataset
        dct['subject_id'] = self.subject_id
        dct['visit_id'] = self.visit_id
        dct['from_analysis'] = self._from_analysis
        return dct


class FileGroup(DataItem, FileGroupMixin):
    """
    A representation of a file_group within the dataset.

    Parameters
    ----------
    name : str
        The name of the file group within the data tree
    format : FileFormat
        The file format used to store the file_group.
    tree_level : TreeLevel
        The level within the dataset tree that the data items sit, i.e. 
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
    id : int | None
        The ID of the file_group in the session. To be used to
        distinguish multiple file_groups with the same scan type in the
        same session, e.g. scans taken before and after a task. For
        datasets where this isn't stored (i.e. Local), id can be None
    subject_id : int | str | None
        The id of the subject which the file_group belongs to
    visit_id : int | str | None
        The id of the visit which the file_group belongs to
    dataset : Repository
        The dataset which the file_group is stored
    from_analysis : str
        Name of the Arcana analysis that that generated the field
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
        data (i.e. XNAT) the name of the resource is to enable straightforward
        format identification
    potential_aux_files : list[str]
        A list of paths to potential files to include in the file_group as
        "side-cars" or headers or in a directory format. Used when the
        format of the file_group is not set when it is detected in the dataset
        but determined later from a list of candidates in the specification it
        is matched to.
    quality : str
        The quality label assigned to the file_group (e.g. as is saved on XNAT)
    """

    def __init__(self, name, format=None, tree_level='per_session',
                 path=None, aux_files=None, id=None, uri=None, subject_id=None,
                 visit_id=None, dataset=None, from_analysis=None,
                 exists=True, checksums=None, record=None, resource_name=None,
                 potential_aux_files=None, quality=None):
        FileGroupMixin.__init__(self, name=name, format=format,
                             tree_level=tree_level)
        DataItem.__init__(self, subject_id, visit_id, dataset,
                               from_analysis, exists, record)
        if aux_files is not None:
            if path is None:
                raise ArcanaUsageError(
                    "Side cars provided to '{}' file_group ({}) but not primary "
                    "path".format(self.name, aux_files))
            if format is None:
                raise ArcanaUsageError(
                    "Side cars provided to '{}' file_group ({}) but format is "
                    "not specified".format(self.name, aux_files))
        if path is not None:
            path = op.abspath(op.realpath(path))
            if aux_files is None:
                aux_files = {}
            elif set(aux_files.keys()) != set(self.format.aux_files.keys()):
                raise ArcanaUsageError(
                    "Provided side cars for '{}' but expected '{}'"
                    .format("', '".join(aux_files.keys()),
                            "', '".join(self.format.aux_files.keys())))
        self._path = path
        self._aux_files = aux_files if aux_files is not None else {}
        self._uri = uri
        self._id = id
        self._checksums = checksums
        self._resource_name = resource_name
        self._quality = quality
        if potential_aux_files is not None and format is not None:
            raise ArcanaUsageError(
                "Potential paths should only be provided to FileGroup.__init__ "
                "({}) when the format of the file_group ({}) is not determined"
                .format(self.name, format))
        if potential_aux_files is not None:
            potential_aux_files = list(potential_aux_files)
        self._potential_aux_files = potential_aux_files

    def __getattr__(self, attr):
        """
        For the convenience of being able to make calls on a file_group that are
        dependent on its format, e.g.

            >>> file_group = FileGroup('a_name', format=AnImageFormat())
            >>> file_group.get_header()

        we capture missing attributes and attempt to redirect them to methods
        of the format class that take the file_group as the first argument
        """
        try:
            frmt = self.__dict__['_format']
        except KeyError:
            frmt = None
        else:
            try:
                format_attr = getattr(frmt, attr)
            except AttributeError:
                pass
            else:
                if callable(format_attr):
                    return lambda *args, **kwargs: format_attr(self, *args,
                                                               **kwargs)
        raise AttributeError("FileGroups of '{}' format don't have a '{}' "
                             "attribute".format(frmt, attr))

    def __eq__(self, other):
        eq = (FileGroupMixin.__eq__(self, other)
              and DataItem.__eq__(self, other)
              and self._aux_files == other._aux_files
              and self._id == other._id
              and self._checksums == other._checksums
              and self._resource_name == other._resource_name
              and self._quality == other._quality)
        # Avoid having to cache file_group in order to test equality unless they
        # are already both cached
        try:
            if self._path is not None and other._path is not None:
                eq &= (self._path == other._path)
        except AttributeError:
            return False
        return eq

    def __hash__(self):
        return (FileGroupMixin.__hash__(self)
                ^ DataItem.__hash__(self)
                ^ hash(self._id)
                ^ hash(tuple(sorted(self._aux_files.items())))
                ^ hash(self._checksums)
                ^ hash(self._resource_name)
                ^ hash(self._quality))

    def __lt__(self, other):
        if isinstance(self.id, int) and isinstance(other.id, str):
            return True
        elif isinstance(self.id, str) and isinstance(other.id, int):
            return False
        else:
            if self.id == other.id:
                # If ids are equal order depending on analysis name
                # with acquired (from_analysis==None) coming first
                if self.from_analysis is None:
                    return other.from_analysis is not None
                elif other.from_analysis is None:
                    return False
                elif self.from_analysis == other.from_analysis:
                    if self.format_name is None:
                        return other.format_name is not None
                    elif other.format_name is None:
                        return False
                    else:
                        return self.format_name < other.format_name
                else:
                    return self.from_analysis < other.from_analysis
            else:
                return self.id < other.id

    def __repr__(self):
        return ("{}('{}', {}, '{}', subj={}, vis={}, stdy={}{}, exists={}, "
                "quality={}{})"
                .format(
                    type(self).__name__, self.name, self.format,
                    self.tree_level, self.subject_id,
                    self.visit_id, self.from_analysis,
                    (", resource_name='{}'".format(self._resource_name)
                     if self._resource_name is not None else ''),
                    self.exists, self.quality,
                    (", path='{}'".format(self.path)
                     if self._path is not None else '')))

    def find_mismatch(self, other, indent=''):
        mismatch = FileGroupMixin.find_mismatch(self, other, indent)
        mismatch += DataItem.find_mismatch(self, other, indent)
        sub_indent = indent + '  '
        if self._path != other._path:
            mismatch += ('\n{}path: self={} v other={}'
                         .format(sub_indent, self._path,
                                 other._path))
        if self._id != other._id:
            mismatch += ('\n{}id: self={} v other={}'
                         .format(sub_indent, self._id,
                                 other._id))
        if self._checksums != other._checksums:
            mismatch += ('\n{}checksum: self={} v other={}'
                         .format(sub_indent, self.checksums,
                                 other.checksums))
        if self._resource_name != other._resource_name:
            mismatch += ('\n{}format_name: self={} v other={}'
                         .format(sub_indent, self._resource_name,
                                 other._resource_name))
        if self._quality != other._quality:
            mismatch += ('\n{}format_name: self={} v other={}'
                         .format(sub_indent, self._quality,
                                 other._quality))
        return mismatch

    @property
    def path(self):
        if not self.exists:
            raise ArcanaDataNotDerivedYetError(
                self.name,
                "Cannot access path of {} as it hasn't been derived yet"
                .format(self))
        if self._path is None:
            if self.dataset is not None:
                self.get()  # Retrieve from dataset
            else:
                raise ArcanaError(
                    "Neither path nor dataset has been set for FileGroup("
                    "'{}')".format(self.name))
        return self._path

    def set_path(self, path, aux_files=None):
        if path is not None:
            path = op.abspath(op.realpath(path))
            self._exists = True
        self._path = path
        if aux_files is None:
            self._aux_files = dict(self.format.default_aux_file_paths(path))
        else:
            if set(self.format.aux_files.keys()) != set(aux_files.keys()):
                raise ArcanaUsageError(
                    "Keys of provided side cars ('{}') don't match format "
                    "('{}')".format("', '".join(aux_files.keys()),
                                    "', '".join(self.format.aux_files.keys())))
            self._aux_files = aux_files
        self._checksums = self.calculate_checksums()
        self.put()  # Push to dataset

    @path.setter
    def path(self, path):
        self.set_path(path, aux_files=None)

    @property
    def paths(self):
        """Iterates through all files in the set"""
        if self.format is None:
            raise ArcanaFileFormatError(
                "Cannot get paths of file_group ({}) that hasn't had its format "
                "set".format(self))
        if self.format.directory:
            return chain(*((op.join(root, f) for f in files)
                           for root, _, files in os.walk(self.path)))
        else:
            return chain([self.path], self.aux_files.values())

    @property
    def format(self):
        return self._format

    @property
    def quality(self):
        return self._quality

    @format.setter
    def format(self, format):
        assert isinstance(format, FileFormat)
        self._format = format
        if format.aux_files and self._path is not None:
            self._aux_files = format.assort_files(
                [self._path] + list(self._potential_aux_files))[1]
        if self._id is None and hasattr(format, 'extract_id'):
            self._id = format.extract_id(self)
        # No longer need to retain potentials after we have assigned the real
        # auxiliaries
        self._potential_aux_files = None

    @property
    def fname(self):
        if self.format is None:
            raise ArcanaFileFormatError(
                "Need to provide format before accessing the filename of {}"
                .format(self))
        return self.name + self.format.ext_str

    @property
    def basename(self):
        return self.name

    @property
    def id(self):
        if self._id is None:
            return self.basename
        else:
            return self._id

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
        return self._aux_files[name]

    @property
    def aux_files(self):
        return self._aux_files

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
    def resource_name(self):
        return self._resource_name

    @property
    def checksums(self):
        if not self.exists:
            raise ArcanaDataNotDerivedYetError(
                self.name,
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
    def from_path(cls, path, **kwargs):
        if not op.exists(path):
            raise ArcanaUsageError(
                "Attempting to read FileGroup from path '{}' but it "
                "does not exist".format(path))
        if op.isdir(path):
            name = op.basename(path)
        else:
            name = split_extension(op.basename(path))[0]
        return cls(name, path=path, **kwargs)

    def detect_format(self, candidates):
        """
        Detects the format of the file_group from a list of possible
        candidates. If multiple candidates match the potential files, e.g.
        NiFTI-X (see dcm2niix) and NiFTI, then the first matching candidate is
        selected.

        If a 'format_name' was specified when the file_group was
        created then that is used to select between the candidates. Otherwise
        the file extensions of the primary path and potential auxiliary files,
        or extensions of the files within the directory for directories are
        matched against those specified for the file formats

        Parameters
        ----------
        candidates : FileFormat
            A list of file-formats to select from.
        """
        if self._format is not None:
            raise ArcanaFileFormatError(
                "Format has already been set for {}".format(self))
        matches = [c for c in candidates if c.matches(self)]
        if not matches:
            raise ArcanaFileFormatError(
                "None of the candidate file formats ({}) match {}"
                .format(', '.join(str(c) for c in candidates), self))
        return matches[0]

    def initkwargs(self):
        dct = FileGroupMixin.initkwargs(self)
        dct.update(DataItem.initkwargs(self))
        dct['path'] = self.path
        dct['id'] = self.id
        dct['uri'] = self.uri
        dct['bids_attr'] = self.bids_attr
        dct['checksums'] = self.checksums
        dct['resource_name'] = self._resource_name
        dct['potential_aux_files'] = self._potential_aux_files
        dct['quality'] = self._quality
        return dct

    def get(self):
        if self.dataset is not None:
            self._exists = True
            self._path, self._aux_files = self.dataset.get_file_group(self)

    def put(self):
        if self.dataset is not None and self._path is not None:
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
    name : str
        The name of the file_group
    dtype : type
        The datatype of the value. Can be one of (float, int, str)
    tree_level : TreeLevel
        The level within the dataset tree that the data items sit, i.e. 
        per 'session', 'subject', 'visit', 'group_visit', 'group' or 'dataset'
    derived : bool
        Whether or not the value belongs in the derived session or not
    subject_id : int | str | None
        The id of the subject which the field belongs to
    visit_id : int | str | None
        The id of the visit which the field belongs to
    dataset : Repository
        The dataset which the field is stored
    from_analysis : str
        Name of the Arcana analysis that that generated the field
    exists : bool
        Whether the field exists or is just a placeholder for a derivative
    record : ..provenance.Record | None
        The provenance record for the pipeline that generated the field,
        if applicable
    """

    def __init__(self, name, value=None, dtype=None,
                 tree_level='per_session', array=None, subject_id=None,
                 visit_id=None, dataset=None, from_analysis=None,
                 exists=True, record=None):
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
                        "an array ({})".format(name, value))
                array = True
            else:
                if array:
                    raise ArcanaUsageError(
                        "Non-array value ({}) passed to '{}', which expects "
                        "array{}".format(value, name,
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
        FieldMixin.__init__(self, name, dtype, tree_level, array)
        DataItem.__init__(self, subject_id, visit_id, dataset,
                               from_analysis, exists, record)
        self._value = value

    def __eq__(self, other):
        return (FieldMixin.__eq__(self, other)
                and DataItem.__eq__(self, other)
                and self.value == other.value)

    def __hash__(self):
        return (FieldMixin.__hash__(self)
                ^ DataItem.__hash__(self)
                ^ hash(self.value))

    def find_mismatch(self, other, indent=''):
        mismatch = FieldMixin.find_mismatch(self, other, indent)
        mismatch += DataItem.find_mismatch(self, other, indent)
        sub_indent = indent + '  '
        if self.value != other.value:
            mismatch += ('\n{}value: self={} v other={}'
                         .format(sub_indent, self.value,
                                 other.value))
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
        if self.name == other.name:
            # If ids are equal order depending on analysis name
            # with acquired (from_analysis==None) coming first
            if self.from_analysis is None:
                return other.from_analysis is None
            elif other.from_analysis is None:
                return False
            else:
                return self.from_analysis < other.from_analysis
        else:
            return self.name < other.name

    def __repr__(self):
        return ("{}('{}',{} '{}', subj={}, vis={}, stdy={}, exists={})"
                .format(
                    type(self).__name__, self.name,
                    (" {},".format(self._value)
                     if self._value is not None else ''),
                    self.tree_level, self.subject_id,
                    self.visit_id, self.from_analysis,
                    self.exists))

    @property
    def value(self):
        if not self.exists:
            raise ArcanaDataNotDerivedYetError(
                self.name,
                "Cannot access value of {} as it hasn't been "
                "derived yet".format(repr(self)))
        if self._value is None:
            if self.dataset is not None:
                self._value = self.dataset.get_field(self)
            else:
                raise ArcanaError(
                    "Neither value nor dataset has been set for Field("
                    "'{}')".format(self.name))
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
