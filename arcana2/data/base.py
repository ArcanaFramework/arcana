from enum import Enum
from .file_format import FileFormat
from copy import copy
from logging import getLogger
from arcana2.exceptions import ArcanaUsageError
from future.types import newstr
from arcana2.utils import PATH_SUFFIX, FIELD_SUFFIX, CHECKSUM_SUFFIX
from arcana2.enum import DataFreq
logger = getLogger('arcana')
    

class DataMixin():
    """Base class for all Data related classes
    """

    def __init__(self, path, frequency):
        self.path = path
        self.frequency = frequency

    def __eq__(self, other):
        return (self.frequency == other.frequency
                and self.path == other.path)

    def __hash__(self):
        return hash(self.frequency) ^ hash(self.path)

    def find_mismatch(self, other, indent=''):
        if self != other:
            mismatch = "\n{}{} != {}".format(indent,
                                             type(self).__name__,
                                             type(other).__name__)
        else:
            mismatch = ''
        sub_indent = indent + '  '
        if self.path != other.path:
            mismatch += ('\n{}path: self={} v other={}'
                         .format(sub_indent, self.path,
                                 other.path))
        if self.frequency != other.frequency:
            mismatch += ('\n{}frequency: self={} v other={}'
                         .format(sub_indent, self.frequency,
                                 other.frequency))
        return mismatch

    def __ne__(self, other):
        return not (self == other)

    def initkwargs(self):
        return {'path': self.path,
                'frequency': self.frequency}


class FileGroupMixin(DataMixin):
    f"""
    An abstract base class representing either an acquired file_group or the
    specification for a derived file_group.

    Parameters
    ----------
    path : str
        The path to the file-group excluding any parts that are specific to its
        location within in a data tree (i.e. no subject or session ID
        information). May contain namespaces (separated by forward slashes) to
        logically separate related derivatives and avoid clashes,
        e.g. 'anat/T1w', 'func/bold' or 'freesurfer/recon-all'
    format : FileFormat
        The file format used to store the file_group. Can be one of the
        recognised formats
    frequency : DataFreq
        The level within the dataset tree that the file group sits, i.e. 
        per 'session', 'subject', 'visit', 'group_visit', 'group' or 'dataset'
    """

    is_file_group = True

    def __init__(self, path, format, frequency):
        super().__init__(path, frequency)
        self.format = format

    def __eq__(self, other):
        return (super().__eq__(other) and self.format == other.format)

    def __hash__(self):
        return (super().__hash__() ^ hash(self.format))

    def find_mismatch(self, other, indent=''):
        mismatch = super().find_mismatch(other, indent)
        sub_indent = indent + '  '
        if self.format != other.format:
            mismatch += ('\n{}format: self={} v other={}'
                         .format(sub_indent, self.format,
                                 other.format))
        return mismatch

    def initkwargs(self):
        dct = super().initkwargs()
        dct['format'] = self.format
        return dct


class FieldMixin(DataMixin):
    """
    An abstract base class representing either an acquired value or the
    specification for a derived value.

    Parameters
    ----------
    path : str
        The path to the field excluding any parts that are specific to its
        location within in a data tree (i.e. no subject or session ID
        information). May contain namespaces (separated by forward slashes) to
        logically separate related derivatives, e.g. 'myanalysis/mymetric'
    dtype : type
        The datatype of the value. Can be one of (float, int, str)
    frequency : DataFreq
        The level within the dataset tree that the field sits, i.e. 
        per 'session', 'subject', 'visit', 'group_visit', 'group' or 'dataset'
    array : bool
        Whether the field contains scalar or array data
    """

    is_field = True

    dtypes = (int, float, str)

    def __init__(self, path, dtype, frequency, array):
        super().__init__(path, frequency)
        if dtype not in self.dtypes + (newstr, None):
            raise ArcanaUsageError(
                "Invalid dtype {}, can be one of {}".format(
                    dtype, ', '.join((d.__name__ for d in self.dtypes))))
        self.dtype = dtype
        self.array = array

    def __eq__(self, other):
        return (super().__eq__(other) and
                self.dtype == other.dtype and
                self.array == other.array)

    def __hash__(self):
        return (super().__hash__() ^ hash(self.dtype) ^ hash(self.array))

    def __repr__(self):
        return ("{}(dtype={}, frequency='{}', array={})"
                .format(self.__class__.__name__, self.dtype,
                        self.frequency, self.array))

    def find_mismatch(self, other, indent=''):
        mismatch = super(FieldMixin, self).find_mismatch(other, indent)
        sub_indent = indent + '  '
        if self.dtype != other.dtype:
            mismatch += ('\n{}dtype: self={} v other={}'
                         .format(sub_indent, self.dtype,
                                 other.dtype))
        if self.array != other.array:
            mismatch += ('\n{}array: self={} v other={}'
                         .format(sub_indent, self.array,
                                 other.array))
        return mismatch

    def initkwargs(self):
        dct = super(FieldMixin, self).initkwargs()
        dct['dtype'] = self.dtype
        dct['array'] = self.array
        return dct
