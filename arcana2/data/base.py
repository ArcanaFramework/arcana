from past.builtins import basestring
from builtins import object
from abc import ABCMeta
from .file_format import FileFormat
from copy import copy
from logging import getLogger
from arcana2.exceptions import ArcanaError
from future.types import newstr
from arcana2.utils import PATH_SUFFIX, FIELD_SUFFIX, CHECKSUM_SUFFIX
logger = getLogger('arcana')


class BaseData(object, metaclass=ABCMeta):

    VALID_FREQUENCIES = ('per_session', 'per_subject', 'per_visit',
                         'per_dataset')

    is_file_group = False
    is_field = False

    def __init__(self, name, frequency='per_session'):
        assert name is None or isinstance(name, basestring)
        if frequency not in self.VALID_FREQUENCIES:
            raise ArcanaError(
                "Unrecognised frequency '{}'".format(frequency))
        self._name = name
        self._frequency = frequency

    def __eq__(self, other):
        return (self.basename == other.basename and
                self.frequency == other.frequency)

    def __hash__(self):
        return hash(self.basename) ^ hash(self.frequency)

    def find_mismatch(self, other, indent=''):
        if self != other:
            mismatch = "\n{}{t}('{}') != {t}('{}')".format(
                indent, self.basename, other.basename,
                t=type(self).__name__)
        else:
            mismatch = ''
        sub_indent = indent + '  '
        if self.basename != other.basename:
            mismatch += ('\n{}name: self={} v other={}'
                         .format(sub_indent, self.name, other.name))
        if self.frequency != other.frequency:
            mismatch += ('\n{}frequency: self={} v other={}'
                         .format(sub_indent, self.frequency,
                                 other.frequency))
        return mismatch

    def __lt__(self, other):
        return self.name < other.name

    def __ne__(self, other):
        return not (self == other)

    @property
    def name(self):
        return self._name

    @property
    def basename(self):
        return self._name

    @property
    def frequency(self):
        return self._frequency

    def renamed(self, name):
        """
        Duplicate the datum and rename it
        """
        duplicate = copy(self)
        duplicate._name = name
        return duplicate

    def initkwargs(self):
        return {'name': self.name,
                'frequency': self.frequency}


class BaseFileGroup(BaseData, metaclass=ABCMeta):
    """
    An abstract base class representing either an acquired file_group or the
    specification for a derived file_group.

    Parameters
    ----------
    name : str
        The name of the file_group
    format : FileFormat
        The file format used to store the file_group. Can be one of the
        recognised formats
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_dataset',
        specifying whether the file_group is present for each session, subject,
        visit or project.
    bids_attr : BidsAttr
        A collection of BIDS attributes for the file_group or spec
    """

    is_file_group = True

    def __init__(self, name, format=None, frequency='per_session'):
        super(BaseFileGroup, self).__init__(name=name, frequency=frequency)
        assert format is None or isinstance(format, FileFormat)
        self._format = format

    def __eq__(self, other):
        return (super(BaseFileGroup, self).__eq__(other) and
                self._format == other._format)

    def __hash__(self):
        return (super(BaseFileGroup, self).__hash__() ^
                hash(self._format))

    def find_mismatch(self, other, indent=''):
        mismatch = super(BaseFileGroup, self).find_mismatch(other, indent)
        sub_indent = indent + '  '
        if self.format != other.format:
            mismatch += ('\n{}format: self={} v other={}'
                         .format(sub_indent, self.format,
                                 other.format))
        return mismatch

    @property
    def format(self):
        return self._format

    def __repr__(self):
        return ("{}(name='{}', format={}, frequency='{}')"
                .format(self.__class__.__name__, self.name, self.format,
                        self.frequency))

    def initkwargs(self):
        dct = super(BaseFileGroup, self).initkwargs()
        dct['format'] = self.format
        return dct

    @property
    def suffixed_name(self):
        return self._name + PATH_SUFFIX

    @property
    def checksum_suffixed_name(self):
        return self._name + CHECKSUM_SUFFIX


class BaseField(BaseData, metaclass=ABCMeta):
    """
    An abstract base class representing either an acquired value or the
    specification for a derived value.

    Parameters
    ----------
    name : str
        The name of the file_group
    dtype : type
        The datatype of the value. Can be one of (float, int, str)
    frequency : str
        One of 'per_session', 'per_subject', 'per_visit' and 'per_dataset',
        specifying whether the file_group is present for each session, subject,
        visit or project.
    array : bool
        Whether the field contains scalar or array data
    """

    is_field = True

    dtypes = (int, float, str)

    def __init__(self, name, dtype, frequency, array=False):
        super(BaseField, self).__init__(name, frequency)
        if dtype not in self.dtypes + (newstr, None):
            raise ArcanaError(
                "Invalid dtype {}, can be one of {}".format(
                    dtype, ', '.join(self._dtype_names())))
        self._dtype = dtype
        self._array = array

    def __eq__(self, other):
        return (super(BaseField, self).__eq__(other) and
                self.dtype == other.dtype and
                self.array == other.array)

    def __hash__(self):
        return (super(BaseField, self).__hash__() ^ hash(self.dtype) ^
                hash(self.array))

    def __repr__(self):
        return ("{}(name='{}', dtype={}, frequency='{}', array={})"
                .format(self.__class__.__name__, self.name, self.dtype,
                        self.frequency, self.array))

    def find_mismatch(self, other, indent=''):
        mismatch = super(BaseField, self).find_mismatch(other, indent)
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

    @property
    def dtype(self):
        return self._dtype

    @property
    def array(self):
        return self._array

    @classmethod
    def _dtype_names(cls):
        return (d.__name__ for d in cls.dtypes)

    def initkwargs(self):
        dct = super(BaseField, self).initkwargs()
        dct['dtype'] = self.dtype
        dct['array'] = self.array
        return dct

    @property
    def suffixed_name(self):
        return self._name + FIELD_SUFFIX

    @property
    def checksum_suffixed_name(self):
        return self.suffixed_name
