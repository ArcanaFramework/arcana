from copy import copy
import functools
import typing as ty
from operator import __or__
from enum import Enum
from arcana2.exceptions import ArcanaBadlyFormattedIDError, ArcanaUsageError


class DataStructure(Enum):
    """
    Base class for tree structure enums. The values for each member of the 
    enum should be a binary string that specifies the relationship between
    the different "data frequencies" present in the dataset.

    Frequencies that have only one non-zero bit in their binary values
    correspond to a layer in the data tree (e.g. subject, timepoint).
    frequency sits below in the data tree. Each bit corresponds to a layer,
    e.g. 'group', 'subject' and 'timepoint', and if they are present in the
    binary string it signifies that the data is specific to a particular
    branch at that layer (i.e. specific group, subject or timepoint). 
    """

    def __str__(self):
        return self.name

    def basis(self):
        """Returns the basis frequencies in the data tree.
        For example in `Clinical` data trees, the following frequencies can
        be decomposed into the following basis frequencies:

            dataset -> []
            group -> [group]
            member -> [member]
            timepoint -> [timepoint]
            subject -> [group, member]
            batch -> [timepoint, group]
            matchedpoint -> [timepoint, member]
            session -> [timepoint, group, member]
        """
        val = self.value
        # Check which bits are '1', and append them to the list of levels
        cls = type(self)
        return [cls(b) for b in sorted(self._nonzero_bits(val), reverse=True)]

    def _nonzero_bits(self, v=None):
        if v is None:
            v = self.value
        nonzero = []
        while v:
            w = v & (v - 1)
            nonzero.append(w ^ v)
            v = w
        return nonzero

    def is_basis(self):
        return len(self._nonzero_bits()) == 1

    def __eq__(self, other):
        return self.value == other.value

    def __lt__(self, other):
        return self.value < other.value

    def __le__(self, other):
        return self.value <= other.value

    def __xor__(self, other):
        return type(self)(self.value ^ other.value)

    def __and__(self, other):
        return type(self)(self.value & other.value)

    def __or__(self, other):
        return type(self)(self.value | other.value)

    def __invert__(self):
        return type(self)(~self.value)

    @classmethod
    def union(cls, freqs: ty.Sequence[Enum]):
        "Returns the union between data frequency values"
        return map(functools.partial(__or__, x=cls(0)), freqs)

    @classmethod
    def default(cls):
        return max(cls)

    # @classmethod
    # def layers(cls):
    #     layer = cls(0)
    #     yield layer
    #     for b in max(cls).basis():
    #         layer |= b
    #         yield layer

    def is_parent(self, child):
        """Checks to see whether the current frequency is a "parent" of the
        other data frequency, i.e. all the base frequency of self appear in
        the "child".

        Parameters
        ----------
        child : DataFrequency
            The data frequency to check parent/child relationship with

        Returns
        -------
        bool
            True if self is parent of child
        """
        return (self & child) == self and child != self


class Clinical(DataStructure):
    """
    An enum that specifies the data hierarcy of data trees typical of
    clinical research, i.e. subjects split into groups scanned at different
    timepoints (in longitudinal studies).
    """

    # Root node of the dataset
    dataset = 0b000  # singular within the dataset

    # Basis frequencies in the data tree structure
    member = 0b001  # subjects relative to their group membership, i.e.
                    # matched pairs of test and control subjects should share
                    # the same member IDs.
    group = 0b010  # subject groups (e.g. test & control)
    timepoint = 0b100  # timepoints in longitudinal studies

    # Combinations
    session = 0b111  # a single session (i.e. a single timepoint of a subject)
    subject = 0b011 # uniquely identified subject within in the dataset.
                    # As opposed to 'member', which specifies a subject in
                    # relation to its group (i.e. one subject for each member
                    # in each group). For datasets with only one study group,
                    # then subject and member are equivalent
    batch = 0b110  # data from separate groups at separate timepoints
    matchedpoint = 0b101 # matched members (e.g. test & control) across
                              # all groups and timepoints


class DataSalience(Enum):
    """An enum that holds the salience levels options that can be used when
    specifying data. Salience is used to indicate whether it would be best to
    store the data in the data repository or whether it can be just stored in
    the local file-system and discarded after it has been used. This choice
    is ultimately specified by the user by defining a salience threshold for
    a repository.

    The salience is also used when providing information on what sinks
    are available to avoid cluttering help menus
    """
    
    primary = (5, 'Primary input data, e.g. raw data or data reconstructed on '
               'the scanner')
    publication = (4, "Results that would typically be used as main outputs "
                   "in publications")
    supplementary = (3, 'Derivatives that would typically only be provided in '
                     'supplementary material')
    qa = (2, 'Derivatives that would typically be only kept for quality '
          'assurance of analysis workflows')
    debug = (1, 'Derivatives that would typically only need to be checked '
             'when debugging analysis workflows')
    temp = (0, "Data only temporarily stored to pass between pipelines")

    def __init__(self, level, desc):
        self.level = level
        self.desc = desc

    def __lt__(self, other):
        return self.level < other.level

    def __le__(self, other):
        return self.level <= other.level

    def __str__(self):
        return self.name

    
class DataQuality(Enum):
    """The quality of a data item. Can be manually specified or set by
    automatic quality control methods
    """
    
    usable = 100
    noisy = 75
    questionable = 50
    artefactual = 25
    unusable = 0

