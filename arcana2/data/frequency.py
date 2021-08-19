from enum import Enum


class DataFrequency(Enum):
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
            subject -> [group, member]
            group_timepoint -> [group, timepoint]
            session -> [group, member, timepoint]
        """
        val = self.value
        # Check which bits are '1', and append them to the list of levels
        cls = type(self)
        return [cls(b) for b in sorted(self._nonzero_bits(val))]

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

    def __lt__(self, other):
        return self.value < other.value

    def __le__(self, other):
        return self.value <= other.value

    def __add__(self, other):
        return type(self)(self.value + other.value)

    def __subtract__(self, other):
        return type(self)(self.value - other.value)

    @classmethod
    def default(cls):
        return max(cls)

    @classmethod
    def layers(cls):
        layer = cls(0)
        yield layer
        for b in max(cls).basis():
            layer += b.value
            yield layer


class Clinical(DataFrequency):
    """
    An enum that specifies the data frequencies within a data tree of a typical
    clinical research study with groups, subjects and timepoints.
    """

    # Root node of the dataset
    dataset = 0b000  # singular within the dataset

    # Primary "layers" of the data tree structure
    group = 0b100  # subject groups
    member = 0b010  # subjects relative to their group membership, i.e.
                    # matched pairs of test and control subjects will share the
                    # same member IDs
    timepoint = 0b001  # time-points in longitudinal studies

    # Commonly used combinations
    subject = 0b110 # uniquely identified subject within in the dataset.
                    # As opposed to 'member', which specifies a subject in
                    # relation to its group (i.e. one subject for each member
                    # in each group). For datasets with only one study group,
                    # then subject and member are equivalent
    session = 0b111  # a single session (i.e. a single timepoint of a subject)

    # Lesser used combinations
    group_timepoint = 0b101  # iterate over group and timepoints, i.e.
                             # matched members are combined 
    member_timepoint = 0b011 # iterate over each matched member and timepoint
                             # combination, groups are combined
