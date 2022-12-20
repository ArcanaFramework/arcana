import typing as ty
import re
from enum import Enum
from arcana.core.utils.serialize import ClassResolver
from arcana.core.utils.misc import classproperty


class DataSpace(Enum):
    """
    Base class for all "data space" enums. DataSpace enums specify
    the relationships between rows of a dataset.

    For example in imaging studies, scannings sessions are typically organised
    by analysis group (e.g. test & control), membership within the group (i.e
    matched subjects) and time-points (for longitudinal studies). We can
    visualise the rows arranged in a 3-D grid along the `group`, `member`, and
    `timepoint` dimensions. Note that datasets that only contain one group or
    time-point can still be represented in the same space, and just be of
    depth=1 along those dimensions.

    All dimensions should be included as members of a DataSpace subclass
    enum with orthogonal binary vector values, e.g.

        member = 0b001
        group = 0b010
        timepoint = 0b100

    In this space, an imaging session row is uniquely defined by its member,
    group and timepoint ID. The most commonly present dimension should be given
    the least frequent bit (e.g. imaging datasets will not always have
    different groups or time-points but will always have different members
    (equivalent to subjects when there is one group).

    In addition to the data items stored in the data rows for each session,
    some items only vary along a particular dimension of the grid. The
    "row_frequency" of these rows can be specified using the "basis" members
    (i.e. member, group, timepoint) in contrast to the `session` row_frequency,
    which is the combination of all three

        session = 0b111

    Additionally, some data is stored in aggregated rows that across a plane
    of the grid. These frequencies should also be added to the enum (all
    combinations of the basis frequencies must be included) and given intuitive
    names if possible, e.g.

        subject = 0b011 - uniquely identified subject within in the dataset.
        batch = 0b110 - separate group+timepoint combinations
        matchedpoint = 0b101 - matched members and time-points aggregated across groups

    Finally, for items that are singular across the whole dataset there should
    also be a dataset-wide member with value=0:

        dataset = 0b000
    """

    def __str__(self):
        return self.name

    @classmethod
    def leaf(cls):
        return max(cls)

    @classmethod
    def axes(cls):
        return cls.leaf().span()

    def span(self):
        """Returns the basis dimensions in the data tree that the given
        enum-member projects into.

        For example in `Clinical` data trees, the following frequencies can
        be decomposed into the following basis dims:

            dataset -> []
            group -> [group]
            member -> [member]
            timepoint -> [timepoint]
            subject -> [group, member]
            batch -> [timepoint, group]
            matchedpoint -> [timepoint, member]
            session -> [timepoint, group, member]
        """
        # Check which bits are '1', and append them to the list of levels
        cls = type(self)
        return [cls(b) for b in sorted(self.nonzero_bits(), reverse=True)]

    def nonzero_bits(self):
        v = self.value
        nonzero = []
        while v:
            w = v & (v - 1)
            nonzero.append(w ^ v)
            v = w
        return nonzero

    def __iter__(self):
        "Iterate over bit string"
        bit = (max(type(self)).value + 1) >> 1
        while bit > 0:
            yield bool(self.value & bit)
            bit >>= 1

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

    def __hash__(self):
        return self.value

    def __bool__(self):
        return bool(self.value)

    def bin(self):
        return bin(self.value)

    @classmethod
    def union(cls, freqs: ty.Sequence[Enum]):
        "Returns the union between data row_frequency values"
        union = cls(0)
        for f in freqs:
            union |= f
        return union

    @classmethod
    def default(cls):
        return max(cls)

    def is_parent(self, child, if_match=False):
        """Checks to see whether the current row_frequency is a "parent" of the
        other data row_frequency, i.e. all the base row_frequency of self appear in
        the "child".

        Parameters
        ----------
        child : DataSpace
            The data row_frequency to check parent/child relationship with
        if_match : bool
            Treat matching frequencies as "parents" of each other

        Returns
        -------
        bool
            True if self is parent of child
        """
        return ((self & child) == self) and (child != self or if_match)

    def tostr(self):
        return f"{ClassResolver.tostr(self, strip_prefix=False)}[{str(self)}]"

    @classmethod
    def fromstr(cls, s):
        match = re.match(r"(.*)\[([^\]]+)\]", s)
        if match is None:
            raise ValueError(
                f"'{s}' is not a string of the format <data-space-enum>[<value>]"
            )
        class_loc, val = match.groups()
        space = ClassResolver(cls)(class_loc)
        return space[val] if not isinstance(space, str) else s

    @classproperty
    def SUBPACKAGE(cls):
        """Cannot be a regular class attribute because then DataSpace won't be able to
        be extended"""
        return "data"
