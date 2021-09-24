import typing as ty
from operator import __or__
from enum import Enum


class DataDimension(Enum):
    """
    Base class for all "data dimensions" enums. DataDimension enums specify
    the relationships between nodes of a dataset.

    For example in imaging studies, scannings sessions are typically organised
    by analysis group (e.g. test & control), membership within the group (i.e
    matched subjects) and time-points (for longitudinal studies). We can
    visualise the nodes arranged in a 3-D grid along the `group`, `member`, and
    `timepoint` dimensions. Note that datasets that only contain one group or
    time-point can still be represented in the same space, and just be of
    depth=1 along those dimensions.

    All dimensions should be included as members of a DataDimension subclass
    enum with orthogonal binary vector values, e.g.

        member = 0b001
        group = 0b010
        timepoint = 0b100

    In this space, an imaging session node is uniquely defined by its member,
    group and timepoint ID. The most commonly present dimension should be given
    the least frequent bit (e.g. imaging datasets will not always have
    different groups or time-points but will always have different members
    (equivalent to subjects when there is one group).
    
    In addition to the data items stored in the data nodes for each session,
    some items only vary along a particular dimension of the grid. The
    "frequency" of these nodes can be specified using the "basis" members
    (i.e. member, group, timepoint) in contrast to the `session` frequency,
    which is the combination of all three

        session = 0b111

    Additionally, some data is stored in aggregated nodes that across a plane
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
    def basis(cls):
        return max(cls).nonzero_basis()

    def nonzero_basis(self):
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

    def __add__(self, other):
        return type(self)(self.value + other.value)

    def __sub__(self, other):
        return type(self)(self.value - other.value)

    def __hash__(self):
        return self.value

    def __bool__(self):
        return bool(self.value)

    @classmethod
    def union(cls, freqs: ty.Sequence[Enum]):
        "Returns the union between data frequency values"
        union = cls(0)
        for f in freqs:
            union |= f
        return union

    @classmethod
    def default(cls):
        return max(cls)

    def is_parent(self, child, if_match=False):
        """Checks to see whether the current frequency is a "parent" of the
        other data frequency, i.e. all the base frequency of self appear in
        the "child".

        Parameters
        ----------
        child : DataDimension
            The data frequency to check parent/child relationship with
        if_match : bool
            Treat matching frequencies as "parents" of each other

        Returns
        -------
        bool
            True if self is parent of child
        """
        return ((self & child) == self) and (child != self or if_match)


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

    def __eq__(self, other):
        return self.value == other.value

    def __lt__(self, other):
        return self.value < other.value

    def __le__(self, other):
        return self.value <= other.value