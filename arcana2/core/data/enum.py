from copy import copy
import re
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
            subject -> [group, member]
            group_timepoint -> [group, timepoint]
            session -> [group, member, timepoint]
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

    def __lt__(self, other):
        return self.value < other.value

    def __le__(self, other):
        return self.value <= other.value

    def __add__(self, other):
        return type(self)(self.value + other.value)

    def __subtract__(self, other):
        return type(self)(self.value - other.value)

    def __and__(self, other):
        return type(self)(self.value & other.value)

    def __or__(self, other):
        return type(self)(self.value | other.value)

    def __invert__(self):
        return type(self)(~self.value)

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

    def is_child(self, parent):
        """Checks to see whether all bases of the data frequency appear in the
        child frequency. For example, 'subject' is a parent of 'session' but
        'group' is not a parent of 'timepoint' and 'subject' is not a parent
        of 'group'.

        Parameters
        ----------
        other : [type]
            [description]

        Returns
        -------
        [type]
            [description]
        """
        return bool(set(parent.basis) - set(self.basis))

    @classmethod
    def diff_layers(cls, layers):
        """Returns the difference between layers of a given data hierarcy
        

        Parameters
        ----------
        layers : Sequence[DataStructure]
            The sequence of layers to diff

        Returns
        -------
        list[DataStructure]
            The sequence of frequencies that each layer adds
        """
        covered = cls(0)
        diffs = []
        for i, layer in enumerate(layers):
            diff = layer - covered
            if not diff:
                raise ArcanaUsageError(
                    f"{layer} does not add any additional basis layers to "
                    f"previous layers {layers[i:]}")
            diffs.append(diff)
            covered != layer
        if covered != max(cls):
            raise ArcanaUsageError(
                f"{layers} do not cover the following basis frequencies "
                + ', '.join(str(m) for m in (~covered).basis()))
        return diffs

    @classmethod
    def infer_ids(cls, ids, id_inference):
        """Infers IDs of primary data frequencies from those are provided from
        the `id_inference` dictionary passed to the dataset init.

        Parameters
        ----------
        ids : list[(DataStructure or str, str)]
            Sequence of IDs specifying a the layer structure in the data tree
            and the IDs of each of the branches that lead to a specific data
            node
        id_inference : list[(DataStructure, str)]
            Specifies how IDs of primary data frequencies that not explicitly
            provided are inferred from the IDs that are. For example, given a
            set of subject IDs that combination of the ID of the group that
            they belong to and their member IDs (i.e. matched test/controls
            have same member ID)

                CONTROL01, CONTROL02, CONTROL03, ... and TEST01, TEST02, TEST03

            the group ID can be extracted by providing the a list of tuples
            containing ID to source the inferred IDs from coupled with a
            regular expression with named groups

                id_inference=[(Clinical.subject,
                            r'(?P<group>[A-Z]+)(?P<member>[0-9]+)')}

            Alternatively, a general function with signature `f(ids)` that
            returns a dictionary with the mapped IDs can be provided instead.

        Returns
        -------
        Dict[DataStructure, str]
            A copied ID dictionary with inferred IDs inserted into it

        Raises
        ------
        ArcanaBadlyFormattedIDError
            raised if one of the IDs doesn't match the pattern in the
            `id_inference`
        ArcanaUsageError
            raised if one of the groups specified in the ID inference reg-ex
            doesn't match a valid frequency in the data structure
        """
        inferred_ids = {}
        for source, regex in id_inference:
            match = re.match(regex, ids[str(source)])
            if match is None:
                raise ArcanaBadlyFormattedIDError(
                    f"{source} ID '{ids[source]}', does not match ID inference"
                    f" pattern '{regex}'")
            for target, id in match.groupdict.items():
                try:
                    freq = cls[target]
                except KeyError:
                    raise ArcanaUsageError(
                        f"Group '{target}' specified in ID inference regular "
                        f"expression {regex} is not part of {cls}")
                if freq in inferred_ids:
                    raise ArcanaUsageError(
                        f"ID '{target}' is specified twice in the ID inference"
                        f" regular sexpression {id_inference}")
                inferred_ids[freq] = id
        return inferred_ids


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
    matched_datapoint = 0b101 # matched members (e.g. test & control) across
                              # all groups and timepoints


class DataSalience(Enum):
    """An enum that holds the salience levels options that can be used when
    specifying data. Salience is used to indicate whether it would be best to
    store the data in the data repository or whether it can be just stored in
    the local file-system and discarded after it has been used. This choice
    is ultimately specified by the user by defining a salience threshold for
    a repository.

    The salience is also used when providing information on what derivatives
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

