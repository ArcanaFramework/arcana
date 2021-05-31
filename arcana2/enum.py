from enum import Enum


class DataFrequency(Enum):
    """
    Base class for tree structure enums. The values for each member of the 
    enum should be a binary string that specifies the relationship between
    the different "data frequencies" present in the dataset.

    Frequencies that have only one non-zero bit in their binary values
    correspond to a layer in the data tree (e.g. subject, timepoint).
    frequency sits below in the data tree. Each bit corresponds to a layer,
    e.g. 'group', 'subject' and 'visit', and if they are present in the
    binary string it signifies that the data is specific to a particular
    branch at that layer (i.e. specific group, subject or visit). 
    """

    def __str__(self):
        return self.name

    def layers(self):
        """Returns the layers in the data tree the frequency consists of, e.g.

            dataset -> 
            subject -> subject
            group_subject -> group + subject
            session -> group + subject + visit
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


class ClinicalStudy(DataFrequency):
    """
    An enum that specifies the data frequencies within a typical clinical
    study data tree with groups, subjects and timepoints.
    """

    group = 0b100  #for each subject group
    subject = 0b010  # for each subject within group, i.e., can be multiple
                     # subject IDs in a dataset, e.g., 01 could be for
                     # both test-01 & control-01
    visit = 0b001  # for each visit (e.g. longitudinal timepoint)

    participant = 0b110 # combination of group and subject, i.e. uniquely
                        # identifies a participant in the dataset as opposed
                        # to 'subject', which has matches between groups
    session = 0b111  # for each session (i.e. a single visit of a subject)
    dataset = 0b000  # singular within the dataset

    # Lesser used combinations
    group_visit = 0b101  # combination of group and visit across all subjects
    subject_visit = 0b011 # combination of subject and visit across all groups,
                          # could be useful for matched control/test studies


class Salience(Enum):
    """An enum that holds the salience levels options that can be used when
    specifying a file-group or field. Salience is used to indicate whether
    it would be best to store the file-group or field a data repository
    or whether it can be just stored in the local cache and discarded after it
    has been used. However, this is ultimately specified by the user and will
    be typically dependent on where in the development-cycle the pipeline that
    generates them is.

    The salience is also used when generating information on what derivatives
    are available
    """
    
    primary = (5, 'Primary input data or difficult to regenerate derivs. e.g. '
               'from scanner reconstruction')
    publication = (4, 'Results that would typically be used as main '
                   'outputs in publications')
    supporting = (3, 'Derivatives that would typically only be kept to support'
                  ' the main results')
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