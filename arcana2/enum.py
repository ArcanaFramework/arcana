from enum import Enum


class DataFreq(Enum):
    """
    The frequency at which a data item is stored within a data tree. For
    typical neuroimaging analysis these levels are hardcoded to one of six
    values.
    """

    group = 0b100  #for each subject group
    subject = 0b010  # for each subject (NB: implies group iteration)
    visit = 0b001  # for each visit (e.g. longitudinal timepoint)
    session = 0b011  # for each session (i.e. a single visit of a subject)
    group_visit = 0b101  # for each combination of subject group and visit
    dataset = 0b000  # singular within the dataset

    # The value for each enum is a binary string that specifies layers the
    # frequency sits below in the data tree. Each bit corresponds to a layer,
    # e.g. 'group', 'subject' and 'visit', and if they are present in the
    # binary string it signifies that the data is specific to a particular
    # branch at that layer (i.e. specific group, subject or visit). 
    # 
    # Note that patterns 0b110 and 0b111 are not explicitly included because
    # subject specificity implies group specificity

    def __str__(self):
        return self.name

    def basis_layers(self):
        """Returns the layers in the data tree above the given layer, e.g.

            group_visit -> group + visit

        Note that subjects are below the group layer as each subject always
        belongs to one group (even if there is only 1 group in the dataset), i.e.

            subject -> group
            session -> group + subject + visit

        """
        val = self.value
        if val & self.subject:
            val += 0b100  # Subject-specificity implies group-specificity
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