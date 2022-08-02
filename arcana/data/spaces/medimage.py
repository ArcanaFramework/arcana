from arcana.core.data.space import DataSpace


class Clinical(DataSpace):
    """
    An enum that specifies the data hierarchy of data trees typical of
    medimage research, i.e. subjects split into groups scanned at different
    timepoints (in longitudinal studies).
    """

    # Root row of the dataset
    dataset = 0b000  # singular within the dataset

    # Axes of the data space
    member = 0b001  # subjects relative to their group membership, i.e.
    # matched pairs of test and control subjects should share
    # the same member IDs.
    group = 0b010  # subject groups (e.g. test & control)
    timepoint = 0b100  # timepoints in longitudinal studies

    # Combinations
    session = 0b111  # a single session (i.e. a single timepoint of a subject)
    subject = 0b011  # uniquely identified subject within in the dataset.
    # As opposed to 'member', which specifies a subject in
    # relation to its group (i.e. one subject for each member
    # in each group). For datasets with only one study group,
    # then subject and member are equivalent
    batch = 0b110  # data from separate groups at separate timepoints
    matchedpoint = 0b101  # matched members (e.g. test & control) across
    # all groups and timepoints
