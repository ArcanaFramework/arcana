from arcana.core.data.space import DataSpace


class Samples(DataSpace):
    """
    The most basic data space within only one dimension
    """

    # Root row of the dataset
    dataset = 0b0  # singular within the dataset

    # Axes of the data space
    sample = 0b1
