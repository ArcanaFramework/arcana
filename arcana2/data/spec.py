import attr
from .enum import DataFrequency, DataSalience


@attr.s
class DataSpec():
    """
    A specification for a file group within a analysis to be derived from a
    processing pipeline.

    Parameters
    ----------
    path : str
        The path to the relative location the corresponding data items will be
        stored within the nodes of the data tree.
    format : FileFormat or type
        The file format used to store the file_group. Can be one of the
        recognised formats
    frequency : DataFrequency
        The frequency of the file-group within the dataset tree, e.g. per
        'session', 'subject', 'timepoint', 'group', 'dataset'
    desc : str
        Description of what the field represents
    salience : Salience
        The salience of the specified file-group, i.e. whether it would be
        typically of interest for publication outputs or whether it is just
        a temporary file in a workflow, and stages in between
    category : str
        A name for a category of file_group specs. Used improve human searching
        of available options
    converters : Dict[FileFormat or type, pydra.task]
        A dictionary of converter tasks that can be used to implicitly convert
        inputs supplied in alternative formats into the format specified by
        the specification.
    """

    path = attr.ib(type=str)
    frequency = attr.ib(type=DataFrequency)
    format = attr.ib()
    desc = attr.ib(type=str)
    salience = attr.ib(type=DataSalience, default=DataSalience.default)
    category = attr.ib(type=str)
    converters = attr.ib(factory=list)
