.. _adding_formats:

New formats and spaces
======================

Arcana 

File formats
------------

All file formats inherit from the ``FileGroup`` class.


Data spaces
-----------

New data spaces (see :ref:`data_spaces`) are defined by extending the
:class:`.DataSpace` abstract base class. :class:`.DataSpace` subclasses are be
`enums <https://docs.python.org/3/library/enum.html>`_ with binary string
values of consistent length (i.e. all of length 2 or all of length 3, etc...).
The length of the binary string defines the number of "dimensions" the space has,
i.e. the maximum depth of a data tree within the space. The enum must contain
members for each permutation of the bit string (e.g. for 2 dimensions, there
must be members corresponding to the values 0b00, 0b01, 0b10, 0b11).

For example, in imaging studies scannings sessions are typically organised
by analysis group (e.g. test & control), membership within the group (i.e
matched subject ID) and time-points for longitudinal studies. In this case, we can
visualise the imaging sessions arranged in a 3-D grid along the `group`, `member`, and
`timepoint` dimensions. Note that datasets that only contain one group or
time-point can still be represented in this space, and just be singleton along
the corresponding dimensions.

All dimensions should be included as members of a DataSpace subclass
enum as orthogonal binary vectors, e.g.::

    member = 0b001
    group = 0b010
    timepoint = 0b100

The most commonly present dimension should be given the smallest bit
as this will be assumed to be the default when there is only one layer in the
data tree, e.g. imaging datasets will not always have different groups or
time-points but will always have different members (which are equivalent to
subjects when there is only one group).

The "leaf nodes" of a data tree, imaging sessions in this example, will be the
bitwise-and of the dimension vectors, i.e. an imaging session
is uniquely defined by its member, group and timepoint ID.::
    
    session = 0b111
    
In addition to the data items stored in leaf nodes, some data, particularly
derivatives, may be stored in the dataset along a particular dimension, at
a lower "frequency" than 'per session'. For example, brain templates are
sometimes calculated 'per group'. Additionally, data
can also be stored in aggregated nodes that across a plane
of the grid. These frequencies should also be added to the enum, i.e. all
permutations of the base dimensions must be included and given intuitive
names if possible::

    subject = 0b011 - uniquely identified subject within in the dataset.
    batch = 0b110 - separate group+timepoint combinations
    matchedpoint = 0b101 - matched members and time-points aggregated across groups

Finally, for items that are singular across the whole dataset there should
also be a dataset-wide member with value=0::

    dataset = 0b000
