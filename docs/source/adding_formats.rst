.. _adding_formats:

New formats and spaces
======================

Arcana was initially developed for medical-imaging analysis. Therefore, with
the notable exceptions of the generic data spaces and file-formats defined in
:mod:`arcana.core.standard`, the
majority of file-formats and data spaces are specific to medical imaging.
However, new formats and data spaces used in other fields can be implemented as
required with just a few lines of code.

.. _file_formats:

File formats
------------

File formats are specified using the FileFormats_ package. Please refer to its documentation
on how to add new file formats


Data spaces
-----------

New data spaces (see :ref:`data_spaces`) are defined by extending the
:class:`.DataSpace` abstract base class. :class:`.DataSpace` subclasses are be
`enums <https://docs.python.org/3/library/enum.html>`_ with binary string
values of consistent length (i.e. all of length 2 or all of length 3, etc...).
The length of the binary string defines the rank of the data space,
i.e. the maximum depth of a data tree within the space. The enum must contain
members for each permutation of the bit string (e.g. for 2 dimensions, there
must be members corresponding to the values 0b00, 0b01, 0b10, 0b11).

For example, in imaging studies scannings sessions are typically organised
by analysis group (e.g. test & control), membership within the group (i.e
matched subject ID) and time-points for longitudinal studies. In this case, we can
visualise the imaging sessions arranged in a 3-D grid along the `group`, `member`, and
`timepoint` axes. Note that datasets that only contain one group or
time-point can still be represented in this space, and just be singleton along
the corresponding axis.

All axes should be included as members of a DataSpace subclass
enum with orthogonal binary vector values, e.g.::

    member = 0b001
    group = 0b010
    timepoint = 0b100

The axis that is most often non-singleton should be given the smallest bit
as this will be assumed to be the default when there is only one layer in the
data tree, e.g. imaging datasets will not always have different groups or
time-points but will always have different members (which are equivalent to
subjects when there is only one group).

The "leaf rows" of a data tree, imaging sessions in this example, will be the
bitwise-and of the dimension vectors, i.e. an imaging session
is uniquely defined by its member, group and timepoint ID.::

    session = 0b111

In addition to the data items stored in leaf rows, some data, particularly
derivatives, may be stored in the dataset along a particular dimension, at
a lower "row_frequency" than 'per session'. For example, brain templates are
sometimes calculated 'per group'. Additionally, data
can also be stored in aggregated rows that across a plane
of the grid. These frequencies should also be added to the enum, i.e. all
permutations of the base dimensions must be included and given intuitive
names if possible::

    subject = 0b011 - uniquely identified subject within in the dataset.
    batch = 0b110 - separate group + timepoint combinations
    matchedpoint = 0b101 - matched members and time-points aggregated across groups

Finally, for items that are singular across the whole dataset there should
also be a dataset-wide member with value=0::

    dataset = 0b000

For example, if you wanted to analyse daily recordings from various
weather stations you could define a 2-dimensional "Weather" data space with
axes for the date and weather station of the recordings, with the following code

.. _weather_example:

.. code-block:: python

    from arcana.core.data.space import DataSpace

    class Weather(DataSpace):

        # Define the axes of the dataspace
        timepoint = 0b01
        station = 0b10

        # Name the leaf and root frequencies of the data space
        recording = 0b11
        dataset = 0b00

.. note::

    All permutations of *N*-D binary strings need to be named within the enum.

.. _Pydra: http://pydra.readthedocs.io
.. _FileFormats: https://arcanaframework.github.io/fileformats
