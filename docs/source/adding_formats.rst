.. _adding_formats:

New formats and spaces
======================

Arcana was initially developed for medicalimaging analysis, so with the exception
of the generic data spaces and file-formats defined in
:mod:`arcana.data.spaces.common` and :mod:`arcana.data.formats.common`, the
majority of file-formats and data spaces are specific to medical imaging.
However, it is straightforward to add new formats and data spaces as required
in order to use Arcana on data from other fields.


File formats
------------

File formats are defined by subclasses of the :class:`.FileGroup` base class.
"File group" is a catch-all term that encompasses three sub-types, each with
their own :class:`.FileGroup` subclass:

* :class:`.File` - single files
* :class:`.FileWithSidecars` - files with headers or side cars
* :class:`.Directory` - directories with arbitrary contents

New format classes should extend one of these classes or an existing file
format class (or both) as they include methods to interact with the data
store.

.. note:: 
    :class:`.File` is a base class of :class:`.FileWithSidecars` so multiple
    inheritance is possible where a format with side cars inherits from the
    same format without side-cars without messing up the MRO for the data
    handling calls.


:class:`.File` subclasses typically only need to set an ``ext`` attribute
to the extension used to identify the type of file.

.. code-block:: python

    from arcana.core.data.format import File

    class Json(File):

        ext = '.json'


If the file format doesn't have an identifiable extension it is possible to
override the :meth:`File.from_paths` method and peak inside the contents of the
file, but this shouldn't be necessary in most cases.

:class:`.FileWithSidecars` subclasses typically set the ``ext`` and ``side_cars``
attributes. The ``side_cars`` attribute is a tuple of the side cars extensions
in the file-group

.. code-block:: python

    from arcana.core.data.format import FileWithSidecars

    class Analyze(FileWithSidecars):

        ext = '.img'
        side_cars = ('hdr',)


:class:`.Directory` subclasses can set ``ext`` but will typically only set
the ``contents`` attribute. The ``contents`` attribute is a tuple of the
file-groups that are expected within the directory. The list is not exclusive
so other files if other files are present within the directory, it will still
match the format.

.. code-block:: python

    from arcana.core.data.format import Directory
    from arcana.data.formats.medicalimaging import Dicom

    class DicomDir(Directory):

        contents = (Dicom,)


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
    batch = 0b110 - separate group + timepoint combinations
    matchedpoint = 0b101 - matched members and time-points aggregated across groups

Finally, for items that are singular across the whole dataset there should
also be a dataset-wide member with value=0::

    dataset = 0b000
