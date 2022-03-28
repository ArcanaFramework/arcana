.. _adding_formats:

New formats and spaces
======================

Arcana was initially developed for medical-imaging analysis. Therefore, with
the notable exceptions of the generic data spaces and file-formats defined in
:mod:`arcana.data.spaces.common` and :mod:`arcana.data.formats.common`, the
majority of file-formats and data spaces are specific to medical imaging.
However, new formats and data spaces used in other fields can be implemented as
required with just a few lines of code.


.. _file_formats:

File formats
------------

File formats are defined by subclasses of the :class:`.FileGroup` base class.
"File group" is a catch-all term that encompasses three sub-types, each with
their own :class:`.FileGroup` subclass:

* :class:`.BaseFile` - single files
* :class:`.BaseFileWithSidecars` - files with side car files (e.g. separate headers)
* :class:`.BaseDirectory` - directories with specific contents

New format classes should extend one of these classes or an existing file
format class (or both) as they include methods to interact with the data
store. Note that :class:`.BaseFile` is a base class of :class:`.BaseFileWithSidecars`
so multiple inheritance is possible where a format with side cars inherits from
the same format without side-cars (e.g. Nifti -> NiftiX), but in this case
ensure that :class:`.BaseFileWithSidecars` appears before the other class to be
extended in the bases list, e.g. ``NiftiX(BaseFileWithSidecars, Nifti)``.

:class:`.File` subclasses typically only need to set an ``ext`` attribute
to the extension string used to identify the type of file, e.g.

.. code-block:: python

    from arcana.core.data.format import File

    class Json(BaseFile):
        ext = 'json'

If the file format doesn't have an identifiable extension it is possible to
override the :meth:`File.from_paths` method and peak inside the contents of the
file to determine its type, but this shouldn't be necessary in most cases.

:class:`.BaseFileWithSidecars` subclasses can set the ``ext`` and ``side_car_exts``
attributes. The ``side_car_exts`` attribute is a tuple of the side cars extensions
that should be present alongside the "primary file",

.. code-block:: python

    from arcana.core.data.format import FileWithSidecars

    class Analyze(BaseFileWithSidecars):
        ext = 'img'
        side_car_exts = ('hdr',)

:class:`.BaseDirectory` subclasses can set ``ext`` but will typically only set
the ``content_types`` attribute. The ``content_types`` attribute is a tuple of
the file formats that are expected within the directory. The list is not
exclusive, so stray files inside the directory will not effect its
identification.


.. code-block:: python

    from arcana.core.data.format import BaseDirectory, BaseFile
    
    class DicomFile(BaseFile):
        ext = 'dcm'

    class Dicom(BaseDirectory):
        content_types = (DicomFile,)

It is a good idea to make use of class inheritance when defining related
formats to capture the relationship between them. For example, adding a format
to handle the Siemens-variant DICOM format which has '.IMA' extensions.

.. code-block:: python

    class SiemensDicomFile(DicomFile):
        ext = 'IMA'

    class SiemensDicom(Dicom):
        content_types = (SiemensDicomFile,)

Defining hierarchical relationships between file formats is most useful when
defining implicit converters between file formats. This is done by adding
classmethods to the file format class decorated by :func:`arcana.core.mark.converter`.
The decorator specifies the format the converter method can specify the
the conversion *from* into the current class. The converter method adds Pydra_
nodes to a pipeline argument to perform

The first argument for converter methods should be the fs_path followed by
any side cars as keyword arguments. Converter methods should return the Pydra_
that performs the conversion followed by a lazy field that points to the
``fs_path`` of the converted file-group. If the format to convert to has side
cars, then the method should return the task followed by a tuple consisting of
lazy fields that point to the ``fs_path`` and then side-car files in the
converted file group in the order they appear in ``side_car_exts``.

.. code-block:: python

    from pydra.engine.core import Workflow, LazyField
    from pydra.tasks.dcm2niix import Dcm2niix
    from pydra.tasks.mrtrix3.utils import MRConvert
    from arcana.core.mark import converter

    class Nifti(BaseFile):
        ext = 'nii'

        @classmethod
        @converter(Dicom)
        def dcm2niix(cls, fs_path: LazyField):
            node = Dcm2niix(
                name=node_name,
                in_file=dicom,
                compress='n')
            return node, node.lzout.out_file

        @classmethod
        @converter(Analyze)
        def mrconvert(cls, fs_path: LazyField, hdr: LazyField):
            node = MRConvert(
                name=node_name,
                in_file=analyze,
                out_file='out.' + cls.ext)
            return node, node.lzout.out_file

If the class to convert to is a :class:`.BaseFileWithSidecars` subclass then the return value
should be a tuple consisting the primary path followed by side-car paths in the
same order they are defined in the class. To remove a converter in a specialised
subclass (which the converter isn't able to convert to) simply override the
converter method with an arbitrary value.


.. code-block:: python

    class NiftiX(BaseFileWithSidecars, Nifti):
        ext = 'nii'
        side_car_exts = ('json',)

        @classmethod
        @converter(Dicom)
        def dcm2niix(cls, fs_path: LazyField):
            node, out_file = super().dcm2niix(fs_path)
            return node, (out_file, node.lzout.out_json)

        mrconvert = None  # Only dcm2niix produces the required JSON files for NiftiX


Use dummy base classes in order to avoid circular reference issues when defining
two-way conversions between formats


.. code-block:: python

    class ExampleFormat2Base(BaseFile):
        pass

    class ExampleFormat1(BaseFile):
        ext = 'exm1'

        @classmethod
        @converter(ExampleFormat2Base)
        def from_example1(cls, fs_path: LazyField):
            node = Converter2to1(
                in_file=example1)
            return node, node.lzout.out_file

    class ExampleFormat2(ExampleFormat2Base):
        ext = 'exm2'

        @classmethod
        @converter(ExampleFormat1)
        def from_example1(cls, pipeline: Pipeline, node_name: str, example1: LazyField):
            node = Converter1to2(
                in_file=example1)
            return node, node.lzout.out_file

While not necessary, it can be convenient to add methods for accessing
file-group data within Python. This makes it possible to write generic methods
to generate publication outputs. Some suggested methods are

* ``data`` - access data array, particuarly relevant for imaging data
* ``metadata`` - access a dictionary containing metadata extracted from a header or side-car


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