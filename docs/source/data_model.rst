Data Model
==========

Arcana contains a rich and flexible object model to access and manipulate data
within data stores. While the framework hides most of the implementation details
from the user, it is important to be familiar with the general concepts. They
key base classes of the data model are:

* :class:`.DataStore` - abstraction of the storage system/format, which is sub-classed for different storage systems/formats
* :class:`.DataSpace` - defines the repetitive structure (or "data space") for a class of datasets, e.g. whether there are data points for each subject and time-point, or each weather-station/day
* :class:`.DataNode` - a set of data items at a common data point, e.g. imaging session
* :class:`.Dataset` - encapsulates a set of data items (files, fields or arrays) repeated across a range of data points (e.g. subjects)
* :class:`.DataItem` - a single measurement or acquisition, e.g. T1-weighted MRI scan, humidity measurement
* :class:`.DataColumn` - a single type of measurement repeated across the dataset

Stores
------

Support for different storage techniques is provided by sub-classes of the
:class:`.DataStore` class. :class:`.DataStore` classes not only encapsulate where the
data is stored, e.g. on local disk or remote repository, but also how the data
is accessed, e.g. whether it is in BIDS format, or whether to access files in
an XNAT repository directly (i.e. as exposed to the container service) or purely
using the API.

There are four currently implemented :class:`.DataStore` classes:

* :class:`.FileSystem` - access data organised within a arbitrary directory tree on the file system
* :class:`.BidsFormat` - access data on file systems organised in the `Brain Imaging Data Structure (BIDS) <https://bids.neuroimaging.io/>`__ format (neuroimaging-specific)
* :class:`.Xnat` - access data stored in XNAT_ repositories by its REST API
* :class:`.XnatViaCS` - access data stored in XNAT_ repositories as exposed to integrated pipelines run in XNAT_'s container service using a combination of direct access to the archive disk and the REST API

To configure access to a data store a via the API, initialise the :class:`.DataStore`
sub-class corresponding to the required data location/access-method then save
it to the YAML configuration file stored at `~/.arcana/stores.yml`.

.. code-block:: python

    import os
    from arcana.data.stores.xnat import Xnat

    # Initialise the data store object
    xnat_store = Xnat(
        server='https://central.xnat.org',
        user='user123',
        password=os.environ['XNAT_PASS'],
        cache_dir='/work/xnat-cache'
    )

    # Save it to the configuration file stored at '~/.arcana/stores.yml' with
    # the nickname 'xnat-central'
    xnat_store.save('xnat-central')

    # Reload store from configuration file
    reloaded = DataStore.load('xnat-central')


To configure access to a store via the CLI use the ``arcana store add`` sub-command

.. code-block:: bash

    $ arcana store add xnat xnat-central https://central.xnat.org \
      --user user123 --cache_dir /work/xnat-cache
    Password:


See also ``arcana store rename`` and ``arcana store remove``.

.. note::

    Data stores that don't require any parameters such as :class:`.FileSystem` and
    :class:`.BidsFormat` don't need to be configured and can be accessed via their aliases,
    ``file`` and ``bids`` when defining a dataset.

.. _data_dimensions:

Dataset dimensions
------------------

A key concept in Arcana's data model is that of "dataset dimensions".
This refers to the structure of repeated measurement events within
a class of datasets. Where a measurement event could be an MRI session in a
clinical trial or a football player's performance as part of a scouting analysis
for example.

Such measurements events can be categorised in a number of ways. Taking the clinical trial example,
each MRI session will belong to a particular subject and may also belong to
a longitudinal timepoint and/or a particular study group.
In the case of the scouting program, a player's performance will
belong to a particular player, team, match, league, season, field position,
and more. In Arcana, these category groups are considered to form the "dimensions"
of the dataset, drawing a loose analogy with a multi-dimensional space where
each measurement event exists as a point on a grid.

Different dataset dimensions are defined in Arcana by sub-classes of the
:class:`.DataSpace` enum. Enum members define both the "primary axes" of
the grid and also the combinations of these axes (planes/sub-spaces if you will)
that make up the possible "frequencies" data can occur at. For example,
the :class:`.ClinicalTrial` has the primary axes of ``group``, ``member`` and
``timepoint``, corresponding to the study group (e.g. 'test' or 'control'),
within-group ID (particularly relevant for matched controls otherwise just needs
to be unique), and longintudinal timepoint.

Note that a particular dataset can have a singleton along any dimension
(e.g. one study group or timepoint). Therefore, when designing analyses for a
particular it is better to include as
many possible axes 


Nodes
-----


which
branches across different *dimensions* of the data (e.g. over separate groups,
subjects or sessions), consisting of both source data (typically
acquired from an instrument)

for a class of datasets, e.g. whether there are data points for each subject and time-point, or each weather-station/day

Data items can exist at any *node* within the data tree, and along any
axis of the dataset even if it is not in the original tree, e.g. summary
statistics that are analysed across the combination of group and time-points
from a data tree organised by group> subject> session.

which should be set to a sub-class of :class:`.DataSpace` enum. By default, Arcana will assume 
:class:`.medicalimaging.ClinicalTrial` is applicable, which is able to
represents the typical structure of a longintudinal medicalimaging trial with multiple
groups, subjects and sessions at different time-points (noting that a dataset
can singletons nodes along a dimension, e.g. a single group or time-point).

Base class for all "data dimensions" enums. DataSpace enums specify
the relationships between nodes of a dataset.

For example in imaging studies, scannings sessions are typically organised
by analysis group (e.g. test & control), membership within the group (i.e
matched subjects) and time-points (for longitudinal studies). We can
visualise the nodes arranged in a 3-D grid along the `group`, `member`, and
`timepoint` dimensions. Note that datasets that only contain one group or
time-point can still be represented in the same space, and just be of
depth=1 along those dimensions.

All dimensions should be included as members of a DataSpace subclass
enum with orthogonal binary vector values, e.g.

    member = 0b001
    group = 0b010
    timepoint = 0b100

In this space, an imaging session node is uniquely defined by its member,
group and timepoint ID. The most commonly present dimension should be given
the least frequent bit (e.g. imaging datasets will not always have
different groups or time-points but will always have different members
(equivalent to subjects when there is one group).

In addition to the data items stored in the data nodes for each session,
some items only vary along a particular dimension of the grid. The
"frequency" of these nodes can be specified using the "basis" members
(i.e. member, group, timepoint) in contrast to the `session` frequency,
which is the combination of all three

    session = 0b111

Additionally, some data is stored in aggregated nodes that across a plane
of the grid. These frequencies should also be added to the enum (all
combinations of the basis frequencies must be included) and given intuitive
names if possible, e.g.

    subject = 0b011 - uniquely identified subject within in the dataset.
    batch = 0b110 - separate group+timepoint combinations
    matchedpoint = 0b101 - matched members and time-points aggregated across groups

Finally, for items that are singular across the whole dataset there should
also be a dataset-wide member with value=0:

    dataset = 0b000


Datasets
--------

In Arcana, a *dataset* refers to a collection of comparable data to be jointly
analysed (e.g. in a research study). Datasets contain both source data and
the derivatives generated from them. Datasets are typically organised into a
tree with a defined "hierarchy" of data frequencies. For example the following tree
structure has a hierarchy of "subjects" > "sessions"

.. code-block::

    my-dataset
    ├── subject1
    │   ├── session1
    │   │   ├── t1_mprage
    │   │   ├── t2_space
    │   │   └── bold_rest
    │   └── session2
    │       ├── t1_mprage
    │       ├── t2_space
    │       └── bold_rest
    ├── subject2
    │   ├── session1
    │   │   ├── t1_mprage
    │   │   ├── t2_space
    │   │   └── bold_rest
    │   └── session2
    │       ├── t1_mprage
    │       ├── t2_space
    │       └── bold_rest
    └── subject1
        ├── session1
        │   ├── t1_mprage
        │   ├── t2_space
        │   └── bold_rest
        └── session2
            ├── t1_mprage
            ├── t2_space
            └── bold_rest

Datasets can be defined via the API using the :meth:`.DataStore.dataset` method.
For example, to define a new dataset corresponding to the XNAT project ID
*MYXNATPROJECT*


.. code-block:: python

    xnat_dataset = xnat_store.dataset(id='MYXNATPROJECT')

For stores that can store arbitrary tree structures (e.g. file-system directories),
the hierarchy of the dataset tree needs to be provided (see :ref:`data_dimensions`).
This is specified by providing a list of data frequencies corresponding to
descending layers of the directory tree

.. code-block:: python

    from arcana.data.stores.file_system import FileSystem
    from arcana.data.spaces.medicalimaging import ClinicalTrial

    fs_dataset = FileSystem().dataset(
        id='/data/imaging/my-project',
        hierarchy=[ClinicalTrial.group, ClinicalTrial.subject])

These definitions can be saved inside the project directory and then reloaded
in new Python contexts.

.. code-block:: python

    fs_dataset.save()

    ...

    reloaded = FileSystem().load_dataset('/data/imaging/my-project')


For some datasets, especially in stores where the tree hierarchy is fixed (e.g. XNAT),
you may need to infer the ID(s) for one or more dimensions from the node labels
following a given naming convention. For example, given an
XNAT project where all the test subjects are numbered *TEST01*, *TEST02*, *TEST03*,...
and the matched control subjects are numbered *CON01*, *CON02*, *CON03*,...,
the group and matched "member" IDs need to be inferred from the subject ID.
This can be done by providing an ``id_inference`` argument which takes a list
of tuples, consisting of the dimension of the ID to infer from and a
regular-expression (Python syntax), with named groups corresponding to inferred
IDs.

.. code-block:: python

    xnat_dataset = xnat_store.dataset(
        id='MYXNATPROJECT',
        id_inference=[
            (ClinicalTrial.subject, r'(?P<group>[A-Z]+)(?P<member>\d+)')])


Often there are nodes that need to be omitted from a given analysis due to
missing or corrupted data. Such nodes can be excluded with the
``excluded`` argument, which takes a dictionary mapping the data
dimension to the list of IDs to exclude.

You can exclude nodes at different levels of data tree, even within in the same dataset.
Note however, that if you exclude nodes low level of the dataset's hierarchy then
corresponding nodes at higher levels will also be excluded. For example,
if you exclude the imaging session for subject 5 at Timepoint 2, then both
Timepoint 2 (for all subjects) and Subject 5 (at all timepoints) will be
dropped from the analysis. Therefore it is typically better to exclude nodes
higher up the tree (e.g. Subject 5).

.. code-block:: python

    fs_dataset = FileSystem().dataset(
        id='/data/imaging/my-project',
        excluded={ClinicalTrial.subject: ['09', '11']})  # Alternatively use 'subject' string instead of enum


The ``included`` argument is the inverse of exclude and can be more convenient when
you only want to select a small sample. ``included`` can be used in conjunction
with ``excluded`` the frequencies must be orthogonal.

.. code-block:: python

    fs_dataset = FileSystem().dataset(
        id='/data/imaging/my-project',
        excluded={ClinicalTrial.subject: ['09', '11']},
        included={Clincial.timepoint: ['T1']})


You may want multiple dataset definitions for a given project/directory,
for different analysese.g. with different subsets of IDs depending on which
scans have passed quality control. To avoid conflicts, you can
assign a dataset definition a name, which is used differentiate between multiple
dataset definitions stored in the same project/directory. To do this simply
provide the ``name`` parameter to the :meth:`.Dataset.save` and
:meth:`.DataStore.load_dataset` methods.

.. code-block:: python

    xnat_dataset.save('passed_dwi_qc')

    dwi_dataset = xnat_store.load_dataset('MYXNATPROJECT', 'passed_dwi_qc')


Datasets can also be defined and saved via the CLI using the ``arcana dataset define``
command. The store the dataset belongs to is prepended to the project ID
separated by '//', e.g.

.. code-block:: bash

    $ arcana dataset define 'xnat-central//MYXNATPROJECT' \
      --excluded subject sub09,sub11 --included timepoint T1 \
      --id_inference subject '(?P<group>[A-Z]+)_(?P<member>\d+)'

To give the dataset definition a name, append the name to the dataset's ID
string separated by ':', e.g.

.. code-block:: bash

    $ arcana dataset define 'file///data/imaging/my-project:training' group subject \
      --include subject 10:20


Fields, file-groups and formats
-------------------------------

:class:`.DataItem` objects are atomic elements in Arcana datasets, and can be either
*fields* (int, float, str or bool), *array fields* (sequence[int or float or str or bool])
or *file groups* (single files, files + header/side-cars or directories).
Data items act as pointers to the data associated provenance in the
dataset and provide methods for pulling and pushing data to the store.

Arcana implicitly handles conversions between different file formats

:class:`.FileGroup` sub-classes may contain methods for accessing the file data and header metadata,
which can be useful in selecting from a collection of acquired data and exploration
of the data.

<explain how to reference them from the command line>

.. warning::
    Under construction


.. _data_columns:

Columns
-------

Matching items across a dataset (e.g. all subject ages or all 'T1-weighted MRI
images') are referred collectively as *columns*, loosely analogous to its use
in tabular datasets, such as those used by Excel and Pandas. However, unlike in tabular
formats, items in data columns in Arcana occur at different *frequencies*,
e.g. 'age values occur per subject and T1-weighted images occur per session.
When specifying a column, the datatype of the items in the column needs to be specified. 

Before data can be accessed or new data appended to a dataset, columns need to be
added. There are two types of columns *sources* and *sinks*. Source columns
select corresponding items from existing data in the dataset using a range of
possible criteria: path (can be a regular-expression), data type, frequency,
quality threshold (an XNAT feature), order within node and header values.
Sink columns define how new data will be written to the dataset.

Columns are given a name, which is used to access them and map the
inputs/outputs of pipelines onto. By default, this name is used by sinks to
name the output fields/files stored in the dataset. However, if a specific
output path is required it can be specified by the ``path`` argument.

Use the :meth:`.Dataset.add_source` and :meth:`.Dataset.add_sink` methods to add
sources and sinks via the API.

.. code-block:: python

    from arcana.data.spaces.medicalimaging import ClinicalTrial
    from arcana.data.types.medicalimaging import dicom, nifti_gz

    xnat_dataset.add_source(
        name='T1w',
        path=r'.*t1_mprage.*'
        datatype=dicom,
        order=1,
        quality_threshold='usable',
        is_regex=True
    )

    fs_dataset.add_sink(
        name='brain_template',
        datatype=nifti_gz,
        frequency=ClinicalTrial.group
    )

To access the data in the columns once they are defined use the ``Dataset[]``
operator

.. code-block:: python

    import matplotlib.pyplot as plt
    from arcana.core.data.store import Dataset

    # Get a column containing all T1-weighted MRI images across the dataset
    xnat_dataset = Dataset.load('xnat-central//MYXNATPROJECT')
    t1w = xnat_dataset['T1w']

    # Plot a slice of the image data from a sample image (Note: such data access
    # is only available for select data types that have convenient Python readers)
    plt.imshow(t1w['sub01_tpoint2'].data[:,:,30])


Use the ``arcana source add`` and ``arcana sink add`` commands to add sources/sinks
to a dataset using the CLI.

.. code-block:: bash

    $ arcana source add 'xnat-central//MYXNATPROJECT' T1w \
      medicalimaging:dicom --path '.*t1_mprage.*' \
      --order 1 --quality usable --regex

    $ arcana sink add 'file///data/imaging/my-project:training' brain_template \
      medicalimaging:nifti_gz --frequency group


One of the main benefits of using datasets in BIDS_ format is that the names
and file formats of the data are strictly defined. This allows the :class:`.BidsFormat`
data store object to automatically add sources to the dataset when it is
initialised.

.. code-block:: python

    from arcana.data.stores.bids import BidsFormat
    from arcana.data.stores.file_system import FileSystem
    from arcana.data.spaces.medicalimaging import ClinicalTrial

    bids_dataset = BidsFormat().dataset(
        id='/data/openneuro/ds00014')

    print(bids_dataset['T1w']['sub01'].header['dim'])

.. _Arcana: https://arcana.readthedocs.io
.. _XNAT: https://xnat.org
.. _BIDS: https://bids.neuroimaging.io