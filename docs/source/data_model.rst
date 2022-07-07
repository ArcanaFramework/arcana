Data model
==========

Arcana's data model sets out to bridge the gap between
the semi-structured data trees in which file-based data are typically stored in,
and the tabular data frames required for statistical analysis. Note that this
transformation is abstract, with the source data remaining within original data
tree, and generated derivatives stored alongside them.

The key classes of Arcana's data model are:

* :ref:`Stores` - encapsulation of tree-based file storage systems 
* :ref:`Datasets` - set of comparable measurement events (e.g. XNAT project or BIDS dataset containing MRI sessions)
* :ref:`Rows and Spaces` - definition of tabular structures for dataset classes
* :ref:`Columns` - the set of comparable elements across a dataset (e.g. T1-weighted MRI scans across every session, ages across all subjects)
* :ref:`Formats` - pointers to atomic elements of datasets (e.g. T1-weighted MRI scan, subject age), which specify the formats they are stored in


Stores
------

Support for different file storage systems (e.g. `XNAT <https://xnat.org>`, `BIDS <https://bids.neuroimaging.io>`)
is provided by sub-classes of the :class:`.DataStore` class. :class:`.DataStore`
classes not only encapsulate where the data are stored, e.g. on local disk or
remote repository, but also how the data are accessed, e.g. whether they are in
BIDS format, or whether files in an XNAT
repository can be accessed directly (i.e. as exposed to the container service),
or purely using the API.

There are four :class:`.DataStore` classes currently implemented (for
instructions on how to add support for new systems see :ref:`alternative_stores`):

* :class:`.FileSystem` - access data organised within an arbitrary directory tree on the file system
* :class:`.Bids` - access data on file systems organised in the `Brain Imaging Data Structure (BIDS) <https://bids.neuroimaging.io/>`__ format (neuroimaging-specific)
* :class:`.Xnat` - access data stored in XNAT_ repositories by its REST API
* :class:`.XnatViaCS` - access data stored in XNAT_ repositories as exposed to integrated pipelines run in `XNAT's container service <https://wiki.xnat.org/container-service/using-the-container-service-122978908.html>`_ using a combination of direct access to the archive disk and the REST API

To configure access to a data store a via the API, initialise the :class:`.DataStore`
sub-class corresponding to the required data location/access-method then save
it to the YAML configuration file stored at `~/.arcana/stores.yaml`.

.. code-block:: python

    import os
    from arcana.data.stores.medimage import Xnat

    # Initialise the data store object
    xnat_store = Xnat(
        server='https://central.xnat.org',
        user='user123',
        password=os.environ['XNAT_PASS'],
        cache_dir='/work/xnat-cache'
    )

    # Save it to the configuration file stored at '~/.arcana/stores.yaml' with
    # the nickname 'xnat-central'
    xnat_store.save('xnat-central')

    # Reload store from configuration file
    reloaded = DataStore.load('xnat-central')


To configure access to a store via the CLI use the ``arcana store add`` sub-command

.. code-block:: console

    $ arcana store add xnat xnat-central https://central.xnat.org \
      --user user123 --cache_dir /work/xnat-cache
    Password:


See also ``arcana store rename``, ``arcana store remove`` and ``arcana store ls``.

.. note::

    Data stores that don't require any parameters such as :class:`.FileSystem` and
    :class:`.Bids` don't need to be configured and can be accessed via their aliases,
    ``file`` and ``bids`` when defining a dataset.

.. _datasets::

Datasets
--------

In Arcana, a *dataset* refers to a collection of comparable data to be
analysed (e.g. data from a single research study, or large collection such as the
Human Connectome Project). Arcana datasets consist of both source data and the
derivatives derived from them. Datasets are organised into a tree with a
consistent "hierarchy" that classify a series of measurement events
(e.g. groups, subjects, sessions). For example, the following dataset consisting
of imaging sessions sorted by subject and longintudinal timepoint within a
directory tree

.. code-block::

    my-dataset
    ├── subject1
    │   ├── timepoint1
    │   │   ├── t1w_mprage
    │   │   ├── t2w_space
    │   │   └── bold_rest
    │   └── timepoint2
    │       ├── t1w_mprage
    │       ├── t2w_space
    │       └── bold_rest
    ├── subject2
    │   ├── timepoint1
    │   │   ├── t1w_mprage
    │   │   ├── t2w_space
    │   │   └── bold_rest
    │   └── timepoint2
    │       ├── t1w_mprage
    │       ├── t2w_space
    │       └── bold_rest
    └── subject3
        ├── timepoint1
        │   ├── t1w_mprage
        │   ├── t2w_space
        │   └── bold_rest
        └── timepoint2
            ├── t1w_mprage
            ├── t2w_space
            └── bold_rest

The leaf directories of the directory tree contain data from "session"
measurement events, as designated by the combination of one of the three
subject IDs and the two timepoint IDs.

While the majority of data items are stored in the "leaves" of the tree (e.g. per-session),
data can exist for any repeating element (e.g. per-subject, per-timepoint),
whether it fits into the originanl hierarchy of the dataset or not. For example, statistics
derived across all subjects at each longitudinal timepoint in the above example
will be saved in the "TIMEPOINT" of the root directory, and subject-specific
data will be stored in "SUBJECT" sub-directories under each subject directory.

.. code-block::

    my-dataset
    ├── TIMEPOINT
    │   ├── timepoint1
    │   │   └── avg_connectivity
    │   └── timepoint2
    │       └── avg_connectivity
    ├── subject1    
    │   ├── SUBJECT
    │   │   └── geneomics.dat
    │   ├── timepoint1
    │   │   ├── t1w_mprage
    │   │   ├── t2w_space
    │   │   └── bold_rest
    │   └── timepoint2
    │       ├── t1w_mprage
    │       ├── t2w_space
    │       └── bold_rest
    ├── subject2
    │   ├── SUBJECT
    │   │   └── geneomics.dat    
    │   ├── timepoint1
    │   │   ├── t1w_mprage
    │   │   ├── t2w_space
    │   │   └── bold_rest
    │   └── timepoint2
    │       ├── t1w_mprage
    │       ├── t2w_space
    │       └── bold_rest
    └── subject3
        ├── SUBJECT
        │   └── geneomics.dat
        ├── timepoint1
        │   ├── t1w_mprage
        │   ├── t2w_space
        │   └── bold_rest
        └── timepoint2
            ├── t1w_mprage
            ├── t2w_space
            └── bold_rest


Datasets can be defined via the API using the :meth:`.DataStore.dataset` method.
For example, to define a new dataset corresponding to the XNAT project ID
*MYXNATPROJECT*


.. code-block:: python

    xnat_dataset = xnat_store.dataset(id='MYXNATPROJECT')

For stores that support datasets with arbitrary tree structures (e.g. file-system directories),
the hierarchy of layers in the data tree needs to be provided (see :ref:`data_spaces`).

.. code-block:: python

    from arcana.data.stores.common import FileSystem
    from arcana.data.spaces.medimage import Clinical

    fs_dataset = FileSystem().dataset(
        id='/data/imaging/my-project',
        space=Clinical,
        hierarchy=['group', 'subject'])  # Members of Clinical data space

These definitions can be saved inside the project directory and then reloaded
in new Python contexts.

.. code-block:: python

    fs_dataset.save()

    ...

    reloaded = FileSystem().load_dataset('/data/imaging/my-project')


For datasets where the fundamental hierarchy of the storage system is fixed
(e.g. XNAT), you may need to infer abstract layers of the hierarchy from the labels
of the fixed layers following a naming convention. For example, given an
XNAT project where all the test subjects are numbered *TEST01*, *TEST02*, *TEST03*,...
and the matched control subjects are numbered *CON01*, *CON02*, *CON03*,...,
the IDs for each subject's group and "matched member" need to be inferred from the subject label.
This can be done by providing an ``id_inference`` argument which takes a list
of tuples, consisting of the layer to infer the ID from and a
regular-expression (Python syntax), with named groups corresponding to inferred
IDs.

    XNAT-PROJECT
    ├── TEST01
    │   └── TEST01_MR01
    │       ├── t1w_mprage
    │       └── t2w_space
    ├── TEST02
    │   └── TEST02_MR01
    │       ├── t1w_mprage
    │       └── t2w_space
    ├── CON01
    │   └── CON01_MR01
    │       ├── t1w_mprage
    │       └── t2w_space
    └── CON02
        └── CON02_MR01
            ├── t1w_mprage
            └── t2w_space
    

.. code-block:: python

    # NB: 'subject' instead of Clinical.subject can be used in this
    # example as the data-space defaults to Clinical for XNAT stores
    xnat_dataset = xnat_store.dataset(
        id='MYXNATPROJECT',
        id_inference=[
            ('session', r'(?P<group>[A-Z]+)(?P<member>\d+)_MR(?P<timepoint>\d+)')])   


Often there are sections of the tree that need to be omitted from a given
analysis due to missing or corrupted data. These sections can be excluded with
the ``exclude`` argument, which takes a dictionary mapping the data
dimension to the list of IDs to exclude. You can exclude at different levels of
the tree's hierarchy.

.. code-block:: python

    fs_dataset = FileSystem().dataset(
        id='/data/imaging/my-project',
        exclude={'subject': ['09', '11']})


The ``include`` argument is the inverse of exclude and can be more convenient when
you only want to select a small sample. ``include`` can be used in conjunction
with ``exclude`` but not for the same frequencies.

.. code-block:: python

    fs_dataset = FileSystem().dataset(
        id='/data/imaging/my-project',
        exclude={'subject': ['09', '11']},
        include={'timepoint': ['T1']})


You may want multiple dataset definitions for a given project/directory,
for different analyses e.g. with different subsets of IDs depending on which
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

.. code-block:: console

    $ arcana dataset define 'xnat-central//MYXNATPROJECT' \
      --exclude subject sub09,sub11 --include timepoint T1 \
      --id_inference subject '(?P<group>[A-Z]+)_(?P<member>\d+)'

To give the dataset definition a name, append the name to the dataset's ID
string separated by ':', e.g.

.. code-block:: console

    $ arcana dataset define 'file///data/imaging/my-project:training' \
      medimage:Clinical group subject \
      --include subject 10:20


.. _data_spaces:

Rows and Spaces
---------------

To map data trees onto tabular data frames, the nodes of the tree
(e.g. imaging sessions, subjects) need to unwrapped to form the rows of the
frame. The majority of items will be stored at the leaves of the tree (e.g.
imaging sessions), but items stored at different levels of the tree
will occur at a lower frequency, e.g. per-subject or per-timepoint.
Therefore, a single dataset actually maps onto multiple data frames of differing
"row frequencies". 

The number of possible row frequencies depends on the depth of the hierarchy of
the data tree. An item can be singular in any layer of the hierarchy,
therefore there are 2^N possible row frequencies for a data tree of depth N.
For example, trees with two layers, 'a' and 'b', have four possible row
frequencies, 'ab', 'a', 'b' and the dataset as a whole. 
In Arcana, this binary structure is refered as a "data space", drawing a
loose analogy with a Cartesian space of dimension N in which measurement events
occur 

Data spaces used to class different types of datasets, such as a collection of imaging
data collected for a clinical trial, or videos collected to assess
player performance for the scouting team of a football club for example.
In these examples, the measurements are classified in different ways.
Taking the clinical trial example, each MRI session will belong to a particular subject
and may also belong to a longitudinal timepoint and/or a particular study group.
In the case of the scouting program, a set of player performance metrics will
belong to a particular player, competition round, league, season and more.


Data spaces are defined by subclassing the :class:`.DataSpace` enums.
Enum members define both the axes of the space and all possible combinations
of these axes (subspaces to stretch the analogy if you will). For example, the :class:`.Clinical`
has the axes of **group**, **member** and **timepoint**, corresponding to the
study group (e.g. 'test' or 'control'), within-group ID (relevant for matched
control studies and arbitrary otherwise, equivalent to subject ID when there is
only on study group), and longintudinal timepoint. These dimensions can be
combined to give all the possible row frequencies of the dataset, i.e. (per):

* **group** (group)
* **member** (member)
* **timepoint** (timepoint)
* **session** (member + group + timepoint),
* **subject** (member + group)
* **batch** (group + timepoint)
* **matchedpoint** (member + timepoint)
* **dataset** ()

Note that a particular dataset can have singleton dimensions
(e.g. one study group or timepoint) and still exist in the data space.
Therefore, when creating data spaces it is better to be inclusive of
all potential dimensions (categories) in order to make them more general.


.. _data_columns:

Columns
-------

Before data in a dataset can be manipulated, it must be assigned to a data frame.
This is done by defining the "columns" of the dataset. Dataset columns are slices of corresponding
data items across each "row" of a data frame, e.g. ages for every subject or
T1-weighted MRI images for every session.

When defining a column the "row frequency" of the data frame it belongs to
(see :ref:`data_spaces`) needs to be specified. For example, age fields occur
per subject, whereas T1-weighted images occur per
imaging session. Items in a column do not need to be named consistently
(although it makes it easier where possible), however,
they must be encapsulated by the same data format (see :ref:`Formats`). 

There are two types of columns in Arcana datasets, *sources* and *sinks*.
Source columns select matching items across the dataset from existing data
using a range of criteria:

* path (can be a regular-expression)
* data type
* row row_frequency
* quality threshold (only currently implemented for XNAT_ stores)
* header values (only available for selected formats)
* order within the data row (e.g. first T1-weighted scan that meets all other criteria in a session)

Sink columns define how derived data will be written to the dataset.

DataColumns are given a name, which is used to map to the inputs/outputs of pipelines.
By default, this name is used by sinks to name the output fields/files stored
in the dataset. However, if a specific output path is desired it can be
specified by the ``path`` argument.

Use the :meth:`.Dataset.add_source` and :meth:`.Dataset.add_sink` methods to add
sources and sinks via the API.

.. code-block:: python

    from arcana.data.spaces.medimage import Clinical
    from arcana.data.formats.medimage import Dicom, NiftiGz

    xnat_dataset.add_source(
        name='T1w',
        path=r'.*t1_mprage.*'
        format=Dicom,
        order=1,
        quality_threshold='usable',
        is_regex=True
    )

    fs_dataset.add_sink(
        name='brain_template',
        format=NiftiGz,
        row_frequency='group'
    )

To access the data in the columns once they are defined use the ``Dataset[]``
operator

.. code-block:: python

    import matplotlib.pyplot as plt
    from arcana.core.data.set import Dataset

    # Get a column containing all T1-weighted MRI images across the dataset
    xnat_dataset = Dataset.load('xnat-central//MYXNATPROJECT')
    t1w = xnat_dataset['T1w']

    # Plot a slice of the image data from a Subject sub01's imaging session
    # at Timepoint T2. (Note: such data access is only available for selected
    # data formats that have convenient Python readers)
    plt.imshow(t1w['T2', 'sub01'].data[:, :, 30])


Use the ``arcana source add`` and ``arcana sink add`` commands to add sources/sinks
to a dataset using the CLI.

.. code-block:: console

    $ arcana dataset add-source 'xnat-central//MYXNATPROJECT' T1w \
      medimage:Dicom --path '.*t1_mprage.*' \
      --order 1 --quality usable --regex

    $ arcana dataset add-sink 'file///data/imaging/my-project:training' brain_template \
      medimage:NiftiGz --row_frequency group


One of the main benefits of using datasets in BIDS_ format is that the names
and file formats of the data are strictly defined. This allows the :class:`.Bids`
data store object to automatically add sources to the dataset when it is
initialised.

.. code-block:: python

    from arcana.data.stores.bids import Bids
    from arcana.data.stores.common import FileSystem
    from arcana.data.spaces.medimage import Clinical

    bids_dataset = Bids().dataset(
        id='/data/openneuro/ds00014')

    # Print dimensions of T1-weighted MRI image for Subject 'sub01'
    print(bids_dataset['T1w']['sub01'].header['dim'])

.. _data_formats:

Formats
-------

Data items within a dataset (i.e. the intersection of a column and a row) are
encapsulated by :class:`DataFormat` objects, which will be subclasses of one
three base classes:

* :class:`.Field` (int, float, str or bool)
* :class:`.ArrayField` (a sequence of int, float, str or bool)
* :class:`.FileGroup` (single files, files + header/side-cars or directories)

Items act as pointers to the data in the data store. Data in remote stores need to be
cached locally with :meth:`.DataItem.get` before they can be accessed.
Modified data is pushed back to the store with :meth:`.DataItem.put`.

The :class:`.FileGroup` class is typically subclassed to specify the format of the files
in the group. There are a number common file formats implemented in
:mod:`arcana.data.formats.common`, including :class:`.Text`,
:class:`.Zip`, :class:`.Json` and :class:`.Directory`. :class:`.FileGroup` subclasses
may contain methods for conveniently accessing the file data and header metadata (e.g.
:class:`.medimage.Dicom` and :class:`.medimage.NiftiGzX`) but this
is not a requirement for usage in workflows.

Arcana will implicily handle conversions between file formats where a
converter has been specified and is available on the processing machine.
See :ref:`adding_formats` for detailed instructions on how to specify new file
formats and conversions between them.

On the command line, file formats can be specified by *<full-module-path>:<class-name>*,
e.g. ``arcana.data.formats.common:Text``, although if the format is in a submodule of
``arcana.data.formats`` then that prefix can be dropped for convenience, e.g. ``common:Text``. 


.. _Arcana: https://arcana.readthedocs.io
.. _XNAT: https://xnat.org
.. _BIDS: https://bids.neuroimaging.io
