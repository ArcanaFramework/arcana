Data model
==========

Arcana's data model sets out to bridge the gap between
the semi-structured data trees that file-based data are typically stored in,
and the tabular data frames required for statistical analysis. Note that this
transformation is abstract, with the source data remaining within original data
tree and generated derivatives stored alongside them.

The key elements of Arcana's data model are:

* :ref:`Stores` - tree-based file storage systems 
* :ref:`Datasets` - sets of comparable data within a store (e.g. `XNAT <https://xnat.org>`__ project or `BIDS <https://bids.neuroimaging.io>`__ dataset)
* :ref:`Items` - references to dataset elements (files, array and scalar fields) and the format they are stored in (e.g. DICOM, NIfTI, JSON, plain-text, etc..)
* :ref:`Frames (Rows and Columns)` - abstract tables of data items within datasets
* :ref:`Grids and Spaces` - conceptual link between tree and tabular data structures


Stores
------

Support for different file storage systems (e.g. `XNAT <https://xnat.org>`__, `BIDS <https://bids.neuroimaging.io>`__)
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


To configure access to a store via the CLI use the ``arcana store add`` sub-command

.. code-block:: console

    $ arcana store add xnat xnat-central https://central.xnat.org \
      --user user123 --cache_dir /work/xnat-cache
    Password:


See also ``arcana store rename``, ``arcana store remove`` and ``arcana store ls``.

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

.. note::

    Data stores that don't require any parameters such as :class:`.FileSystem` and
    :class:`.Bids` don't need to be configured and can be accessed via their aliases,
    ``file`` and ``bids`` when defining a dataset.

.. _datasets::

Datasets
--------

In Arcana, a *dataset* refers to a collection of comparable data
(e.g. data from a single research study, or large collection such as the
Human Connectome Project). Arcana datasets consist of both source data and the
derivatives derived from them. Datasets are organised into a tree with a
consistent "hierarchy" that classifies a series of measurement events
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

The leaf sub-directories of the directory tree contain data from "image session"
measurement events, as designated by the combination of one of the three
subject IDs and one of the two timepoint IDs.

While the majority of data items are stored in the leaves of the tree (e.g. per-session),
data can exist for any repeating element. For example, an analysis may use
genomics data, which will be constant for each subject, and therefore sits at
the subject level of the tree

.. code-block::

    my-dataset
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


Datasets are referenced in the CLI by the nickname of the store it is stored in
as saved by `arcana store add` (i.e. , see :ref:`Stores`) and the dataset ID,
separated by '//'. For `FileSystem` an `Bids` stores, the dataset ID is just the
absolute path to the file-system directory the data is stored in. For `Xnat`
stores the dataset ID is the project ID. For example, if the login details
for XNAT Central have been saved under the nickname *xnat-central*, then
the *MYXNATPROJECT* project on XNAT central can be referenced by
``xnat-central//MYXNATPROJECT``.

Alternatively dataset objects can be created via the Python API using the
:meth:`.DataStore.dataset` method. For example, to define a new dataset
corresponding to *MYXNATPROJECT*

.. code-block:: python

    xnat_dataset = xnat_store.dataset(id='MYXNATPROJECT')

For stores that support datasets with arbitrary tree structures
(i.e. :ref:`FileSystem`), the "data space" and the hierarchy of layers
in the data tree needs to be provided. Data spaces are explained in more
detail in :ref:`data_spaces`, however, for the majority of datasets in the
medical imaging field, the `arcana.data.spaces.medimage.Clinical` space is
appropriate.

.. code-block:: python

    from arcana.data.stores.common import FileSystem
    from arcana.data.spaces.medimage import Clinical

    fs_dataset = FileSystem().dataset(
        id='/data/imaging/my-project',
        # Define hierarchy within Clinical data spacethat defines sessions
        # separated into sub-dirs by study group (i.e. test & control)
        space=Clinical,
        hierarchy=['group', 'session'])  


.. _data_formats:

Items
-----

Atomic items within a dataset are encapsulated by :class:`DataItem` objects.
Data items are one of three sub-types:

* :class:`.FileGroup` (single files, files + header/side-cars or directories)
* :class:`.Field` (int, float, str or bool)
* :class:`.ArrayField` (an array of int, float, str or bool)

Data item objects reference files and fields stored in the data store, rather
than necessarily holding the data themselves. Before data in remote stores
are accessed it is cached locally with :meth:`.DataItem.get`.
Derivatives and modified data items are placed into the store with :meth:`.DataItem.put`.

The :class:`.FileGroup` class is typically subclassed to specify the format of the
files/directories in the group. For example, there are a number common file
formats implemented in :mod:`arcana.data.formats.common`, including

* :class:`.Text`
* :class:`.Zip`
* :class:`.Json`
* :class:`.Directory`
* :class:`.FileGroup`

Such sub-classesmay also contain methods for conveniently accessing the file data and header
metadata (e.g. :class:`.medimage.Dicom` and :class:`.medimage.NiftiGzX`), but this
is not necessary in general.

Arcana will implicily handle conversions between compatible file formats where a
converter has been specified. See :ref:`adding_formats` for detailed
instructions on how to specify new file formats and conversions between them.

On the command line, file formats are specified by *<full-module-path>:<class-name>* syntax,
e.g. ``arcana.data.formats.common:Text``. If the format is in a submodule of
``arcana.data.formats`` then that prefix can be dropped for convenience, e.g. ``common:Text``. 


.. _data_columns:

Frames (Rows and Columns)
-------------------------

Before data within a dataset can be manipulated, they must be assigned to a data frame.
The "rows" of a data frame correspond to nodes across a single layer of the data
tree, such as imaging sessions, subjects or study groups (e.g. 'test' or 'control'),
and the "columns" are slices of comparable data items across each row, e.g.
a T1-weighted MR acquisition for each imaging session, a genetic test for each
subject, or an fMRI activation map derived for each study group.

Defining a data frame in a dataset is done by adding "source" columns, to
access existing (typically acquired) data, or "sink" columns, to define where
derivatives will be stored within the data tree. The "row frequency"
(e.g. per 'session', 'subject', etc...) of the data frame and format of the
member items (see :ref:`Items`) need to be specified when adding a column
to a dataset, and must be consistent across the column. 

Files and fields containing the data to be accessed by a source column do not need to
be named consistently across the dataset (although it makes it easier where possible).
Source columns can be configured to select the matching item in each row of the
frame via a number of criteria

* "path", either the relative file path for `FileSystem`/`Bids` stores, or scan-type for `Xnat`/`XnatViaCS` stores
    * the path is treated as a regular-expression if the `is_regex` flag is set.
* quality threshold (currently only available for XNAT_ stores)
* header values (available for selected formats such as `arcana.data.formats.medimage.Dicom`)
* order the item appears the data row (e.g. first T1-weighted scan that meets all other criteria in a session, currently only available for XNAT_ stores)

Sink columns define how derived data will be written to the dataset via their
`path` argument. The provided path is either the relative path to the target location for
`FileSystem`/`Bids` stores, or resource name for `Xnat`/`XnatViaCS` stores.

Each column is assigned a name when it is created, which is used when
accessing the data and connecting pipeline inputs and outputs to the dataset.
By default, this name will be used as the path of the of sink columns.

Use the ``arcana source add`` and ``arcana sink add`` commands to add sources/sinks
to a dataset using the CLI.

.. code-block:: console

    $ arcana dataset add-source 'xnat-central//MYXNATPROJECT' T1w \
      medimage:Dicom --path '.*t1_mprage.*' \
      --order 1 --quality usable --regex

    $ arcana dataset add-sink 'file///data/imaging/my-project' fmri_activation_map \
      medimage:NiftiGz --row_frequency group


Alternatively, the :meth:`.Dataset.add_source` and :meth:`.Dataset.add_sink` methods can be used
directly to add sources and sinks via the Python API.

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

.. note::
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


.. _data_spaces:

Grids and Spaces
----------------

The number of possible row frequencies depends on the depth of the hierarchy of
the data tree. An item can be singular in any layer of the hierarchy,
therefore there are 2^N possible row frequencies for a data tree of depth N.
For example, trees with two layers, 'a' and 'b', have four possible row
frequencies, 'ab', 'a', 'b' and the dataset as a whole. 
In Arcana, this binary structure is refered as a "data space", drawing a
loose analogy with a Cartesian space of dimension N in which measurement events
occur 

whether it fits into the original hierarchy of the dataset or not. For example, statistics
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
    ├── MEMBER
    │   ├── member1
    │   │   └── age_diff
    │   └── member2
    │       └── age_diff
    ├── MATCHEDPOINT
    │   ├── member1_timepoint1
    │   │   └── comparative_trial_performance
    │   ├── member1_timepoint2
    │   │   └── comparative_trial_performance
    │   ├── member2_timepoint1
    │   │   └── comparative_trial_performance
    │   └── member2_timepoint2
    │       └── comparative_trial_performance
    ├── group1
    │   ├── member1    
    │   │   ├── timepoint1
    │   │   │   ├── t1w_mprage
    │   │   │   ├── t2w_space
    │   │   │   └── bold_rest
    │   │   └── timepoint2
    │   │       ├── t1w_mprage
    │   │       ├── t2w_space
    │   │       └── bold_rest
    │   └── member2
    │       ├── timepoint1
    │       │   ├── t1w_mprage
    │       │   ├── t2w_space
    │       │   └── bold_rest
    │       └── timepoint2
    │           ├── t1w_mprage
    │           ├── t2w_space
    │           └── bold_rest
    └── group2
        |── member1    
        │   ├── timepoint1
        │   │   ├── t1w_mprage
        │   │   ├── t2w_space
        │   │   └── bold_rest
        │   └── timepoint2
        │       ├── t1w_mprage
        │       ├── t2w_space
        │       └── bold_rest
        └── member2
            ├── timepoint1
            │   ├── t1w_mprage
            │   ├── t2w_space
            │   └── bold_rest
            └── timepoint2
                ├── t1w_mprage
                ├── t2w_space
                └── bold_rest

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

These definitions can be saved inside the project directory and then reloaded
in new Python contexts.

.. code-block:: python

    fs_dataset.save()

    ...

    reloaded = FileSystem().load_dataset('/data/imaging/my-project')            


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


.. _Arcana: https://arcana.readthedocs.io
.. _XNAT: https://xnat.org
.. _BIDS: https://bids.neuroimaging.io
