Data Model
==========

Arcana handles all workflow inputs and outputs to file or network locations.
To manage these interactions, Arcana contains a rich and flexible
object model. They key classes of Arcana's data model are:

* **DataStore** - abstraction of the storage system/format, which is sub-classed for different storage systems/formats
* **Dataset** - Encapsulates a set of data items (files, fields or arrays) repeated across a range of data points (e.g. subjects)
* **DataDimensions** - Defines the repetition structure (or "data space") for a class of datasets, e.g. whether there are data points for each subject and time-point, or each weather-station/day
* **DataNode** - a set of data items at a common data point, e.g. imaging session
* **DataColumn** - a single type of measurement repeated across the dataset
* **DataItem** - a single measurement or acquisition, e.g. T1-weighted MRI scan, humidity measurement

Stores
------

Support for different storage techniques is provided by sub-classes of the
``DataStore`` class. ``DataStore`` sub-classes not only encapsulate where the
data is stored, e.g. on local disk or remote repository, but also how the data
is accessed, e.g. whether it is in BIDS format or not, or whether to access
data in an XNAT repository as exposed to the container service or purely by
the XNAT API.

There are currently four implemented ``DataStore`` sub-classes:

* **FileSystem** - access data organised within a arbitrary directory tree on the file system
* **BidsFormat** - access data on file systems organised in the `Brain Imaging Data Structure (BIDS) <https://bids.neuroimaging.io/>`__ format (neuroimaging-specific)
* **Xnat** - access data stored in XNAT_ repositories by its REST API
* **XnatViaCS** - access data stored in XNAT_ repositories as exposed to integrated pipelines run in XNAT_'s container service using a combination of direct access to the archive disk and the REST API

Alternative storage systems can be implemented by writing a new sub-class of
``DataStore``. The developers are interested in adding support for new systems,
so if you would like to use Arcana with a different storage system please
create an issue for it in the `GitHub Issue Tracker <https://github.com/Australian-Imaging-Service/arcana/issues>`__.

To configure access to a data store a via the API, initialise the ``DataStore``
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

    $ arcana store add xnat-central xnat https://central.xnat.org --user user123 \
      --cache_dir /work/xnat-cache
    Password:


See also ``arcana store rename`` and ``arcana store remove``.

.. note::

    Data stores that don't require any parameters such as ``FileSystem`` and
    ``BIDS`` don't need to be configured and can be accessed via their aliases,
    ``file`` and ``bids`` when defining a dataset.


Datasets, dimensions and nodes
------------------------------

In Arcana, *datasets* refer to collection of data organised into a tree, which
branches across different *dimensions* of the data (e.g. over separate groups,
subjects or sessions), consisting of both source data (typically
acquired from an instrument) and data that has been derived from the source
data. Datasets are typically stored in a single location (although support for
distributed datasets is planned) such as a file-system directory or an
XNAT project. Workflows and analyses in Arcana operate on and over whole
datasets.

Data items can exist at any *node* within the data tree, and along any
axis of the dataset even if it is not in the original tree, e.g. summary
statistics that are analysed across the combination of group and time-points
from a data tree organised by group> subject> session.

When defining a dataset, you specify its tree structure and which nodes are to
be included in the analysis (e.g. the ones that passed QC). The first thing
to define is the dimensions of the dataset, which should be set to a sub-class of
``DataDimension`` enum. By default, Arcana will assume 
``arcana.data.dimensions.medicalimaging:ClinicalTrial`` is applicable, which is able to
represents the typical structure of a longintudinal medicalimaging trial with multiple
groups, subjects and sessions at different time-points (noting that a dataset
can singletons nodes along a dimension, e.g. a single group or time-point).

For stores that can store arbitrary tree structures (e.g. file-system directories),
the hierarchy of each dimension in the dataset tree needs to be provided, i.e.
whether the sub-directories immediately below the root contain data for different
groups, subjects, time-points or sessions, and the what the sub-directory layer
below that corresponds to (if present) and so on. This is defined by providing
a list of values, e.g. ``[ClinicalTrial.subject, ClinicalTrial.session]``.

In some datasets, especially in stores where the tree hierarchy is fixed (e.g. XNAT),
you may need to infer the ID(s) for one or more dimensions from the combination
with other IDs following an arbitrary naming convention. For example, given an
XNAT project where all the test subjects are numbered "TEST01", "TEST02", "TEST03",...
and the matched control subjects are numbered "CON01", "CON02", "CON03",...,
the group and (matched) "member" IDs need to be inferred from the subject ID.
This can be done by providing an ``id_inference`` argument which takes a list
of tuples, consisting of the frequency of the ID to infer from and a
regular-expression (Python syntax), with named groups corresponding to inferred
IDs.

After datasets have undergone quality control checks there are often a number
of data nodes that need to be omitted from a given analysis. These nodes can
be specified using the ``excluded`` argument, which takes the data dimension and
and a list of IDs to be excluded from it. You can exclude over multiple dimensions,
noting that if you exclude along the lower levels of your hierarchy then corresponding
IDs at higher levels will also be excluded. For example, if you exclude the timepoint 2
imaging session for subject 5, then both Timepoint 2 and Subject 5 will be dropped)
therefore it is typically better to exclude at a higher level (e.g. Subject 5).
The ``include`` argument is the inverse of exclude and can be more convenient when
you only want to select a small sample from a larger dataset.

You may want multiple dataset definitions for a given project/directory, e.g. with
different subsets of IDs, for different analyses. To avoid conflicts you can
assign a dataset definition a ``name``, which is used differentiate between multiple
dataset definitions stored in the same project/directory.

.. warning::

    This needs to be broken up into smaller parts


Datasets can be defined in from data store using the ``DataStore.dataset()`` method,

.. code-block:: python

    from arcana.data.stores.xnat import Xnat
    from arcana.data.stores.file_system import FileSystem
    from arcana.data.dimensions.medicalimaging import ClinicalTrial

    xnat_dataset = xnat_store.dataset(
        id='MYXNATPROJECT',
        excluded={ClinicalTrial.subject: ['09', '11']},  # Alternatively use 'subject' string instead of enum
        included={Clincial.timepoint: ['T1']}
        id_inference=[
            (ClinicalTrial.subject, r'(?P<group>[A-Z]+)_(?P<member>\d+)')])

    fs_dataset = FileSystem().dataset(
        id='/data/imaging/my-project',
        hierarchy=[ClinicalTrial.group, ClinicalTrial.subject])

Dataset definitions can be saved inside the project directory and then reloaded
in new sessions.

.. code-block:: python

    xnat_dataset.save()

    reloaded = xnat_store.load_dataset('MYXNATPROJECT')

Naming of the dataset can be done providing the ``name`` parameter to the
``Dataset.save()`` and ``DataStore.load_dataset()`` methods.

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
string separated by ':'

.. code-block:: bash

    $ arcana dataset define 'file///data/imaging/my-project:training' group subject \
      --include subject 10:20


Items and data types
--------------------

``DataItem`` objects are atomic elements in Arcana datasets, and can be either
*fields* (int, float, str or bool), *array fields* (sequence[int or float or str or bool])
or *file groups* (single files, files + header/side-cars or directories).
Data items act as pointers to the data associated provenance in the
dataset and provide methods for pulling and pushing data to the store.

Arcana implicitly handles conversions between different file formats

``FileGroup`` sub-classes may contain methods for accessing the file data and header metadata,
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

Use the ``Dataset.add_source()`` and ``Dataset.add_sink()`` methods to add
sources and sinks via the API.

.. code-block:: python

    from arcana.data.dimensions.medicalimaging import ClinicalTrial
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
and file formats of the data are strictly defined. This allows the ``BidsFormat``
data store object to automatically add sources to the dataset when it is
initialised.

.. code-block:: python

    from arcana.data.stores.bids import BidsFormat
    from arcana.data.stores.file_system import FileSystem
    from arcana.data.dimensions.medicalimaging import ClinicalTrial

    bids_dataset = BidsFormat().dataset(
        id='/data/openneuro/ds00014')

    print(bids_dataset['T1w']['sub01'].header['dim'])

.. _Arcana: https://arcana.readthedocs.io
.. _XNAT: https://xnat.org
.. _BIDS: https://bids.neuroimaging.io