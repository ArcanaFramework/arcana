Data Model
==========

Arcana handles all workflow file/network I/O interactions, making pipeline
implementations transportable between storage systems and allowing workflow
designers to focus on the analysis being implemented rather than boring details.
As such Arcana contains a rich and flexible data model consisting of
``DataStore``, ``Dataset``, ``DataDimension``, ``DataColumn``, ``DataNode`` and
``DataItem`` classes and their corresponding sub-classes.


Stores
------

Support for different storage techniques is provided by modular ``DataStore``
objects. ``DataStore`` objects not only encapsulate where the data is stored
(e.g. on local disk or remote repository) but also how the data is accessed
(whether it is in BIDS format or not, or whether using the XNAT container
service or purely the XNAT API).

There are currently three ``DataStore`` sub-types implemented in Arcana:

* FileSystem - access data simply organised within a directory tree on the file system
* BidsFormat - access data on file systems organised in the
               `Brain Imaging Data Structure (BIDS) <https://bids.neuroimaging.io/>`__
               format (neuroscience-specific)
* Xnat - access data stored in XNAT_ repositories by its REST API (only)
* XnatViaCS - access data stored in XNAT_ repositories as exposed to integrated
              pipelines run in XNAT_'s container service (using a combination
              of direct access to data archive and API)

Alternative storage systems can be implemented by extending the base
``DataStore`` class. The developers are interested to add support for new systems,
so if you would like to use Arcana with a different storage system please
create an issue for it `here <https://github.com/Australian-Imaging-Service/arcana/issues>`__.

Configuring access via API
~~~~~~~~~~~~~~~~~~~~~~~~~~

To configure access to a data store a via the API initialise the ``DataStore``
sub-class corresponding to your required data location/access-method then save
it to the YAML configuration file stored at `~/.arcana/stores.yml`, e.g.

.. code-block:: python

    import os
    from arcana.data.stores.xnat import Xnat

    # Initialise the data store object
    xnat_store = Xnat(
        server='http://central.xnat.org',
        user='user123',
        password=os.environ['XNAT_PASS'],
        cache_dir='/work/xnat-cache'
    )

    # Save it to the configuration file
    xnat_store.save('central-xnat')

    # Reload store from configuration file
    reloaded = DataStore.load('central-xnat')


Configuring access via CLI
~~~~~~~~~~~~~~~~~~~~~~~~~~

To configure access to a store via the CLI use the ``arcana store add`` sub-command

.. code-block:: bash

    $ arcana store add central-xnat xnat http://central.xnat.org --user user123 --cache_dir /work/xnat-cache --password
    Please enter you password for 'central-xnat': *******


See also ``arcana store rename`` and ``arcana store remove``.

.. note::

    Data stores that don't require any parameters such as ``FileSystem`` and
    ``BIDS`` don't need to be configured and can be accessed via their aliases,
    ``file`` and ``bids`` when defining a dataset.


Datasets and Data Dimensions
----------------------------

In Arcana, "datasets" refer to collection of data organised into a tree, which
branches across different "dimensions" of the data (e.g. over separate groups,
subjects or sessions), consisting of both "source data" (typically
acquired from an instrument) and data that has been derived from the source
data. Datasets are typically stored in a single location (although support for
distributed datasets is planned) such as a file-system directory or an
XNAT project. Workflows and analyses in Arcana operate on and over whole
datasets.

Data items can exist at any "node" within the data tree, and along any
axis of the dataset even if it is not in the original tree, e.g. summary
statistics that are analysed across the combination of group and time-points
from a data tree organised by group> subject> session.

When defining a dataset, you specify its tree structure and which nodes are to
be included in the analysis (e.g. the ones that passed QC). The first thing
to define is the dimensions of the dataset, which should be set to a sub-class of
``DataDimension`` enum. By default, Arcana will assume 
``arcana.data.dimensions.clinical:Clinical`` is applicable, which is able to
represents the typical structure of a longintudinal clinical trial with multiple
groups, subjects and sessions at different time-points (noting that a dataset
can singletons nodes along a dimension, e.g. a single group or time-point).

For stores that can store arbitrary tree structures (e.g. file-system directories),
the hierarchy of each dimension in the dataset tree needs to be provided, i.e.
whether the sub-directories immediately below the root contain data for different
groups, subjects, time-points or sessions, and the what the sub-directory layer
below that corresponds to (if present) and so on. This is defined by providing
a list of values, e.g. ``[Clinical.subject, Clinical.session]``.

<para on inferring IDs from others>

<para on including/excluding certain ids>


Defining a dataset via API
~~~~~~~~~~~~~~~~~~~~~~~~~~

Datasets can be defined in from data store using the ``DataStore.dataset()`` method,

.. code-block:: python

    from arcana.data.stores.xnat import Xnat
    from arcana.data.stores.file_system import FileSystem
    from arcana.data.dimensions.clinical import Clinical

    xnat_dataset = xnat_store.dataset(
        id='MYXNATPROJECT',
        excluded={Clinical.subject: ['09', '11']},
        included={Clincial.timepoint: ['T1']}
        id_inference={
            Clinical.subject: r'(?P<group>[A-Z]+)_(?P<member>\d+)'})
    
    fs_dataset = FileSystem().dataset(
        id='/data/imaging/my-project',
        hierarchy=[Clinical.group, Clinical.subject])

Dataset definitions can be saved inside the project directory and then reloaded
in new sessions.

.. code-block:: python

    xnat_dataset.save()

    reloaded = xnat_store.load_dataset('MYXNATPROJECT')

Sometimes, multiple dataset definitions may need to be saved inside a single
project (e.g. defining different subsets of subjects), this can be done by
providing the ``name`` parameter to the ``Dataset.save()`` and
``DataStore.load_dataset()`` methods.

.. code-block:: python

    xnat_dataset.save('passed_dwi_qc')

    dwi_dataset = xnat_store.load_dataset('MYXNATPROJECT', 'passed_dwi_qc')


Defining a dataset via CLI
~~~~~~~~~~~~~~~~~~~~~~~~~~

Datasets can also be defined via the CLI using the ``arcana dataset define``
command, prepending the data store nickname to the project ID separated by '//'

.. code-block:: bash

    $ arcana dataset define 'central-xnat//MYXNATPROJECT' \
      --excluded subject sub09,sub11 --included timepoint T1 \
      --id_inference subject '(?P<group>[A-Z]+)_(?P<member>\d+)'

To give the dataset definition a name append it to the ID string separated by ':'

.. code-block:: bash

    $ arcana dataset define 'file///data/imaging/my-project:training' group subject \
      --include subject 10:20


Columns
-------

The collection of corresponding items along a dimension of the dataset is
referred to as a "data column", drawing parallels with the way data is organised
in tabular data formats such as those used by Excel and Pandas. However, data
columns in Arcana occur at different "frequencies", some only have nodes per
subject (e.g. DOB), others per group (e.g. )


.. _Arcana: http://arcana.readthedocs.io
.. _XNAT: http://xnat.org