Data model 
==========
 
Arcana's data model sets out to bridge the gap between 
the semi-structured data trees that file-based data are typically stored in, 
and the tabular data frames used in statistical analysis. Note that this 
transformation is abstract, with the source data remaining within original data 
tree and generated derivatives stored alongside them. 
 
The key elements of Arcana's data model are: 
 
* :ref:`Stores` - tree-based file storage systems 
* :ref:`Datasets` - comparable data to be jointly analysed 
* :ref:`Items` - references to data elements: files, scalars, and arrays. 
    * sub-classes specify the data format, e.g. :class:`.medimage.Dicom`, :class:`.medimage.NiftiGzX`, :class:`.common.Json` 
* :ref:`data_columns` - abstract tables of data items within datasets 
* :ref:`data_spaces` - conceptual link between tree and tabular data structures 
* :ref:`data_grids` - selection of data points to be included in an analysis
 
 
Stores 
------
 
Support for different file storage systems (e.g. `XNAT <https://xnat.org>`__, `BIDS <https://bids.neuroimaging.io>`__) 
is provided by sub-classes of the :class:`.DataStore` class. :class:`.DataStore` 
sub-classes not only encapsulate where the data are stored, e.g. on local disk or 
remote repository, but also how the data are accessed, e.g. whether they are in 
BIDS format, or whether files in an XNAT repository can be accessed directly 
(i.e. as exposed to the container service), or purely using the API. 
 
There are four :class:`.DataStore` sub-classes in the common and medimage 
arcana packages (for instructions on how to add support for new systems 
see :ref:`alternative_stores`): 
 
* :class:`.FileSystem` - access data organised within an arbitrary directory tree on the file system 
* :class:`.Bids` - access data on file systems organised in the `Brain Imaging Data Structure (BIDS) <https://bids.neuroimaging.io/>`__ format (neuroimaging-specific) 
* :class:`.Xnat` - access data stored in XNAT_ repositories by its REST API 
* :class:`.XnatViaCS` - access data stored in XNAT_ repositories as exposed to integrated pipelines run in `XNAT's container service <https://wiki.xnat.org/container-service/using-the-container-service-122978908.html>`_ using a combination of direct access to the archive disk and the REST API 
 
 
To configure access to a store via the CLI use the ``arcana store add`` sub-command 
 
.. code-block:: console 
 
    $ arcana store add xnat xnat-central https://central.xnat.org \ 
      --user user123 --cache_dir /work/xnat-cache 
    Password: 
 
.. note:: 
    See also ``arcana store rename``, ``arcana store remove`` and ``arcana store ls``. 
 
Alternatively, to configure access to a data store a via the Python API, initialise the :class:`.DataStore` 
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
 
.. _datasets: 
 
Datasets 
--------
 
In Arcana, a *dataset* refers to a collection of comparable data, 
e.g. data from a single research study, or large collection such as the 
Human Connectome Project. Arcana datasets consist of both source data and the 
derivatives derived from them. Datasets are organised into a tree with a 
consistent "hierarchy" that classifies a series of data points 
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
data points, as designated by the combination of one of the three 
subject IDs and one of the two timepoint IDs. 
 
While the majority of data items are stored in the leaves of the tree (e.g. per-session), 
data can exist for any branch. For example, an analysis may use 
genomics data, which will be constant for each subject, and therefore sits at 
the subject level of the tree under a special *SUBJECT* sub-directory
 
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
 
 
Datasets are referenced in the CLI by the nickname of the store they are stored in 
(as saved by ``arcana store add``, see :ref:`Stores`) and the dataset ID, 
separated by ``//``. For :class:`.FileSystem` and :class:`.Bids` stores, the dataset ID is just the 
absolute path to the file-system directory the data are stored in. For :class:`.Xnat` 
stores the dataset ID is the project ID. For example, if the login details 
for XNAT Central have been saved under the nickname "xnat-central", then 
the "MYXNATPROJECT" project on XNAT central can be referenced by 
``xnat-central//MYXNATPROJECT``. 
 
Alternatively, dataset objects can be created via the Python API using the 
:meth:`.DataStore.dataset` method. For example, to define a new dataset 
corresponding to *MYXNATPROJECT* 
 
.. code-block:: python 
 
    xnat_dataset = xnat_store.dataset(id='MYXNATPROJECT') 
 
For stores that support datasets with arbitrary tree structures 
(i.e. :class:`.FileSystem`), the "data space" and the hierarchy of layers 
in the data tree needs to be provided. Data spaces are explained in more 
detail in :ref:`data_spaces`. However, for the majority of datasets in the 
medical imaging field, the :class:`arcana.data.spaces.medimage.Clinical` space is 
appropriate. 
 
.. code-block:: python 
 
    from arcana.data.stores.common import FileSystem 
    from arcana.data.spaces.medimage import Clinical 
 
    fs_dataset = FileSystem().dataset( 
        id='/data/imaging/my-project', 
        # Define the hierarchy of the dataset in which imaging session 
        # sub-directories are separated into directories via their study group 
        # (i.e. test & control) 
        space=Clinical, 
        hierarchy=['group', 'session'])   
 
 
.. _data_formats: 
 
Items 
-----
 
Atomic items within a dataset are encapsulated by :class:`DataItem` objects. 
:class:`DataItem`` has three sub-types: 
 
* :class:`.FileGroup` (single files, files + header/side-cars or directories) 
* :class:`.Field` (int, float, str or bool) 
* :class:`.ArrayField` (an array of int, float, str or bool) 
 
Data item objects reference files and fields stored in the data store, rather 
than necessarily holding the data themselves. Before data in remote stores 
are accessed they are cached locally with :meth:`.DataItem.get`. 
Newly created and modified data items are placed into the store with
:meth:`.DataItem.put`. 
 
:class:`.FileGroup` is typically subclassed to specify the format of the 
files/directories in the group. For example, there are a number common file 
formats implemented in :mod:`arcana.data.formats.common`, including 
 
* :class:`.common.Text` 
* :class:`.common.Zip` 
* :class:`.common.Json` 
* :class:`.common.Directory` 
 
:class:`FileGroup` sub-classes specify the files and directories expected in
the file group, converters from alternative file formats, and may
also contain methods for accessing the headers and the contents of files
(e.g. :class:`.medimage.Dicom` and :class:`.medimage.NiftiGzX`). Arcana will
automatically convert between file formats when there is a mismatch
between the format of the item stored in the dataset and that required by a
pipeline when a converter is specified. See :ref:`adding_formats` for detailed
instructions on how to specify new file formats and conversions between them. 
 
On the command line, file formats are specified by *<full-module-path>:<class-name>*, 
e.g. ``arcana.data.formats.common:Text``. However, if the format is in a submodule of 
``arcana.data.formats`` then that prefix can be dropped for convenience, e.g. ``common:Text``.  
 
 
.. _data_columns: 
 
Frames: Rows and Columns 
-------------------------
 
Before data within a dataset can be manipulated by Arcana, they must be 
assigned to a data frame. The "rows" of a data frame correspond to nodes 
across a single layer of the data tree, such as 
 
* imaging sessions 
* subjects 
* study groups (e.g. 'test' or 'control') 
 
and the "columns" are slices of comparable data items across each row, e.g. 
 
* T1-weighted MR acquisition for each imaging session 
* a genetic test for each subject 
* an fMRI activation map derived for each study group. 

.. TODO: visualisation of data frame

A data frame is defined by adding "source" columns to access existing 
(typically acquired) data, and "sink" columns to define where 
derivatives will be stored within the data tree. The "row frequency" argument 
of the column (e.g. per 'session', 'subject', etc...) determines which data frame 
the column belongs to. The format of a column's member items (see :ref:`Items`)
must be consistent and is also specified when the column is created.
 
The data items within a source column do not need to have consistent labels
although it makes it easier where possible. Source columns match single items
in each row based on several criteria (an error is raised if no, or multiple
items are matched):
 
* **path** - label for the file-group or field 
    * scan type for XNAT stores 
    * relative file path from row sub-directory for file-system/BIDS stores 
    * is treated as a regular-expression if the `is_regex` flag is set. 
* **quality threshold** - the minimum quality for the item to be included 
    * only applicable for XNAT_ stores, where the quality 
* **header values** - header values are sometimes needed to distinguish file 
    * only available for selected item formats such as :class:`.medimage.Dicom` 
* **order** - the order that an item appears the data row 
    * e.g. first T1-weighted scan that meets all other criteria in a session 
    * only applicable for XNAT_ stores 
 
The ``path`` argument provided to sink columns defines where derived data will 
be stored within the dataset 
 
* the resource name for XNAT stores. 
* the relative path to the target location for file-system stores 
 
Each column is assigned a name when it is created, which is used when 
connecting pipeline inputs and outputs to the dataset and accessing the data directly. 
The column name is used as the default value for the paths of sink columns. 
 
Use the ``arcana source add`` and ``arcana sink add`` commands to add sources/sinks 
to a dataset using the CLI. 
 
.. code-block:: console 
 
    $ arcana dataset add-source 'xnat-central//MYXNATPROJECT' T1w \ 
      medimage:Dicom --path '.*t1_mprage.*' \ 
      --order 1 --quality usable --regex 
 
    $ arcana dataset add-sink 'file///data/imaging/my-project' fmri_activation_map \ 
      medimage:NiftiGz --row_frequency group 
 
 
Alternatively, the :meth:`.Dataset.add_source` and :meth:`.Dataset.add_sink`
methods can be used directly to add sources and sinks via the Python API. 
 
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
 
Spaces 
------
 
In addition to data frames corresponding to row frequencies that explicitly 
appear in the hierarchy of the data tree (see :ref:`data_columns`), 
there are a number of frames that are implied and may be needed to store 
derivatives of a particular analysis. In clinical imaging research studies/trials,
imaging sessions are classified by the subject who was scanned and, if applicable,
the longitudinal timepoint. The subjects themselves are often classified by which
group they belong to. Therefore, we can factor imaging session
classifications onto three "axes" 
 
* **group** - study group (e.g. 'test' or 'control') 
* **member** - ID relative to group 
    * can be arbitrary or used to signify control-matched pairs 
    * e.g. the '03' in 'TEST03' & 'CONT03' pair of control-matched subject IDs 
* **timepoint** - longintudinal timepoint 
 
Depending on the hierarchy of the data tree, data belonging to these axial 
frequencies may or may not have a corresponding branch to be stored in
In these cases, new branches are created off the root of the tree to 
hold the derivatives. For example, average trial performance data, calculated 
at each timepoint and the age difference between matched-control pairs, would 
need to be stored in new sub-branches for timepoints and members, respectively
 
.. code-block:: 
 
    my-dataset 
    ├── TIMEPOINT 
    │   ├── timepoint1 
    │   │   └── avg_trial_performance 
    │   └── timepoint2 
    │       └── avg_trial_performance 
    ├── MEMBER 
    │   ├── member1 
    │   │   └── age_diff 
    │   └── member2 
    │       └── age_diff 
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
 
In this framework, ``subject`` IDs are equivalent to the combination of
``group + member`` IDs and ``session`` IDs are equivalent to the combination of
``group + member + timepint`` IDs. Therefore, there are,  2\ :sup:`N`
row frequencies for a given data tree, where ``N`` is the depth of the tree
(i.e. ``N=3`` in this case). In Arcana, the set of all possible ID combinations
is imagined as a "data space", in which data points (e.g. imaging sessions) are
visualised as being laid out on a grid along the axes (e.g. ``group``, ``member``,
``timepoint``).

.. TODO: 3D plot of grid
 
Note that the grid of a particular dataset can have a single point along any 
given dimension (e.g. one study group or timepoint) and still exist in the data 
space. Therefore, when creating data spaces it is better to be inclusive of 
potential categories to make them more general.

.. TODO: another 3D grid plot
 
All combinations of the data spaces axes are given a name within 
:class:`.DataSpace` enums. In the case of the :class:`.medimage.Clinical`
data space, the members are
 
* **group** (group) 
* **member** (member) 
* **timepoint** (timepoint) 
* **session** (member + group + timepoint), 
* **subject** (member + group) 
* **batch** (group + timepoint) 
* **matchedpoint** (member + timepoint) 
* **dataset** () 
 
If they are not present in the data tree, alternative row frequencies are 
stored in new branches under the dataset root, in the same manner as data space
axes
 
.. code-block:: 
 
    my-dataset 
    ├── BATCH 
    │   ├── group1_timepoint1 
    │   │   └── avg_connectivity 
    │   ├── group1_timepoint2 
    │   │   └── avg_connectivity     
    │   ├── group2_timepoint1 
    │   │   └── avg_connectivity     
    │   └── group2_timepoint2 
    │       └── avg_connectivity 
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
 
.. TODO Should include example of weird data hierarchy using these frequencies 
.. and how the layers add to one another 
 
As mentioned previously, the :class:`.medimage.Clinical` data space is 
likely to be sufficient for most applications in medical imaging research, 
but please see :ref:`adding_formats` for a description on how to create custom 
data spaces as required.
 
For datasets where the fundamental hierarchy of the storage system is fixed 
(e.g. XNAT), you may need to infer the data point IDs along an axis
by decomposing a branch label following a given naming convention. 
This is specified via the ``id_inference`` argument to the dataset definition.
For example, given a an XNAT project with the following structure and a naming
convention where the subject ID is composed of the group and member ID,
*<GROUPID><MEMBERID>*, and the session ID is composed of the subject ID and timepoint,
*<SUBJECTID>_MR<TIMEPOINTID>*
 
.. code-block:: 
 
    MY_XNAT_PROJECT 
    ├── TEST01 
    │   └── TEST01_MR01 
    │       ├── t1w_mprage 
    │       └── t2w_space 
    ├── TEST02 
    │   └── TEST02_MR01 
    │       ├── t1w_mprage 
    │       └── t2w_space 
    ├── CONT01 
    │   └── CONT01_MR01 
    │       ├── t1w_mprage 
    │       └── t2w_space 
    └── CONT02 
        └── CONT02_MR01 
            ├── t1w_mprage 
            └── t2w_space 
 
IDs for group, member and timepoint can be inferred from the subject and session 
IDs, by providing the frequency of the ID to decompose and a 
regular-expression (in Python syntax) to decompose it with. The regular 
expression should contain named groups that correspond to row frequencies of
the IDs to be inferred, e.g.
 
.. code-block:: console 
 
    $ arcana dataset define 'xnat-central//MYXNATPROJECT' \ 
      --id_inference subject '(?P<group>[A-Z]+)_(?P<member>\d+)' \
      --id_inference session '[A-Z0-9]+_MR(?P<timepoint>\d+)' 

.. _data_grids: 
 
Grids 
-----
 
Often there are data points that need to be removed from a given 
analysis due to missing or corrupted data. Such sections need to be removed 
in a way that the data points still lie on a rectangular grid within the
data space (see :ref:`data_spaces`) so derivatives computed over a given axis
or axes are drawn from comparable number of data points.

Sections of the data grid can be excluded at any point, or along lines or planes.
However, it is often advisable to exclude along an axes of data space so the
grid is rectangular. The ``exclude`` argument is used to 
in the dataspace which takes a dictionary mapping the data dimension to the
list of IDs to exclude.

.. TODO image of excluding points in grid

.. code-block:: console 
 
    $ arcana dataset define 'file///data/imaging/my-project' \ 
      medimage:Clinical subject session \ 
      --exclude member 03,11,27 

 
The ``include`` argument is the inverse of exclude and can be more convenient when 
you only want to select a small sample or split the dataset into sections.
``include`` can be used in conjunction with ``exclude`` but not for the same
frequencies. 
 
.. code-block:: console 
 
    $ arcana dataset define 'file///data/imaging/my-project' \ 
      medimage:Clinical subject session \ 
      --exclude member 03,11,27 
      --include timepoint 1,2
 
You may want multiple dataset definitions for a given project/directory, 
for different analyses e.g. with different subsets of IDs depending on which 
scans have passed quality control, or to define training and test datasets 
for machine learning. To keep these analyses separate, you can 
assign a dataset definition a name, which is used differentiate between multiple 
definitions stored in the same dataset project/directory. To do this via the  
CLI, append the name to the dataset's ID string separated by '::', e.g. 
 
.. code-block:: console 
 
    $ arcana dataset define 'file///data/imaging/my-project::training' \ 
      medimage:Clinical group subject \ 
      --include member 10:20 
 
 
.. _Arcana: https://arcana.readthedocs.io 
.. _XNAT: https://xnat.org 
.. _BIDS: https://bids.neuroimaging.io 
