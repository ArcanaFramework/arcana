
Getting started
===============

Pydra
-----

Pipelines in Arcana are implemented in and executed with the Pydra_ dataflow
engine, so before getting started with Arcana it is a good idea to familiarise
yourself with Pydra_'s syntax and concepts. There is short Jupyter notebook
tutorial available at `<https://github.com/nipype/pydra-tutorial>`_, which is a
nice place to start with this after the reading the
`official docs <https://pydra.readthedocs.io>`_.

Software requirements
---------------------

Arcana requires a recent version of Python (>=3.8) to run. So you may
need to upgrade your Python version before it is installed. The best way
to install Python depends on your OS:

* Windows - it is very strongly recommended to use Anaconda_ to install Python because it will manage C dependencies as well
* Mac - either `Homebrew <https://brew.sh/>`_ or Anaconda_ are good options
* Linux - the native package manager should work ok unless you are using an old Linux distribution that doesn't support Python 3.8, in which case `Linuxbrew <https://docs.brew.sh/Homebrew-on-Linux>`_ is a good option


To deploy Arcana pipelines to Docker_ images XNAT's container service,
Docker_ needs to be installed. Please see the Docker_ docs for how to do this,
`<https://docs.docker.com/engine/install/>`_ for your system.

One of the main strengths of Pydra is the ability to link 3rd party tools
together into coherent workflows. 3rd party tools are best run within software
containers (e.g. Docker_ or Singularity_), but in cases where that isn't possible
(i.e. when nested within other containers without access to Docker socket or
on some high-performance computing clusters) you will obviously need to have
installed these dependencies on the system and ensure they are on the `system
path <https://learn.sparkfun.com/tutorials/configuring-the-path-system-variable/all>`_.

Two command-line tools that the the `arcana-medimage` sub-package uses
for implicit file-format conversions are

* `Dcm2Niix <https://github.com/rordenlab/dcm2niix>`_
* `Mrtrix3 <https://mrtrix.readthedocs.io/en/latest/index.html>`_

Both these packages can be installed using Home/LinuxBrew (you will need to tap
``MRtrix3/mrtrix3``) and Anaconda_ (use the ``conda-forge`` and ``mrtrix3``
repositories for Dcm2Niix and MRtrix3 respectively).


Installation
------------

Arcana can be installed along with its Python dependencies from the
`Python Package Index <http://pypi.org>`_ using *Pip3*

.. code-block:: console

    $ pip3 install arcana


Basic usage
-----------

Arcana is implemented in Python, and can be accessed either via it's
API or via the command-line interface (CLI).

The basic usage pattern is

#. Define a dataset to work with (see :ref:`datasets`)
#. Specify columns in the dataset to access data from and store data to (see :ref:`data_columns`)
#. Connect a `Pydra task or workflow <https://pydra.readthedocs.io/en/latest/components.html#dataflows-components-task-and-workflow>`_, or an analysis class between columns (see :ref:`Analysis classes`)
#. Select derivatives to generate (see :ref:`derivatives`)

For example, given a dataset stored within the ``/data/my-dataset`` directory,
which contains two-layers of sub-directories, for subjects and sessions
respectively, FSL's Brain Extraction Tool (BET) can be executed
over all sessions using the command line interface

.. code-block:: console

    # Define dataset
    $ arcana dataset define '/data/my-project' subject session

    # Add source column to select a single T1-weighted image in each session subdirectory
    $ arcana dataset add-source '/data/my-dataset' T1w '.*mprage.*' medimage:Dicom --regex

    # Add sink column to store brain mask
    $ arcana dataset add-sink '/data/my-dataset' brain_mask medimage:NiftiGz

    # Apply BET Pydra task, connecting it between the source and sink
    $ arcana apply pipeline '/data/my-dataset' pydra.tasks.fsl.preprocess.bet:BET \
      --arg name brain_extraction \
      --input T1w in_file medimage:NiftiGz \
      --output brain_mask out_file .

    # Derive brain masks for all imaging sessions in dataset
    $ arcana derive column '/data/my-dataset' brain_maskAPI

This code will iterate over all imaging sessions in the directory tree, find and
convert T1-weighted images (which contain 'mprage' in their names) from
DICOM into the required gzipped NIfTI format, and then execute BET on the converted
files before they are saved back into the directory structure at
``<subject-id>/<session-id>/derivs/brain_mask.nii.gz``.

Alternatively, the same steps can be performed using the Python API:

.. code-block:: python

    # Import arcana module
    from pydra.tasks.fsl.preprocess.bet import BET
    from arcana.core.data import Dataset
    from arcana.medimage.data import Clinical
    from fileformats.medimage.data import Dicom, NiftiGz

    # Define dataset
    my_dataset = Dataset.load('/data/my-dataset', space=Clinical,
                              hierarchy=['subject', 'session'])

    # Add source column to select a single T1-weighted image in each session subdirectory
    my_dataset.add_source('T1w', '.*mprage.*', datatype=Dicom, is_regex=True)

    # Add sink column to store brain mask
    my_dataset.add_sink('brain_mask', 'derivs/brain_mask', datatype=NiftiGz)

    # Apply BET Pydra task, connecting it between the source and sink
    my_dataset.apply_pipeline(
        BET(name='brain_extraction'),
        inputs=[('T1w', 'in_file', NiftiGz)],  # Specify required input format
        outputs=[('brain_mask', 'out_file')])  # Output datatype matches stored so can be omitted

    # Derive brain masks for all imaging sessions in dataset
    my_dataset['brain_mask'].derive()


Applying an Analysis class instead of a Pydra task/workflow follows the same
steps up to 'add-source' (sinks are automatically added by the analysis class).
The following example applies methods for analysing T1-weighted MRI images to the
dataset, then calls the methods calculates the average cortical thickness for
each session of each subject.

.. code-block:: console

    $ arcana apply analysis '/data/my-project' bids.mri:T1wAnalysis
    $ arcana derive column '/data/my-project' avg_cortical_thickness


Doing the same steps via the Python API provides convenient access to the
generated data, which a histogram of the distribution over all subjects at
Timepoint 'T3' can be plotted.


.. code-block:: python

    import matplotlib.pyplot as plt
    from arcana.analyses.bids.mri import T1wAnalysis

    # Apply the T1wAnalysis class to the dataset
    my_dataset.apply(T1wAnalysis())

    # Generate the average cortical thickness derivative that was added by
    # the T1wAnalysis class
    my_dataset['avg_cortical_thickness'].derive()

    # Get all members at the 'T3' timepoint. Indexing of a column can either
    # be a single arg in order to use the IDs for the row_frequency of the column
    # ('session') in this case, or the rank of the data space
    plt.histogram(my_dataset['avg_cortical_thickness']['T3', None, :])


.. note::

    When referencing objects within the ``arcana`` package from the CLI such
    as file-datatype classes or data spaces (see :ref:`data_spaces`), the
    standard ``arcana.*.`` prefix can be dropped, e.g. ``medimage:Dicom``
    instead of the full path ``fileformats.medimage.data:Dicom``.
    Classes installed outside of the Arcana package, should be referred to
    with their full import path.


Licence
-------

Arcana >=v2.0 is licenced under the `Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International Public License <https://creativecommons.org/licenses/by-nc-sa/4.0/>`_
(see `LICENCE <https://raw.githubusercontent.com/Australian-Imaging-Service/arcana/master/LICENSE>`_).
Non-commercial usage is permitted freely on the condition that Arcana is
appropriately acknowledged in related publications. Commercial usage is encouraged,
but permission from the authors for specific uses must be granted first
(see `AUTHORS <https://raw.githubusercontent.com/Australian-Imaging-Service/arcana/master/AUTHORS>`_).



.. _Pydra: http://pydra.readthedocs.io
.. _Anaconda: https://www.anaconda.com/products/individual
.. _Docker: https://www.docker.com/
.. _Singularity: https://sylabs.io/guides/3.0/user-guide/index.html
