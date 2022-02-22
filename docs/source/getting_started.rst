
Getting started
===============

Pydra
-----

Pipelines in Arcana are implemented in, and executed with, the Pydra_ dataflow
engine, so before getting started with Arcana it is a good idea to familiarise
yourself with Pydra_'s syntax and concepts. There is short Jupyter notebook
tutorial available at `<https://github.com/nipype/pydra-tutorial>`_, which is a
nice place to start with this.

Software requirements
---------------------

Arcana requires recent version of Python (>=3.8) to run. So you may
need to upgrade your Python version before it is installed. The best way
to install Python depends on your OS:

* Windows - it is strongly recommended to use Anaconda_ to install Python
* Mac - either `Homebrew <https://brew.sh/>`_ or Anaconda_ are good options
* Linux - the native package manager should work ok unless you are using an old distribution, in which case `Linuxbrew <https://docs.brew.sh/Homebrew-on-Linux>`_ is a good option


To deploy Arcana pipelines to Docker_ images XNAT's container service,
Docker_ needs to be installed. Please see the Docker_ docs for how to do this,
`<https://docs.docker.com/engine/install/>`_.

One of the main strengths of Pydra is the ability to link 3rd party tools
together into coherent workflows. 3rd party tools are best run within software
containers (e.g. Docker_ or Singularity_), but in cases where that isn't possible
(i.e. when nested within other containers without access to Docker socket or
on some high-performance computing clusters) you will obviously need to have
installed these dependencies on the system and ensure they are on the `system
path <https://learn.sparkfun.com/tutorials/configuring-the-path-system-variable/all>`_.

Two command-line tools that the the `arcana-medicalimaging` sub-package uses
for implicit file-format conversions are

* `Dcm2Niix <https://github.com/rordenlab/dcm2niix>`_
* `Mrtrix3 <https://mrtrix3.readthedocs.io>`_

Both these packages can be installed using \*Brew (you will need to tap
``MRtrix3/mrtrix3``) and Anaconda_ (use the ``conda-forge`` and ``mrtrix3``
packages for Dcm2Niix and MRtrix3 respectively).


Installation
------------

Arcana can be installed along with its Python dependencies from the
`Python Package Index <http://pypi.org>`_ using *Pip3*

.. code-block:: bash

    $ pip3 install arcana


Basic usage
-----------

Arcana is implemented in Python, and can be accessed either via it's
API or via the command-line interface (CLI).

The basic usage pattern is

* Specify a :ref:`Dataset` to work with
* Define columns to access and store data
* Apply a Pydra_ task or workflow, or an :ref:`Analysis classes` to the dataset
* Generate selected derivatives

For example, to execute FSL's Brain Extraction Tool (BET) over all subjects of
a dataset stored within the ``/data/my-dataset`` directory (which contains
two-layers of sub-directories, for subjects and longitudinal time-points
respectively) via the API

.. code-block:: python

    # Import arcana module
    from pydra.tasks.fsl.preprocess.bet import BET
    from arcana.core.data import Dataset
    from arcana.data.formats.medicalimaging import dicom, nifti_gz

    # Load dataset
    my_dataset = Dataset('file///data/my-dataset', ['subject', 'session'])

    # Add source column to select T1-weighted images in each sub-directory
    my_dataset.add_source('T1w', '.*mprage.*', format=dicom, is_regex=True)

    # Add sink column to store brain mask
    my_dataset.add_sink('brain_mask', 'derivs/brain_mask', format=nifti_gz)

    # Apply BET Pydra task, connecting it betwee between the source and sink
    my_dataset.pipeline(
        'brain_extraction',
        BET(),
        inputs=[('T1w', 'in_file', nifti_gz)],
        outputs=[('brain_mask', 'out_file')])

    # Generate brain mask derivative
    my_dataset.derive('brain_mask')

This will iterate over all imaging sessions in the directory tree, find and
convert T1-weighted images (which contain 'mprage' in their names) from
DICOM to gzipped NIfTI file formats and then execute BET on the converted
files before they are saved back into the directory structure at
``<subject-id>/<timepoint-id>/derivs/brain_mask.nifti_gz``.

Alternatively, the same steps can be performed using the command line

.. code-block:: bash

    $ arcana dataset define 'file///data/my-project' subject session
    $ arcana column add-source 'file///data/my-dataset' T1w '.*mprage.*' medicalimaging:dicom --regex
    $ arcana column add-sink 'file///data/my-dataset' brain_mask medicalimaging:nifti_gz
    $ arcana pipeline add 'file///data/my-dataset' pydra.tasks.fsl.preprocess.bet:BET \
      --input T1w in_file medicalimaging:nifti_gz \
      --output brain_mask out_file medicalimaging:nifti_gz
    $ arcana derive brain_mask

Applying an Analysis class instead of a Pydra task/workflow follows the same
steps up to the 'add-sink' call (sinks are automatically added by the analysis).
The following example applies a generic T1w analysis class to the dataset,
calculates the average cortical thickness for each session of each subject,
and then plots a histogram of the distribution at Timepoint 'T3'.


.. code-block:: python

    import matplotlib.pyplot as plt
    from arcana.analyses.bids.mri import T1wAnalysis

    # Apply the T1wAnalysis class to the dataset
    my_dataset.apply(T1wAnalysis())

    # Generate the average cortical thickness derivative that was added by
    # the T1wAnalysis class
    my_dataset.derive('avg_cortical_thickness')

    plt.histogram(my_dataset['avg_cortical_thickness'][:, 'T3'])


To apply the Analysis class and derive the metric via the command line you can
use

.. code-block:: bash

    $ arcana analysis apply 'file///data/my-project' bids.mri:T1wAnalysis
    $ arcana derive 'file///data/my-project' avg_cortical_thickness


Licence
-------

Arcana >=v2.0 is licenced under the `Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International Public License <https://creativecommons.org/licenses/by-nc-sa/4.0/>`_
(see `LICENCE <https://raw.githubusercontent.com/Australian-Imaging-Service/arcana/master/LICENSE>`_).
Non-commercial usage is permitted freely on the condition that Arcana is
appropriately acknowledged in related publications. Commercial usage is encouraged,
but consent from the authors for particular use cases must be granted first
(see `AUTHORS <https://raw.githubusercontent.com/Australian-Imaging-Service/arcana/master/AUTHORS>`_).



.. _Pydra: http://pydra.readthedocs.io
.. _Anaconda: https://www.anaconda.com/products/individual
.. _Docker: https://www.docker.com/
.. _Singularity: https://sylabs.io/guides/3.0/user-guide/index.html