
Getting started
===============

Pydra
-----

Pipelines in Arcana are implemented using the Pydra_ dataflow engine, so
before getting started with Arcana it is a good idea to familiarise yourself
with Pydra_'s syntax and concepts. There is short Jupyter notebook tutorial
available at `<https://github.com/nipype/pydra-tutorial>`_, which is a nice
place to start with this.

Software requirements
---------------------

Arcana requires recent version of Python (>=3.8) to run. So you may
need to upgrade your Python version before it is installed. The best way
to install Python depends on your OS:

* Windows - it is strongly recommended to use Anaconda_ to install Python
* Mac - either `Homebrew <https://brew.sh/>`_ or Anaconda_ are good options
* Linux - the native package manager should work ok unless you are using an old distribution, in which case `Linuxbrew <https://docs.brew.sh/Homebrew-on-Linux>`_ is a good option


To deploy Arcana pipelines to Docker_ images XNAT's container service
Docker_ to be installed. Please see the Docker_ docs for how to do this,
`<https://docs.docker.com/engine/install/>`_.

One of the main strengths of Pydra is the ability to link 3rd party tools into
coherent workflows. These are best run within software containers
(e.g. Docker_ or Singularity_), but in cases where that isn't possible (i.e.
nested within other containers or some high-performance computing clusters)
you will obviously need to install these dependencies and ensure they are
on the system path.

Two external tools used for implicit file-format conversions in the
`arcana-medicalimaging` sub-package are

* `Dcm2Niix <https://github.com/rordenlab/dcm2niix>`_
* `Mrtrix3 <https://mrtrix3.readthedocs.io>`_

which will typically need to be installed to use objects in that sub-package


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

* Specify a dataset to work with
* Define columns to access and store data
* Connect columns with pipelines
* Generate derivatives

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

    # Connect pipeline between source and sink
    my_dataset.pipeline(
        'brain_extraction',
        BET(),
        inputs=[('T1w', 'in_file', nifti_gz)],
        outputs=[('brain_mask', 'out_file')])

    # Generate brain mask
    my_dataset.derive('brain_mask')

This will iterate over all imaging sessions in the directory tree, find and
convert T1-weighted images (which contain 'mprage' in their names) from
DICOM to gzipped NIfTI file formats and then execute BET on the converted
files before they are saved back into the directory structure at
``<subject-id>/<timepoint-id>/derivs/brain_mask.nifti_gz``.

Alternatively, the same steps can be performed using the command line


.. code-block:: bash

    $ arcana dataset define 'file///data/my-project'
    $ arcana column add-source 'file///data/my-dataset' T1w '.*mprage.*' medicalimaging:dicom --regex
    $ arcana column add-sink 'file///data/my-dataset' brain_mask medicalimaging:nifti_gz
    $ arcana pipeline add 'file///data/my-dataset' pydra.tasks.fsl.preprocess.bet:BET \
      --input T1w in_file medicalimaging:nifti_gz \
      --output brain_mask out_file medicalimaging:nifti_gz
    $ arcana derive brain_mask


Licence
-------

Arcana is licenced under the "Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International Public License"
(see `LICENCE <https://raw.githubusercontent.com/Australian-Imaging-Service/arcana/master/LICENSE>`_).
Non-commercial usage is permitted without restriction except attribution of the Arcana package.
Commercial usage is encouraged, but consent from the authors for particular
use cases must be granted first (see `AUTHORS <https://raw.githubusercontent.com/Australian-Imaging-Service/arcana/master/AUTHORS>`_).



.. _Pydra: http://pydra.readthedocs.io
.. _Anaconda: https://www.anaconda.com/products/individual
.. _Docker: https://www.docker.com/
.. _Singularity: https://sylabs.io/guides/3.0/user-guide/index.html