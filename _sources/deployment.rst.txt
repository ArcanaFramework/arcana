Deployment
==========

Arcana provides tools for deploying pipelines in Docker containers
that can be run in XNAT's `container service <https://wiki.xnat.org/container-service/>`_. Pipelines
can be built done on an individual basis or as part of a wider a suite (e.g.
`Australian Imaging Service Pipelines <https://github.com/australian-imaging-service/pipelines-core>`_).
As well as building Docker images, the deployment workflow includes
procedures to test and generate documentation.

Command definitions
-------------------

The XNAT container service uses `command configuration files <https://wiki.xnat.org/container-service/command-resolution-122978876.html>`_
saved in the `org.nrg.commands` image label to resolve metadata for the pipelines
that available on a given Docker image. The :meth:`.XnatViaCS.generate_xnat_command`
method is used to generate the JSON metadata to be saved in this field.

There are four key fields that will determine the functionality of the command
(the rest are metadata fields that are exposed to the XNAT UI):

* ``task``
* ``inputs``
* ``outputs``
* ``parameters``

The ``task`` keyword argument should be the path to an installed
Python module containing a Pydra task followed by a colon and the name of
the task, e.g. ``pydra.tasks.fsl.preprocess.fast:Fast``. Note that Arcana
will attempt to resolve the package that contains the Pydra task and install the
same version (including local development versions) within the Anaconda_ environment
in the image. ``inputs`` and ``parameters`` expose text boxes in the XNAT dialog when
the pipelines are run. ``outputs`` determine where the outputs will
be stored in the XNAT data tree.

Inputs prompt the user to enter selection criteria for
input data and are used by the entrypoint of the Docker containers to add
source columns to the dataset (see :ref:`data_columns`). They are specified by
4-tuple consisting of

* name of field in the pydra task input interface
* datatype required by pydra task
* description of input that will be exposed to the XNAT UI
* the row row_frequency of the column (see :ref:`data_spaces` and :ref:`data_columns`)

Parameters are passed directly through the pipeline add method (see :ref:`applying_workflows`) that
is run in the container, and consist of a 2-tuple with

* name of field in the pydra task input interface
* description of parameters that will be exposed to the XNAT UI

Outputs do not show up in the XNAT dialog and are specified by a 3-tuple:

* name of field in the pydra task output interface
* datatype produced by pydra task
* destination path (slashes are permitted interpreted as a relative path from the derivatives root)

.. .. code-block:: python

..     import json
..     from arcana.xnat.deploy import XnatCommand
..     from arcana.medimage.data import Clinical
..     from fileformats.medimage.data import NiftiGz

..     xnat_command = XnatCommand(
..         name='example_pipeline',
..         task='pydra.tasks.fsl.preprocess.fast:FAST',
..         image_tag='example/0.1',
..         description=(
..             "FAST (FMRIB's Automated Segmentation Tool) segments a 3D image of "
..             "the brain into different tissue types (Grey Matter, White Matter, "
..             "CSF, etc.), whilst also correcting for spatial intensity variations "
..             "(also known as bias field or RF inhomogeneities)."),
..         version='6.0-1',
..         info_url='https://fsl.fmrib.ox.ac.uk/fsl/fslwiki/FAST',
..         inputs={
..             "field": 'in_files', NiftiGz, 'File to segment', 'session'),
..             ('number_of_classes', int, 'Number of classes', 'session')],
..         outputs=[
..             ('tissue_class_files', NiftiGz, 'fast/tissue-classes'),
..             ('partial_volume_map', NiftiGz, 'fast/partial-volumes'),
..             ('partial_volume_files', NiftiGz, 'fast/partial-volume-files'),
..             ('bias_field', NiftiGz, 'fast/bias-field'),
..             ('probability_maps', NiftiGz, 'fast/probability-map')],
..         parameters=[
..             ('use_priors', 'Use priors'),
..             ('bias_lowpass', 'Low-pass filter bias field')],
..         configuration=[  # If different from the Pydra task
..             ('output_biasfield', True),
..             ('output_biascorrected', True),
..             ('bias_lowpass', 5.0)],
..         row_frequency='session')

..         with open("/path/to/a/file", "w") as f:
..             json.dump(f, xnat_command.make_json())

When working with the CLI, command configurations are stored in YAML_ format,
with keys matching the arguments of :meth:`XnatViaCS.generate_xnat_command`.

.. note::
    ``image_tag`` and ``registry`` are omitted from the YAML representation
    of the commands as they are provided by the image configuration
    (see :ref:`Building`)


Building
--------

Dockerfiles for pipeline images are created using Neurodocker_
and can therefore work with any Debian/Ubuntu or Red-Hat based images
(ensuring that the value for ``base_image>package_manager`` is set to the correct value,
i.e.  ``"apt"`` for Debian based or ``"yum"`` for Red-Hat based). Arcana installs
itself into the Docker image within an Anaconda_ environment named "arcana". Therefore,
it shouldn't conflict with packages on existing Docker images for third-party
pipelines.

Extending the YAML_ format used to define the command configurations,
the full configuration required to build an XNAT docker image looks like

.. code-block:: yaml

    title: FMRIB Scientific Library (FSL)
    version:
        package: &package_version '6.0.1'
        build: '1'
    authors:
        - name: Thomas G. Close
          email: thomas.close@sydney.edu.au
    base_image:
        name: brainlife/fsl'
        tag: *package_version
        package_manager: apt
    packages:
        neurodocker:
            dcm2niix: v1.0.20201102
        pip:
            pydra-dcm2niix:  # Uses the default version on PyPI
    docs:
        info_url: https://fsl.fmrib.ox.ac.uk/fsl/fslwiki
    command:
        task: pydra.tasks.fsl.preprocess.fast:FAST
        description:
            FAST (FMRIBs Automated Segmentation Tool) segments a 3D image of
            the brain into different tissue types (Grey Matter, White Matter,
            CSF, etc.), whilst also correcting for spatial intensity variations
            (also known as bias field or RF inhomogeneities).
        inputs:
            in_files:
              datatype: medimage/nifti-gz
              column_defaults:
                datatype: medimage/dicom-series
              help: Anatomical image to segment into different tissues
        outputs:
            tissue_classes:
              datatype: medimage/nifti-gz
              path: fast/tissue-classes
              help: Segmented tissue classes
            probability_maps:
              datatype: medimage/nifti-gz
              path: fast/probability-map
              help: Probability maps for tissue classes
        parameters:
            use_priors:
              help: Use priors in tissue estimation
            bias_lowpass:
              help: Low-pass filter bias field
        configuration:
            output_biasfield: true
            bias_lowpass: 5.0
        row_frequency: session
    arcana_spec_version: 1.0


The CLI command to build the image from the YAML_ configuration is

.. code-block:: console

    $ arcana deploy make-app xnat:XnatApp 'your-pipeline-config.yml'
    Successfully built "FSL" image with ["fast"] commands

To build a suite of pipelines from a series of YAML_ files stored in a directory tree
simply provide the root directory instead and Arcana will walk the sub-directories
and attempt to build any YAML_ files it finds, e.g.

.. code-block:: console

    $ arcana deploy make-app xnat:XnatApp 'config-root-dir'
    ./config-root-dir/mri/neuro/fsl.yml: FSL [fast]
    ./config-root-dir/mri/neuro/mrtrix3.yml: MRtrix3 [dwi2fod, dwi2tensor, tckgen]
    ./config-root-dir/mri/neuro/freesurfer.yml: Freesurfer [recon-all]
    ...


Testing
-------

After an image has been built successfully, it can be tested against previously
generated results to check for consistency with previous versions. This can be
particularly useful when updating dependency versions. Tests that don't match
previous results within a given tolerance will be flagged for manual review.

To avoid expensive runs when not necessarily (particularly within CI/CD
pipelines), in the case that the provenance data saved along the generated
reference data will be checked before running the pipelines. If the provenance
data would be unchanged (including software dependency versions), then the
pipeline test will be skipped.

Test data, both inputs to the pipeline and reference data to check against
pipeline outputs, need to be stored in separate directories for each command.
Under the pipeline data directory, there should be one or more subdirectories
for different tests of the pipeline, and in each of these subdirectories there
should be an ``inputs`` and an ``outputs`` directory, and optionally a YAML_
file named ``parameters.yml``. Inside the ``inputs`` directory there should be
file-groups named after each input of the pipeline, and likewise in the
``outputs`` directory there should be file-groups named after each output
of the pipeline. Any field inputs or outputs should be placed alongside the
file-groups in a JSON file called ``__fields__.json``.

Specifying two tests ('test1' and 'test2') for the FSL FAST example given above
(see :ref:`Building`) the directory structure would look like:

.. code-block::

     FAST
     ├── test1
     │   ├── inputs
     │   │   └── in_files.nii.gz
     │   ├── outputs
     |   │   └── fast
     |   │       ├── tissue_class_files.nii.gz
     |   │       ├── partial_volumes.nii.gz
     |   │       ├── partial-volume-files.nii.gz
     |   │       ├── bias-field.nii.gz
     |   │       └── probability-map.nii.gz
     │   └── parameters.yml
     └── test2
         ├── inputs
         │   └── in_files.nii.gz
         ├── outputs
         │   └── fast
         │       ├── tissue_class_files.nii.gz
         │       ├── partial_volumes.nii.gz
         │       ├── partial-volume-files.nii.gz
         │       ├── bias-field.nii.gz
         │       └── probability-map.nii.gz
         └── parameters.yml

To run a test via the CLI point the test command to the YAML_ configuration
file and the data directory containing the test data, e.g.

.. code-block:: console

    $ arcana deploy test ./fast.yml ./fast-data
    Pipeline test 'test1' ran successfully and outputs matched saved
    Pipeline test 'test2' ran successfully and outputs matched saved

To run tests over a suite of image configurations in a directory containing a
number of YAML_ configuration files (i.e. same as building) simply provide the
directory to ``arcana deploy test`` instead of the path to the YAML_ config
file and supply a directory tree containing the test data, with matching
sub-directory structure to the configuration dir. For example, given the following
directory structure for the configuration files

.. code-block::

    mri
    └── neuro
        ├── fsl.yml
        ├── mrtrix3.yml
        ...

The test data should be laid out like

.. code-block::

    mri-data
    └── neuro
        ├── fsl
        │   └── fast
        |       ├── test1
        |       │   ├── inputs
        |       │   │   └── in_files.nii.gz
        |       │   ├── outputs
        |       |   │   └── fast
        |       |   │       ├── tissue_class_files.nii.gz
        |       |   │       ├── partial_volumes.nii.gz
        |       |   │       ├── partial-volume-files.nii.gz
        |       |   │       ├── bias-field.nii.gz
        |       |   │       └── probability-map.nii.gz
        |       │   └── parameters.yml
        |       └── test2
        |           ├── inputs
        |           │   └── in_files.nii.gz
        |           ├── outputs
        |           │   └── fast
        |           │       ├── tissue_class_files.nii.gz
        |           │       ├── partial_volumes.nii.gz
        |           │       ├── partial-volume-files.nii.gz
        |           │       ├── bias-field.nii.gz
        |           │       └── probability-map.nii.gz
        |           └── parameters.yml
        └── mrtrix3
            ├── dwi2fod
            |   ├── test1
            |   |   ├── inputs
        ...

Like in the case of a single YAML_ configuration file, the CLI command to test
a suite of image/command configurations is.

.. code-block:: console

    $ arcana deploy test ./mri ./mri-data --output test-results.json
    ...E..F..

While not strictly necessary, it is strongly advised to store test data alongside
image/command configurations inside some kind of version control. However, storing
large files inside vanilla Git repositories is **not recommended**, therefore, you
will probably want to use one of the extensions designed for dealing with large
files:

* `git-lfs <https://git-lfs.github.com/>`_ - integrates with GitHub but GitHub requires you to pay for storage/egest
* `git-annex <https://git-annex.branchable.com/>`_ - complicated to set up and use, even for experienced Git users, but much more flexible in your storage options.


Autodocs
--------

Documentation can be automatically generated using from the
pipeline configuration YAML_ files (see :ref:`Building`) using

.. code-block:: console

    $ arcana deploy docs <path-to-yaml-or-directory> <docs-output-dir>

Generated HTML documents will be placed in the output dir, with pipelines
organised hierarchically to match the structure of the source directory.


.. _Anaconda: https://www.anaconda.com/
.. _Neurodocker: https://github.com/ReproNim/neurodocker
.. _YAML: https://yaml.org
