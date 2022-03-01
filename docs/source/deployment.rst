Deployment
==========

Arcana provides tools for deploying pipelines in Docker containers
that can be run in XNAT's `container service <https://wiki.xnat.org/container-service/>`_. Pipelines
can be built done on an individual basis or as part of a wider a suite (e.g.
`Australian Imaging Service Pipelines <https://github.com/australian-imaging-service/pipelines-core>`_).
As well as building Docker images, the deployment workflow includes
procedures to test and generate documentation.

Building
--------

Dockerfiles for pipeline images are created using `Neurodocker <https://github.com/ReproNim/neurodocker>`_
and can therefore work with any Debian/Ubuntu or Red-Hat based images
(using a value for ``package_manager`` keyword argument of ``"apt"`` for
Debian-based or ``"yum"`` for Red Hat). Arcana installs itself into the Docker image
within an Anaconda_ environment named "arcana". Therefore, it won't typically
conflict with packages on existing Docker images for third-party pipelines
unless they are also installed using Anaconda.

The :meth:`.XnatViaCS.generate_xnat_command` method is used to create the
`command configuration files <https://wiki.xnat.org/container-service/command-resolution-122978876.html>`_
that are read by the XNAT container service to resolve the available pipelines
installed on an image. There are four key fields that will determine the
functionality of the command (the rest are metadata fields that are exposed to
the UI):

* pydra_task
* inputs
* outputs
* parameters 

The ``pydra_task`` keyword argument should be the path to an installed
Python module containing a Pydra task followed by a colon and the name of
the task, e.g. ``pydra.tasks.fsl.preprocess.fast:Fast``. Note that Arcana
will attempt to resolve the package that contains the Pydra task and install the
same version (including local development versions) within the Anaconda_ environment
in the image.

The inputs and parameters fields expose input text boxes to the user when
the pipelines are run. Inputs prompt the user to enter selection criteria for
input data and are used by the entrypoint of the Docker containers to add
source columns to the dataset (see :ref:`data_columns`). Parameter inputs are
passed directly through the pipeline add method (see :ref:`Pipelines`).

.. code-block:: python

    from arcana.data.stores.xnat import XnatViaCS
    from arcana.data.spaces.medicalimaging import ClinicalTrial
    from arcana.data.formats.medicalimaging import NiftiGz


    XnatViaCS.generate_xnat_command(
        pipeline_name='example_pipeline',
        pydra_task='pydra.tasks.fsl.preprocess.fast:Fast',
        image_tag='example/0.1',
        inputs=[
            ('in_files', NiftiGz, 'File to segment', ClinicalTrial.session),
            ('number_of_classes', int, 'Number of classes', ClinicalTrial.session)],
        outputs=[])


Testing
-------

* Testing individual images via API
* Testing individual images via CLI
* Testing suite via CLI


Documenting
-----------

Documentation can be automatically generated using the metadata saved in the
pipeline definitions.


.. _Anaconda: https://www.anaconda.com/