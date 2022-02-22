
Getting started
===============

Arcana is implemented in Python, and can be accessed either via it's
API or via the command-line interface (CLI). 

Pipelines are implemented using the Pydra_ dataflow, so
before getting started with Arcana it is a good idea to familiarise yourself
with Pydra_'s syntax. There is short Jupyter notebook tutorial available
at `<https://github.com/nipype/pydra-tutorial>`_, which is a nice place to
start with this.

Requirements
------------

Arcana requires quite a recent version of Python (>=3.8) to run. So you may
need to upgrade your Python version before installation. On Windows, it is
strongly recommended to use an Anaconda 

To deploy Arcana pipelines to Docker images XNAT's container service
Docker to be installed. Please see the Docker docs for how to do this,
`<https://docs.docker.com/engine/install/>`_.

One of the main strengths of Pydra is the ability to link 3rd party tools into
coherent workflows. Obviously these software also need to be installed separately
unless they run within Docker or Singularity containers (recommended).


Installation
------------

Arcana can be installed along with all Python dependencies from the
`Python Package Index <http://pypi.org>`_ using *Pip3*

.. code-block:: bash

    $ pip3 install arcana


Basic usage
-----------


To execute a Pydra_ task/workflow over all nodes of a dataset stored in a
directory file system using the API

.. code-block:: python

    # Import 


.. warning::
    Under construction


Licence
-------

Arcana is licenced under the "Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International Public License"
(see `LICENCE <https://github.com/australian-imaging-service/arcana/LICENCE>`_)


.. _Pydra: http://pydra.readthedocs.io