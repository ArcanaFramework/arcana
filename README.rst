Arcana
======

.. image:: https://travis-ci.org/MonashBI/arcana.svg?branch=master
  :target: https://travis-ci.org/MonashBI/arcana
.. image:: https://codecov.io/gh/MonashBI/arcana/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/MonashBI/arcana
.. image:: https://img.shields.io/pypi/pyversions/arcana.svg
  :target: https://pypi.python.org/pypi/arcana/
  :alt: Supported Python versions
.. image:: https://img.shields.io/pypi/v/arcana.svg
  :target: https://pypi.python.org/pypi/arcana/
  :alt: Latest Version
.. image:: https://readthedocs.org/projects/arcana/badge/?version=latest
  :target: http://arcana.readthedocs.io/en/latest/?badge=latest
  :alt: Documentation Status


Abstraction of Repository-Centric ANAlysis (Arcana_) v2 is Python framework
for "repository-centric" analyses of study groups (e.g. NeuroImaging
studies) built on Pydra_.

Arcana_ 2 interacts closely with a repository, storing intermediate
outputs, along with the parameters used to derive them, for reuse by
subsequent analyses. Repositories can either be XNAT_ repositories or
plain file system directories, and a BIDS_ module is under development.

Analysis workflows are constructed and executed using the Pydra_
package, and can either be run locally or submitted to HPC
schedulers using Pydra_'s execution plugins. For a requested analysis
output, Arcana determines the required processing steps by querying
the repository to check for missing intermediate outputs before
constructing the workflow graph. When running in an environment
with `Environment Modules`_ installed,
Arcana manages the loading and unloading of software modules per
pipeline node.

Design
------

Arcana_ 2 is designed with an object-oriented philosophy, with
the acquired and derived data sets along with the analysis pipelines
used to derive the derived data sets encapsulated within "Analysis" classes.

The Arcana_ 2 package itself only provides the abstract *Analysis* and
*MultiAnalysis* base classes, which are designed to be sub-classed to provide
specialised classes representing the analysis that can be performed on specific
types of data (e.g. BoldAnalysis, PetAnalysis). These specific classes can then
be sub-classed further into classes that are specific to a particular analysis,
and integrate complete analysis workflows from preprocessing to statistics.

Installation
------------

Arcana can be installed for Python 3 using *pip*::

    $ pip3 install arcana

.. _Arcana: http://arcana.readthedocs.io
.. _Pydra: http://pydra.readthedocs.io
.. _XNAT: http://xnat.org
.. _BIDS: http://bids.neuroimaging.io/
.. _`Environment Modules`: http://modules.sourceforge.net
