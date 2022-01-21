Arcana
======
.. image:: https://github.com/australian-imaging-service/arcana/actions/workflows/test.yml/badge.svg
   :target: https://github.com/australian-imaging-service/arcana/actions/workflows/test.yml
.. image:: https://codecov.io/gh/australian-imaging-service/arcana/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/australian-imaging-service/arcana
.. image:: https://img.shields.io/pypi/pyversions/arcana.svg
   :target: https://pypi.python.org/pypi/arcana/
   :alt: Supported Python versions
.. image:: https://img.shields.io/pypi/v/arcana.svg
   :target: https://pypi.python.org/pypi/arcana/
   :alt: Latest Version
.. image:: https://readthedocs.org/projects/arcana/badge/?version=latest
  :target: http://arcana.readthedocs.io/en/latest/?badge=latest
  :alt: Documentation Status


Abstraction of Repository-Centric ANAlysis (Arcana_) is Python framework
for "repository-centric" analyses of study groups (e.g. NeuroImaging
studies) built on Pydra_.

..note::
  For the legacy version of Arcana as described in
  *Close TG, et. al. Neuroinformatics. 2020 18(1):109-129. doi:* `<10.1007/s12021-019-09430-1>`_
  please see `<https://github.com/MonashBI/arcana>`_.
  Conceptually the legacy version and v2 are similar but v2 uses the Pydra
  engine instead of Nipype and the syntax has been rewritten from scratch to
  streamline it and make it more intuitive.*

Arcana_ interacts closely with a repository, storing intermediate
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


User/Developer Guide
--------------------

.. toctree::
    :maxdepth: 2

    installation
    design
    example
