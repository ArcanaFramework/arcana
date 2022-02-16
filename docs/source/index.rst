Arcana
======
.. image:: https://github.com/australian-imaging-service/arcana/actions/workflows/tests.yml/badge.svg
   :target: https://github.com/australian-imaging-service/arcana/actions/workflows/tests.yml
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


Arcana_ (Abstraction of Repository-Centric ANAlysis) is Python framework
for "repository-centric" analyses of study groups (e.g. NeuroImaging
studies) built on the Pydra_ workflow engine. The framework handles
all interactions with a data stores (e.g. repositories), storing
derivatives, along with the parameters used to derive them, in the data store
for reuse by subsequent analyses. Currently, data stores can either be file
systems or XNAT_ repositories.

Derivatives are generated using workflows constructed and run within the Pydra_
workflow engine. Therefore, they can be executed locally or submitted to HPC
schedulers using Pydra_'s various execution plugins. For a requested analysis
output, Arcana determines the required processing steps by querying
the data store to check for missing outputs prerequisite derivatives before
constructing workflows to perform them.

Guide
-----

.. toctree::
   :maxdepth: 2

   installation
   data_model
   processing
   deployment
   developer


.. note::
   For the legacy version of Arcana as described in
   *Close TG, et. al. Neuroinformatics. 2020 18(1):109-129. doi:* `<10.1007/s12021-019-09430-1>`_
   please see `<https://github.com/MonashBI/arcana>`_.
   Conceptually, the legacy version and the versions in this repository (>=2) are very similar.
   However, instead of Nipype, v2 uses the Pydra workflow engine (Nipype's successor)
   and the syntax has been rewritten from scratch to make it more streamlined and intuitive.


.. _Arcana: http://arcana.readthedocs.io
.. _Pydra: http://pydra.readthedocs.io
.. _XNAT: http://xnat.org
.. _BIDS: http://bids.neuroimaging.io/
.. _`Environment Modules`: http://modules.sourceforge.net
