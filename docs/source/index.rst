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
studies) built on Pydra_.

Arcana_ interacts closely with a data stores (repositories), storing intermediate
outputs, along with the parameters used to derive them, for reuse by
subsequent analyses. Data stores can either be file systems, XNAT_ repositories,
and support is available for reading and writing BIDS_ formatted datasets.

Analysis workflows are constructed and executed using the Pydra_
package, and can either be run locally or submitted to HPC
schedulers using Pydra_'s execution plugins. For a requested analysis
output, Arcana determines the required processing steps by querying
the repository to check for missing intermediate outputs before
constructing the workflow graph.


User/Developer Guide
--------------------

.. toctree::
    :maxdepth: 2

    installation
    data_access
    example


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
