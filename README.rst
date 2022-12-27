Arcana - Core
=============
.. image:: https://github.com/ArcanaFramework/arcana-core/actions/workflows/tests.yml/badge.svg
   :target: https://github.com/ArcanaFramework/arcana-core/actions/workflows/tests.yml
.. image:: https://codecov.io/gh/ArcanaFramework/arcana-core/branch/main/graph/badge.svg?token=UIS0OGPST7
   :target: https://codecov.io/gh/ArcanaFramework/arcana
.. image:: https://img.shields.io/pypi/pyversions/arcana-core.svg
   :target: https://pypi.python.org/pypi/arcana-core/
   :alt: Supported Python versions
.. image:: https://img.shields.io/pypi/v/arcana-core.svg
   :target: https://pypi.python.org/pypi/arcana-core/
   :alt: Latest Version
.. image:: https://readthedocs.org/projects/arcana/badge/?version=latest
  :target: http://arcana.readthedocs.io/en/latest/?badge=latest
  :alt: Documentation Status


Abstraction of Repository-Centric ANAlysis (Arcana_) is Python framework
for "repository-centric" analyses of study groups (e.g. NeuroImaging
studies) built on the Pydra_ dataflow engine.

Arcana_ interacts closely with a data store (e.g. XNAT repository or BIDS dataset),
storing intermediate outputs, along with the parameters used to derive them,
for reuse by subsequent analyses.

Analysis workflows are constructed and executed using the Pydra_
package, and can either be run locally or submitted to HPC
schedulers using Pydra_'s execution plugins. For a requested analysis
output, Arcana determines the required processing steps by querying
the repository to check for missing intermediate outputs before
constructing the workflow graph.

Documentation
-------------

Detailed documentation on Arcana can be found at https://arcana.readthedocs.io

Quick Installation
------------------

Arcana can be installed for Python 3 using *pip*::

    $ pip3 install arcana-core

.. _Arcana: http://arcana.readthedocs.io
.. _Pydra: http://pydra.readthedocs.io
.. _XNAT: http://xnat.org
.. _BIDS: http://bids.neuroimaging.io/
.. _`Environment Modules`: http://modules.sourceforge.net


License
-------

This work is licensed under a
`Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License <http://creativecommons.org/licenses/by-nc-sa/4.0/>`_

.. image:: https://i.creativecommons.org/l/by-nc-sa/4.0/88x31.png
  :target: http://creativecommons.org/licenses/by-nc-sa/4.0/
  :alt: Creative Commons License: Attribution-NonCommercial-ShareAlike 4.0 International

|

*Note: For the legacy version of Arcana as described in
Close TG, et. al. Neuroinformatics. 2020 18(1):109-129. doi:* `<10.1007/s12021-019-09430-1>`_
*please see* `<https://github.com/MonashBI/arcana-legacy>`_.
*Conceptually, the legacy version and the versions in this repository (>=2) are similar.
However, instead of Nipype, v2 uses the Pydra workflow engine (Nipype's successor)
and the syntax has been rewritten from scratch to make it more streamlined and intuitive.*
