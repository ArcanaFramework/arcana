Arcana
======
.. image:: https://github.com/ArcanaFramework/arcana/actions/workflows/tests.yml/badge.svg
   :target: https://github.com/ArcanaFramework/arcana/actions/workflows/tests.yml
   :alt: Tests
.. image:: https://codecov.io/gh/ArcanaFramework/arcana/branch/main/graph/badge.svg?token=UIS0OGPST7
   :target: https://codecov.io/gh/ArcanaFramework/arcana
   :alt: Codecov
.. image:: https://img.shields.io/pypi/pyversions/arcana.svg
   :target: https://pypi.python.org/pypi/arcana/
   :alt: Python versions
.. image:: https://img.shields.io/pypi/v/arcana.svg
   :target: https://pypi.python.org/pypi/arcana/
   :alt: Latest Version
.. image:: https://github.com/ArcanaFramework/arcana/actions/workflows/docs.yml/badge.svg
   :target: https://arcanaframework.github.io/arcana
   :alt: Docs

Abstraction of Repository-Centric ANAlysis (Arcana_) is Python framework
for "repository-centric" analyses of data tree (e.g. NeuroImaging
studies) built on the Pydra_ dataflow engine.

Arcana_ manages all interactions with "store" the data tree is stored in via adapter layers
designed for specific repository software or data structures (e.g. XNAT or BIDS).
Intermediate outputs are stored, along with the parameters used to derive them,
back into the store for reuse by subsequent analysis steps.

Analysis workflows are constructed and executed using the Pydra_ dataflow
API, and can either be run locally or submitted to cloud or HPC clusters
using Pydra_'s various execution plugins. For a requested output, Arcana determines the
required processing steps by querying the store to check for missing intermediate
outputs and parameter changes before constructing the required workflow graph.

Documentation
-------------

Detailed documentation on Arcana can be found at https://arcana.readthedocs.io


Quick Installation
------------------

Arcana-core can be installed for Python 3 using *pip*::

    $ python3 -m pip install arcana


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
*Conceptually, the legacy version and the versions in this repository are similar.
However, instead of Nipype, later versions use the Pydra dataflow engine (Nipype's successor)
and the syntax has been rewritten from scratch to make it more streamlined and intuitive.*

Acknowledgements
~~~~~~~~~~~~~~~~

The authors acknowledge the facilities and scientific and technical assistance of the National Imaging Facility, a National Collaborative Research Infrastructure Strategy (NCRIS) capability.


.. _Arcana: http://arcana.readthedocs.io
.. _Pydra: http://pydra.readthedocs.io
.. _XNAT: http://xnat.org
.. _BIDS: http://bids.neuroimaging.io/
.. _`Environment Modules`: http://modules.sourceforge.net
