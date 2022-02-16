Arcana
======

Arcana_ (Abstraction of Repository-Centric ANAlysis) is Python framework for
analysing datasets "in-place", i.e. pulling data from a data store, performing
computations on it, and then uploading the processed data alongside the original.
Derivatives are generated incrementally, allowing intermediate products to be
manually checked before subsequent stages are processed.

All processing is performed by the Pydra_ workflow engine, which enables tasks
to be spread over multiple cores or submitted to job-schedulers typically used
by high-performance clusters (i.e. SLURM and SGE).

Guide
-----

.. toctree::
   :maxdepth: 2

   installation
   data_model
   processing
   deployment
   developer
   api

|
.. note::
   For the legacy version of Arcana as described in
   *Close TG, et. al. Neuroinformatics. 2020 18(1):109-129. doi:* `<10.1007/s12021-019-09430-1>`_
   please see `<https://github.com/MonashBI/arcana>`_.
   Conceptually, the legacy version and the versions in this repository (version >= 2.0) are similar.
   However, instead of Nipype, versions >= 2 use the Pydra_ workflow engine (Nipype's successor)
   and the syntax has been rewritten from scratch to make it more streamlined and intuitive.


.. _Arcana: http://arcana.readthedocs.io
.. _Pydra: http://pydra.readthedocs.io
.. _XNAT: http://xnat.org
.. _BIDS: http://bids.neuroimaging.io/
.. _`Environment Modules`: http://modules.sourceforge.net
