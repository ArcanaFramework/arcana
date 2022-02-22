.. _home:

Arcana
======

Arcana (Abstraction of Repository-Centric ANAlysis) is framework for
analysing datasets "in-place", i.e. pulling data from a data store to a
(typically neighbouring) computing resource, performing computations on the
data, and then uploading the processed data back to the store alongside the
original. Data store interactions are abstracted by modular handlers, making
worklows portable between different storage systems. Many menial aspects
of workflow design, such as node iteration, format conversions and provenance
handling are also abstracted away from the designer, enabling them to focus on
the core logic of the analysis to be implemented.

This approach has several advantages over typical workflow design, particularly when analysing large datasets:

* Derivatives are kept in central location, avoiding duplication of processing
* Incremental processing works well with manual-QC of intermediate products at key milestones in the workflow (e.g. brain masks)
* Portability and extensibility facilitates the development of shared workflow libraries that can be refined by multiple collaborators to capture the “arcana" of domain-specific data analysis, the obscure knowledge required to apply an appropriate combination of software tools and parameters.

The framework also includes tools for deploying pipelines in Docker images that
can be run in `XNAT's container service <https://wiki.xnat.org/container-service/>`_
or as `BIDS apps <https://bids-apps.neuroimaging.io/>`_. These tools can be used
to maintain continuous integration and deployment of pipeline suites (see
`<https://github.com/australian-imaging-service/pipelines-core>`).

Arcana was initially developed for neuroimaging analysis, and therefore is
designed to efficiently handle the requirements typical of neuroimaging
workflows (i.e. manipulation of file-based images by various third-party
tools). However, at its core, Arcana is a general framework, which could be
applied to datasets from any field.


.. toctree::
   :maxdepth: 2
   :hidden:

   getting_started
   data_model
   processing
   analysis_classes
   deployment

.. .. toctree::
..    :maxdepth: 2
..    :caption: Development
..    :hidden:
   
..    dev_contributing
..    dev_formats
..    dev_analyses
..    dev_stores
  
.. toctree::
   :maxdepth: 2
   :caption: Reference
   :hidden:

   api
   cli

|
.. note::
   For the legacy version of Arcana as described in
   *Close TG, et. al. Neuroinformatics. 2020 18(1):109-129. doi:* `10.1007/s12021-019-09430-1 <https://doi.org/10.1007/s12021-019-09430-1>`_
   please see `<https://github.com/MonashBI/arcana-legacy>`_.
   Conceptually, the legacy version and the versions in this repository (version >= 2.0) are similar.
   However, instead of Nipype, versions >= 2 use the Pydra_ workflow engine (Nipype's successor)
   and the syntax has been rewritten from scratch to make it more streamlined and intuitive.


.. _Pydra: http://pydra.readthedocs.io
.. _XNAT: http://xnat.org
.. _BIDS: http://bids.neuroimaging.io/
.. _`Environment Modules`: http://modules.sourceforge.net
