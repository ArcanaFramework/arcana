Contributing
============

Contributions to the project are more welcome in various forms. Please see the
`contribution guide  <https://github.com/Australian-Imaging-Service/arcana/blob/main/CONTRIBUTING.md>`_
for details.


Code structure
--------------

The Arcana code base is separated into :mod:`arcana.core`, where all the core
elements of the framework reside, and sub-packages to hold framework implementations
added by sub-packages (e.g. ``arcana-common``, ``arcana-medimage``,
``arcana-bids``)

* :mod:`arcana.data.spaces` - data space definitions (see :ref:`Spaces`)
* :mod:`arcana.data.types` - datatype definitions (see :ref:`data_formats`)
* :mod:`arcana.cli` - command-line tools
* :mod:`arcana.analyses` - Arcana analysis class definitions
* :mod:`arcana.analysis.tasks` - Pydra tasks required by analysis classes, generic Pydra tasks should be implemented in Pydra sub-packages (see `Pydra tasks template <https://github.com/nipype/pydra-tasks-template>`_)


Authorship
----------

If you contribute code, documentation or bug reports to the repository please
add your name and affiliation to the `Zenodo file <https://github.com/Australian-Imaging-Service/arcana/blob/main/.zenodo.json>`_
