Contributing
============

Contributions to the project are more welcome in various forms. Please see the
`contribution guide  <https://github.com/Australian-Imaging-Service/arcana/blob/main/CONTRIBUTING.md>`_
for details.


Code structure
--------------

The core Arcana code base is implemented in the "arcana-core" package, which installs
in the :mod:`arcana.core` module. Extensions which implement data store connectors
and analyses are installed in separate namesapces (e.g. ``arcana-xnat``, ``arcana-bids``).

In each extension package, there are four special sub-packages, which will be searched
by the CLI and enable definitions within them to be specified by the extension name
alone, e.g. xnat:XnatApp, bids:BidsApp

* :mod:`arcana.*.analysis` - Arcana analysis class definitions
* :mod:`arcana.*.cli` - data space definitions (see :ref:`Spaces`)
* :mod:`arcana.*.data` - datatype definitions (see :ref:`data_formats`)
* :mod:`arcana.*.deploy` - command-line tools


Authorship
----------

If you contribute code, documentation or bug reports to the repository please
add your name and affiliation to the `Zenodo file <https://github.com/Australian-Imaging-Service/arcana/blob/main/.zenodo.json>`_
