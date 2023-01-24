Contributing
============

Contributions to the project are more welcome in various forms. Please see the
`contribution guide  <https://github.com/ArcanaFramework/arcana/blob/main/CONTRIBUTING.md>`_
for details.


Code structure
--------------

The core Arcana code base is implemented in the :mod:`arcana.core` module. Extensions
which implement data store connectors and analyses are installed in separate namesapces
(e.g. ``arcana-xnat``, ``arcana-bids``).

All ``Analysis``, ``DataStore``, ``DataSpace`` and ``App`` classes, should be
imported into the extension package root (e.g. ``arcana.xnat.__init__.py``) so they can
be found by references ``xnat/App``. CLI commands should be implemented as ``click``
commands under the ``arcana.core.cli.ext.ext`` group and imported into the subpackage
root.


Authorship
----------

If you contribute code, documentation or bug reports to the repository please
add your name and affiliation to the `Zenodo file <https://github.com/ArcanaFramework/arcana/blob/main/.zenodo.json>`_
