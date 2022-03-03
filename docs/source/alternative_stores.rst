.. _alternative_stores:

Alternative storage systems
===========================

Alternative storage systems can be implemented by writing a new subclass of
:class:`.DataStore`. The developers are interested in adding support for new systems,
so if you would help to use Arcana with a different storage system please
create an issue for it in the `GitHub Issue Tracker <https://github.com/Australian-Imaging-Service/arcana/issues>`__.

Required methods
----------------

When subclassing :class:`.DataStore`, the following abstract methods must be
overridden to implement the appropriate functionality of the data store.

.. autoclass:: arcana.core.data.store.DataStore
    :noindex:
    :members: find_nodes, find_items, get_file_group, get_field, put_file_group, put_field, save_dataset_metadata, load_dataset_metadata

Optional methods
----------------

The following methods are not strictly necessary to override, but can offer
significant performance boosts by avoiding unnecessary downloads in the
case of :meth:`.DataStore.get_checksums` and unnecessary remote connections
in the case of :meth:`.DataStore.connect` and :meth:`.DataStore.disconnect`
(by caching the connection between multiple calls).

.. autoclass:: arcana.core.data.store.DataStore
    :noindex:
    :members: get_checksums, connect, disconnect    
