Public API
==========

The core of Arcana's framework is located under the ``arcana.core`` sub-package,
which contains all the domain-independent logic. Domain-specific extensions
for alternative data stores, dimensions and formats should be placed in
``arcana.data.stores``, ``arcana.data.dimensions`` and ``arcana.data.formats``
respectively.


.. warning::
    Under construction



Data Model
----------

Core
~~~~

.. autoclass:: arcana.core.data.store.DataStore

.. autoclass:: arcana.core.data.set.Dataset

.. autoclass:: arcana.core.data.dimensions.DataDimensions

.. autoclass:: arcana.core.data.node.DataNode

.. autoclass:: arcana.core.data.spec.DataSource

.. autoclass:: arcana.core.data.spec.DataSink

.. autoclass:: arcana.core.data.item.FileGroup

.. autoclass:: arcana.core.data.item.Field


Stores
~~~~~~

.. autoclass:: arcana.data.stores.file_system.FileSystem

.. autoclass:: arcana.data.stores.bids.BidsFormat

.. autoclass:: arcana.data.stores.xnat.Xnat

.. autoclass:: arcana.data.stores.xnat.XnatViaCS    



Processing
----------

.. autoclass:: arcana.core.pipeline.Pipeline
