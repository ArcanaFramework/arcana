Application Programming Interface
=================================

The core of Arcana's framework is located under the ``arcana.core`` sub-package,
which contains all the domain-independent logic. Domain-specific extensions
for alternative data stores, dimensions and formats should be placed in
``arcana.data.stores``, ``arcana.data.spaces`` and ``arcana.data.formats``
respectively.


.. warning::
    Under construction



Data Model
----------

Core
~~~~

.. autoclass:: arcana.core.data.store.DataStore

.. autoclass:: arcana.core.data.set.Dataset
    :members: add_source, add_sink

.. autoclass:: arcana.core.data.space.DataSpace

.. autoclass:: arcana.core.data.node.DataNode

.. autoclass:: arcana.core.data.spec.DataSource

.. autoclass:: arcana.core.data.spec.DataSink

.. autoclass:: arcana.core.data.item.DataItem
    :members: get, put

.. autoclass:: arcana.core.data.item.FileGroup

.. autoclass:: arcana.core.data.item.Field


Stores
~~~~~~

.. autoclass:: arcana.data.stores.file_system.FileSystem

.. autoclass:: arcana.data.stores.bids.BidsFormat

.. autoclass:: arcana.data.stores.xnat.Xnat

.. autoclass:: arcana.data.stores.xnat.XnatViaCS
    :members: generate_xnat_command, generate_dockerfile, create_wrapper_image





Processing
----------

.. autoclass:: arcana.core.pipeline.Pipeline


Enums
~~~~~

.. autoclass:: arcana.core.enum.DataSalience
    :members:
    :undoc-members:                                  
    :member-order: bysource  

.. autoclass:: arcana.core.enum.ParamSalience
    :members:
    :undoc-members:                                  
    :member-order: bysource  

.. autoclass:: arcana.core.enum.DataQuality
    :members:
    :undoc-members:                                  
    :member-order: bysource  