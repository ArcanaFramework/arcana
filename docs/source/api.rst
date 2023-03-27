Application Programming Interface
=================================

The core of Arcana's framework is located under the ``arcana.core`` sub-package,
which contains all the domain-independent logic. Domain-specific extensions
for alternative data stores, dimensions and formats should be placed in
``arcana.data.stores``, ``arcana.data.spaces`` and ``arcana.data.types``
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

.. autoclass:: arcana.core.data.row.DataRow

.. autoclass:: arcana.core.data.column.DataSource

.. autoclass:: arcana.core.data.column.DataSink

.. autoclass:: arcana.core.data.datatype.DataType
    :members: get, put

.. autoclass:: arcana.core.data.datatype.FileSet

.. autoclass:: arcana.core.data.datatype.Field

.. autoclass:: arcana.core.data.datatype.BaseFile

.. autoclass:: arcana.core.data.datatype.Directory

.. autoclass:: arcana.core.data.datatype.WithSideCars


Stores
~~~~~~

.. autoclass:: arcana.dirtree.data.SimpleStore

.. autoclass:: arcana.bids.data.Bids

.. autoclass:: arcana.medimage.data.Xnat

.. autoclass:: arcana.medimage.data.XnatViaCS
    :members: generate_xnat_command, generate_dockerfile, create_wrapper_image


Processing
----------

.. autoclass:: arcana.core.analysis.pipeline.Pipeline


Enums
~~~~~

.. autoclass:: arcana.core.enum.ColumnSalience
    :members:
    :undoc-members:
    :member-order: bysource

.. autoclass:: arcana.core.enum.ParameterSalience
    :members:
    :undoc-members:
    :member-order: bysource

.. autoclass:: arcana.core.enum.DataQuality
    :members:
    :undoc-members:
    :member-order: bysource
