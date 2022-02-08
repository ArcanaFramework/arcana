Data Access
===========

Stores
------

Arcana_ handles all workflow file/network I/O interactions, making pipeline
implementations transportable between storage systems and freeing
designers to focus on the analysis being implemented.

Support for different storage techniques is provided by modular `DataStore`
objects. There are currently three types stores:

* FileSystem - access data simply organised within file system directories
* BidsFormat - access neuroscience data on file systems organised in the BIDS_ format
* Xnat - access data stored in XNAT_ repositories
* XnatViaCS - access data stored in XNAT_ repositories as exposed to integrated pipelines run in XNAT_'s container service

Additional storage systems can be implemented in Arcana_ by extending the base
`DataStore` class. The developers are keen to add support for new
systems, so if you are interested in support for a different storage system please
create an issue for it `here <https://github.com/Australian-Imaging-Service/arcana/issues>`__.


API
~~~

To access a `DataStore` via the API simply initialise the corresponding class, e.g.

.. code-block:: python

    from arcana.data.stores.xnat import Xnat

    xnat_store = Xnat(
        
    )



.. _Arcana: http://arcana.readthedocs.io
.. _BIDS: https://bids.neuroimaging.io/
.. _XNAT: http://xnat.org