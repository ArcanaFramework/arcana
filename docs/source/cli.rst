Command-line interface
======================

Arcana's command line interface is grouped into five categories, `store`,
`dataset`, `apply`, `derive`, and `deploy`. Below these categories are the
commands that interact with Arcana's data model, processing and deployment
streams.


Store
-----

Commands used to access remove data stores and save them for further use

.. click:: arcana.cli.store:add
   :prog: arcana store add

.. click:: arcana.cli.store:rename
   :prog: arcana store rename

.. click:: arcana.cli.store:remove
   :prog: arcana store remove

.. click:: arcana.cli.store:refresh
   :prog: arcana store refresh


Dataset
-------

Commands used to define and work with datasets

.. click:: arcana.cli.dataset:define
   :prog: arcana dataset define

.. click:: arcana.cli.dataset:rename
   :prog: arcana dataset rename

.. click:: arcana.cli.dataset:add_source
   :prog: arcana dataset add-source

.. click:: arcana.cli.dataset:add_sink
   :prog: arcana dataset add-sink

.. click:: arcana.cli.dataset:missing_items
   :prog: arcana dataset missing-items


Apply
-----

Commands for applying workflows and analyses to datasets

.. click:: arcana.cli.apply:apply_pipeline
   :prog: arcana apply pipeline


.. click:: arcana.cli.apply:apply_analysis
   :prog: arcana apply analysis


Derive
-------

Commands for calling workflows/analyses to derive derivative data

.. click:: arcana.cli.derive:derive_column
   :prog: arcana derive column

.. click:: arcana.cli.derive:derive_output
   :prog: arcana derive output

.. click:: arcana.cli.derive:menu
   :prog: arcana derive menu

.. click:: arcana.cli.derive:ignore_diff
   :prog: arcana derive ignore-diff


Deploy
------

Commands for deploying arcana pipelines


.. click:: arcana.cli.deploy:build
   :prog: arcana deploy build

.. click:: arcana.cli.deploy:test
   :prog: arcana deploy test

.. click:: arcana.cli.deploy:build_docs
   :prog: arcana deploy docs

.. click:: arcana.cli.deploy:inspect_docker
   :prog: arcana deploy inspect-docker
