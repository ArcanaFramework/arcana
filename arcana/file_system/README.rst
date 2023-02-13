Arcana Extension - DirTree
==========================
.. image:: https://github.com/ArcanaFramework/arcana-dirtree/actions/workflows/tests.yml/badge.svg
   :target: https://github.com/ArcanaFramework/arcana-dirtree/actions/workflows/tests.yml
.. image:: https://codecov.io/gh/ArcanaFramework/arcana-dirtree/branch/main/graph/badge.svg?token=UIS0OGPST7
   :target: https://codecov.io/gh/ArcanaFramework/arcana-dirtree
.. image:: https://readthedocs.org/projects/arcana/badge/?version=latest
  :target: http://arcana.readthedocs.io/en/latest/?badge=latest
  :alt: Documentation Status


This Arcana extension provides a data store for operating on loosely structured data stored within sub-directories
of a file-system directory. The only constraints on the structure of the sub-directories is that the leaves of the
tree, the collected data points, are of equal depth, and each directory layer corresponds to a different classifier.
For example, an imaging dataset with multiple subjects scanned at multiple time-points could be laid out as

.. code-block::

    imaging-dataset
    ├── subject1
    │   ├── timepoint1
    │   │   ├── t1w_mprage
    │   │   ├── t2w_space
    │   │   └── bold_rest
    │   └── timepoint2
    │       ├── t1w_mprage
    │       ├── t2w_space
    │       └── bold_rest
    ├── subject2
    │   ├── timepoint1
    │   │   ├── t1w_mprage
    │   │   ├── t2w_space
    │   │   └── bold_rest
    │   └── timepoint2
    │       ├── t1w_mprage
    │       ├── t2w_space
    │       └── bold_rest
    └── subject3
        ├── timepoint1
        │   ├── t1w_mprage
        │   ├── t2w_space
        │   └── bold_rest
        └── timepoint2
            ├── t1w_mprage
            ├── t2w_space
            └── bold_rest

with the classification hierarchy of ``subject > timepoint``

However, the same dataset could alternatively be laid out in the reverse
classification hierarchy, ``timepoint > subject``

.. code-block::

    imaging-dataset
    ├── timepoint1
    │   ├── subject1
    │   │   ├── t1w_mprage
    │   │   ├── t2w_space
    │   │   └── bold_rest
    |   ├── subject2
    │   │   ├── t1w_mprage
    │   │   ├── t2w_space
    │   │   └── bold_rest
    │   └── subject3
    │       ├── t1w_mprage
    │       ├── t2w_space
    │       └── bold_rest
    └── timepoint2
        ├── subject1
        │   ├── t1w_mprage
        │   ├── t2w_space
        │   └── bold_rest
        ├── subject2
        │   ├── t1w_mprage
        │   ├── t2w_space
        │   └── bold_rest
        └── subject3
            ├── t1w_mprage
            ├── t2w_space
            └── bold_rest


or in a single level directory structure where the classifiers are combined to form the
sub-directory names

.. code-block::

    imaging-dataset
        ├── sub1-tp1
        │   ├── t1w_mprage
        │   ├── t2w_space
        │   └── bold_rest
        ├── sub2-tp1
        │   ├── t1w_mprage
        │   ├── t2w_space
        │   └── bold_rest
        ├── sub3-tp1
        │   ├── t1w_mprage
        │   ├── t2w_space
        │   └── bold_rest
        ├── sub1-tp2
        │   ├── t1w_mprage
        │   ├── t2w_space
        │   └── bold_rest
        ├── sub2-tp2
        │   ├── t1w_mprage
        │   ├── t2w_space
        │   └── bold_rest
        └── sub3-tp2
            ├── t1w_mprage
            ├── t2w_space
            └── bold_rest


Quick Installation
------------------

This extension can be installed for Python 3 using *pip*::

    $ pip3 install arcana-dirtree

This will also install the core Arcana_ package and any required dependencies.

License
-------

This work is licensed under a
`Creative Commons Attribution 4.0 International License <http://creativecommons.org/licenses/by/4.0/>`_

.. image:: https://i.creativecommons.org/l/by/4.0/88x31.png
  :target: http://creativecommons.org/licenses/by/4.0/
  :alt: Creative Commons Attribution 4.0 International License



.. _Arcana: http://arcana.readthedocs.io
