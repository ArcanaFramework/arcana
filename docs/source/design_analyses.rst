.. _design_analyses:

Designing Analyses
==================

An important way to contribute to the development of Arcana is to help to develop new
:class:`.Analysis` classes or extend an existing classes. :class:`.Analysis`
classes are also intended to be tailored meet the specific requirements of
particular use cases. However, one of the key strengths of Arcana's design is
that :class:`.Analysis` classes are ready to be distributed as
part of the Arcana ecosystem as soon as they are implemented.

This page builds upon the description of :class:`.Analysis` class design
introduced in :ref:`analysis_classes`. The basic building blocks of the design
are described in detail in the :ref:`column_param_specs`, :ref:`pipeline_constructors`
and :ref:`analysis_outputs` sections, while more advanced concepts involved in
extending existing classes and merging multiple classes into large analsyes are
covered in the :ref:`inheritance` and :ref:`subanalyses` sections respectively.
Finally, a comprehensive example displaying all potential features is given in
:ref:`comprehensive_example`.


.. _column_param_specs:

Column and parameter specification
----------------------------------

While columns in an :class:`.Analysis` class can be specified using the
dataclass-like syntax of ``column_name: Format``, in most cases you will want to
explicitly use the ``arcana.core.mark.column`` function to include some basic
metadata for the column, such as a description of what the column represents
in the ``desc`` keyword arg.

.. code-block:: python

    @analysis(ExampleDataSpace)
    class ExampleAnalysis():

        recorded_datafile: ZippedDir  = column(
            desc=("Datafile acquired from an example scanner. Contains key "
                  "data to analyse"))
        recorded_metadata: Json = column(
            desc="Metadata accompanying the recorded data")
        preprocessed: ZippedDir = column(
            desc="Preprocessed data file, corrected for distortions")
        derived_image: Png = column(
            desc="Map of the processed data")
        summary_metric: float = column(
            desc="A summary metric extracted from the derived image")

The column spec descriptions will be shown to the user when they use the :meth:`.Dataset.menu()`
or ``arcana menu`` CLI command.

* **primary** - Primary input data, e.g. raw data or data reconstructed on the scanner 
* **output** - Results that would typically be used as main outputs in publications 
* **supplementary** - Derivatives that would typically only be provided in supplementary material 
* **qa** - Derivatives that would typically be only kept for quality assurance of analysis workflows 
* **debug** - Derivatives that would typically only need to be checked when debugging analysis workflows 
* **temp** - Data only temporarily stored to pass between pipelines 

The row frequency of the column (e.g. per-session, per-subject, per-group, etc...
see :ref:`data_spaces` and :ref:`data_columns`) is specified by the ``frequency``
keyword argument, and should match the data space (see :ref:`data_spaces`)
provided to the :func:`arcana.core.mark.analysis` class decorator.

Descriptions and saliences can also be set for parameter attributes, where the
saliences are drawn from :class:`.ParamSalience` enum.

* **debug** - typically only needs to be altered for debugging  
* **recommended** - recommended to keep default value
* **dependent** - can be dependent on the context of the analysis but default should work for most cases  
* **check** - the default should be at checked for validity for particular use case
* **arbitrary** - a default is provided, but it is not clear which value is best
* **required** - no sensible default value, the parameter should be set manually

With the exception of required parameters, default values should be provided
to the parameter specificiation via the ``default`` keyword. The default
value should match the type of the parameter specification. Parameters can
be any of the following types:

* ``float``
* ``int``
* ``bool``
* ``str``
* ``list[float]``
* ``list[int]``
* ``list[bool]``
* ``list[str]``


See :ref:`comprehensive_example` L4-29 for examples of these attributes of
column and parameter specifications.


.. _pipeline_constructors:

Pipeline constructors
---------------------

Pipeline constructor methods are special methods that Arcana calls to construct
workflows that generate requested derivatives. The :func:`arcana.core.mark.pipeline`
decorator is used to specify a pipeline constructor and list the sink columns
that the pipeline can generate outputs for.

The first argument to a constructor method is the :class:`.Pipeline` object
to construct. The pipeline initialisation is handled by Arcana, the constructor
method just needs to add the pipeline nodes to actually perform the analysis.
Pipeline nodes are added in the same following `Pydra's workflow syntax <https://pydra.readthedocs.io/en/latest/components.html#workflows>`_).
The remaining arguments to the constructor correspond to any columns
and parameters that are required for inputs of any nodes to be added. The
names of the arguments should match column/parameter names exactly as they
will be "automagically" provided to the method by Arcana (a bit like PyTest
fixtures).


* frequency
* conditions, overloading
* accessing side-cars


.. _analysis_outputs:

Outputs
-------

.. warning::
    Under construction

* Outputs are for publication 


.. _inheritance:

Inheritance
-----------


.. warning::
    Under construction

* overriding methods
* accessing columns from base classes
* mixins

.. _subanalyses:

Sub-analyses
------------


.. warning::
    Under construction

* How to define sub-analyses
* sub-analysis arrays (e.g. for fMRI tasks)


.. _comprehensive_example:

Comprehensive example
---------------------


.. warning::
    Under construction



.. code-block:: python
    :linenos:

    @analysis(ExampleDataSpace)
    class ExampleAnalysis():

        recorded_datafile: ZippedDir  = column(
            desc=("Datafile acquired from an example scanner. Contains key "
                  "data to analyse"),
            salience='primary')
        recorded_metadata: Json = column(
            desc="Metadata accompanying the recorded data",
            salience='primary')
        preprocessed: ZippedDir = column(
            desc="Preprocessed data file, corrected for distortions",
            salience='qa')
        derived_image: Png = column(
            desc="Map of the processed data",
            salience='supplementary')
        summary_metric: float = column(
            desc="A summary metric extracted from the derived image",
            salience='output')

        contrast: float = parameter(
            default=0.5,
            desc="Contrast of derived image",
            salience='arbitrary')
        kernel_fwhms: list[float] = parameter(
            default=[0.5, 0.3, 0.1],
            desc=("Kernel full-width-at-half-maxium values for iterative "
                  "smoothing in preprocessing"),
            salience='dependent')    

        