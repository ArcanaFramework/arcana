.. _design_analyses:

Designing Analyses
==================

An great way to contribute to the development of Arcana is to implement new
:class:`.Analysis` classes or extend existing ones. :class:`.Analysis`
classes are designed to be able to be tailored to meet specific requirements of
particular use cases/research studies.

This page builds upon the description of :class:`.Analysis` class design
introduced in :ref:`analysis_classes`. The basic building blocks of the design
are described in detail in the :ref:`column_param_specs`, :ref:`pipeline_builders`
and :ref:`analysis_outputs` sections, while more advanced concepts involved in
extending existing classes and merging multiple classes into large analsyes are
covered in the :ref:`inheritance` and :ref:`subanalyses` sections respectively.
Finally, a examples showing all potential features is given in
:ref:`comprehensive_example`.


Basics
------

.. _column_param_specs:

Column and parameter specification
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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


.. _pipeline_builders:

Pipeline builders
~~~~~~~~~~~~~~~~~

"Pipeline builders" are called by Arcana to construct the Pydra workflows that
derive data columns. The :func:`arcana.core.mark.pipeline`
decorator is used to mark a method as a pipeline builder and specify the
columns the workflow it builds derives.

The first argument to a builder method is the :class:`.Pipeline` object
that is being constructed. The initialisation of the pipeline and nodes to iteract
with the data store are handled by Arcana, the builder method just needs to add
the nodes that actually perform the analysis. Pipeline nodes are added using
`Pydra's workflow syntax <https://pydra.readthedocs.io/en/latest/components.html#workflows>`_.
(the only exception being that the newly added node is returned from
:meth:`.Pipeline.add` for convenience).

The remaining arguments to the builder should be named after any columns
and parameters that are required for the pipeline nodes to be added. Arcana will
automagically provide ``LazyField`` pointers to the arguments named after
column specs, and values to the arguments named after parameter specs.
For file formats with side cars, lazy-field pointers to side car
files can be accessed as attributes of the primary ``LazyField``, e.g.

.. code-block:: python

    repetition_time: float = column("The repetition time of the MR sequence used")

    @pipeline(repetition_time)
    def preprocess_pipeline(
            self,
            pipeline,
            primary_image: NiftiGzX):

        extract_tr = pipeline.add(
            ExtractFromJson(
                name='extract_tr',
                # JSON side car is accessed by an attribute of the primary image
                in_file=primary_image.json,  
                field='tr'))

        return extract_tr.lzout.out_file

The "frequency" (see :ref:`data_spaces` and :ref:`data_columns`) of a pipeline,
(whether it is run per-session, per-subject, per-timepoint, etc... for example)
is determined by the frequency of its output columns. Therefore, all columns
derived from a single pipeline need to have the same row frequency. If the
frequency of an input column provided to the builder method is higher than that
of the pipeline then the lazy field provided will point to a list (sorted by the
axis IDs they are combined over) rather than a single value. If the frequency
of an input is lower than that of the pipeline then that value is simply
repeated. For example, an analysis of flood levels using datasets in the ``Weather``
data space (see :ref:`weather_example`) to calculate the average rainfall per
station, could look like


.. code-block:: python

    import numpy
    import pydra.mark
    from arcana.data.spaces.weather import Weather  # See example in Data spaces section


    # A basic Pydra function task used in the analysis
    @pydra.mark.task
    def average(measurements: list[float]) -> float:
        "A simple function task to convert daily to yearly figures"
        return numpy.average(measurements)

    # Another basic Pydra function task used in the analysis
    @pydra.mark.task
    def delta(measurements: list[float], average: float) -> list[float]:
        "A simple function task to convert daily to yearly figures"
        return list(numpy.asarray(measurements) - average)


    @analysis(Weather)
    class FloodAnalysis():

        record_time: datetime = column(
            desc="The time/date the recording was taken"
            frequency='recording')
        rain: float = column(
            desc="Daily rain measurements at different locations",
            frequency='recording')
        avg_rainfall: float  = column(
            desc="Average rainfall for a given location",
            frequency='station')
        delta_rain: float = column(
            desc="Deviation from average rainfall for a given month"
            frequency='recording')

        # Pipeline is of 'per-station' frequency due to frequency of output column
        # 'avg_rainfall'
        @pipeline(avg_rainfall)  
        def average_rainfall_pipeline(
                self,
                pipeline,
                # 'rain' arg is a lazy-field to a list[float] over all dates since the
                # frequency of the 'rain' column ('recording') is higher than
                # the pipeline's frequency ('station')
                rain: list[float]):  

            average_rain = pipeline.add(
                average(
                    name='average_rain',
                    measurements=rainfall))
            
            return average_rain.lzout.out

        # Pipeline is of 'per-recording' frequency due to delta_rainfall
        # output column
        @pipeline(delta_rain)
        def delta_pipeline(
                self,
                pipeline,
                rain: float,  # 
                avg_rainfall: float):

            delta_rain = pipeline.add(
                delta(
                    name="delta_rain",
                    measurements=rain,
                    average=avg_rainfall))

            return delta_rain.lzout.out


.. _analysis_outputs:

Output methods
~~~~~~~~~~~~~~

"Output methods" take derivatives and produce the visualisations or tables to be
included in publications or reports. Since these methods typically rely on
graphical libraries, they are executed on the local workstation/node and
therefore should not contain any heavy computations. The feature that
differentiates them from a regular method is that they are accessible from the
CLI

.. code-block:: console

    $ arcana derive output 'file///data/my-dataset' connectivity-matrix \
      --save '~/Documents/papers/my-connectivity-paper/' \
      --option figsize 10,10

The ``arcana.core.mark.output`` decorator is used to specify an output method
and the outputs that are generated by it. Output methods should take the
directory to save the outputs in as its first argument and optional keyword
arguments for "options" of the method following that. The save directory
should have a default of ``None``, and display the results in the case that it
isn't provided.


.. code-block:: python

    import matplotlib.pyplot as plt

    @analysis(ClinicalTrial)
    class ExampleAnalysis2():

        ...

        @output('connectivity-matrix')
        def plot_connectivity_matrix(self, save_dir: str=None, figsize: tuple[float]=(5, 5)):
            plt.figure(figsize=figsize)
            plt.imshow(self['connectivity_matrix'].data)
            if save_dir:
                plt.savefig(save_dir)
            else:
                plt.show()


Advanced
--------

.. warning::
    Under construction

.. _conditional_pipelines:

Conditional pipelines
~~~~~~~~~~~~~~~~~~~~~


* conditions + symbolic logic
* resolution order

.. _inheritance:

Inheritance
~~~~~~~~~~~


* overriding methods
* accessing columns from base classes
* mixins

.. _subanalyses:

Sub-analyses
~~~~~~~~~~~~


* How to define sub-analyses
* sub-analysis arrays (e.g. for fMRI tasks)


.. _analysis_examples:

Examples
--------

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

        