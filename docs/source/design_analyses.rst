.. _design_analyses:

Designing Analyses
==================

An great way to contribute to the development of Arcana is to implement new
:class:`.Analysis` classes or extend existing ones. Analysis
classes are designed to be able generic enough to be used widely, but able to
be tailored to meet specific requirements of particular use cases/research studies
via class inheritance where required (see :ref:`inheritance`).

.. This page builds upon the description of analysis-class design
.. introduced in :ref:`analysis_classes`. The basic building blocks of the design
.. are described in detail in the :ref:`Basics` section, while more advanced
.. concepts involved in extending existing classes and merging multiple classes
.. into large analsyes are covered in the :ref:`Advanced` section.
.. Finally, examples showing all features in action are given in
.. :ref:`analysis_examples`.


Basics
------

There are two main components of analysis classes, column specifications
(:ref:`column_param_specs`), which define the data to be provided to and
derived by the class, and pipeline builder methods (:ref:`pipeline_builders`),
which construct the `Pydra workflows <https://pydra.readthedocs.io/en/latest/components.html#workflows>`_
used to generate the derivatives. Parameter attributes (:ref:`column_param_specs`)
expose key parameters used by the workflow construction and output methods
(:ref:`analysis_outputs`) provide a convenient way to include the final steps
analyses (e.g. plotting figures) all in the one place.


.. _column_param_specs:

DataColumn and parameter specification
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

While columns in an :class:`.Analysis` class can be specified using the
dataclass-like syntax of ``column_name: Format``, in most cases you will want to
explicitly use the ``arcana.core.mark.column`` function to include some basic
metadata for the column, such as a description of what the column represents
in the ``desc`` keyword arg.

.. code-block:: python

    @analysis(ExampleDataSpace)
    class ExampleAnalysis():

        recorded_datafile: Zip  = column(
            desc=("Datafile acquired from an example scanner. Contains key "
                  "data to analyse"))
        recorded_metadata: Json = column(
            desc="Metadata accompanying the recorded data")
        preprocessed: Zip = column(
            desc="Preprocessed data file, corrected for distortions")
        derived_image: Png = column(
            desc="Map of the processed data")
        summary_metric: float = column(
            desc="A summary metric extracted from the derived image",
            row_frequency='dataset')

The column spec descriptions will be shown to the user when they use the :meth:`.Dataset.menu()`
or ``arcana menu`` CLI command. The row row_frequency of the column (e.g. per-session,
per-subject, per-group, once per-dataset etc..., see :ref:`data_spaces` and
:ref:`data_columns`) is specified by the ``row_frequency``
keyword argument. The row_frequency should be a member of the data space(see :ref:`data_spaces`)
provided to the :func:`arcana.core.mark.analysis` class decorator.

Not all columns specifications are created equal. Some refer to key inputs
(e.g. the primary MRI image) or outputs (e.g. lesion load) and others just need
to be sanity checked or useful in debugging. Therefore, to avoid the menu being
cluttered up with non-salient specifications, the "salience" of the columns can
be specified in addition to a description via the ``salience`` keyword arg.
Values for ``salience`` must be drawn from the :class:`arcana.core.enum.ColumnSalience` enum:

* **primary** - Primary input data, e.g. raw data or data reconstructed on the scanner
* **output** - Results that would typically be used as main outputs in publications
* **supplementary** - Derivatives that would typically only be provided in supplementary material
* **qa** - Derivatives that would typically be only kept for quality assurance of analysis workflows
* **debug** - Derivatives that would typically only need to be checked when debugging analysis workflows
* **temp** - Data only temporarily stored to pass between pipelines

Descriptions and saliences can also be set for parameter attributes, where the
saliences are drawn from :class:`arcana.core.enum.ParameterSalience` enum.

* **debug** - typically only needs to be altered for debugging
* **recommended** - recommended to keep default value
* **dependent** - can be dependent on the context of the analysis but default should work for most cases
* **check** - the default should be at checked for validity for particular use case
* **arbitrary** - a default is provided, but it is not clear which value is best
* **required** - no sensible default value, the parameter should be set manually

With the exception of required parameters, default values should be provided
to the parameter specification via the ``default`` keyword. The default
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
that is being constructed. The initialisation of the pipeline and rows to iteract
with the data store are handled by Arcana, the builder method just needs to add
the rows that actually perform the analysis. Pipeline rows are added using
`Pydra's workflow syntax <https://pydra.readthedocs.io/en/latest/components.html#workflows>`_.
(the only exception being that the newly added row is returned from
:meth:`.Pipeline.add` for convenience).

The remaining arguments to the builder should be named after any columns
and parameters that are required for the pipeline rows to be added. Arcana will
automagically provide ``LazyField`` pointers to the arguments named after
column specs, and values to the arguments named after parameter specs.
For file formats with side cars, lazy-field pointers to side car
files can be accessed as attributes of the primary ``LazyField``, e.g.

.. code-block:: python

    from arcana.data.spaces.medimage import Clinical
    from arcana.tasks.misc import ExtractFromJson
    from arcana.data.salience import ColumnSalience as ds


    @analysis(Clinical)
    class AnotherExampleAnalysis():

        primary_image: Dicom = column(
            desc="The primary image to be analysed",
            salience=ds.primary)
        repetition_time: float = column(
            "The repetition time of the MR sequence used",
            salience=ds.debug)
        slice_timing_interval: float = column(
            "The time interval between slices",
            salience=ds.debug)

        @pipeline(repetition_time, slice_timing_interval)
        def preprocess_pipeline(self, wf, primary_image: NiftiGzX):

            wf.add(
                ExtractFromJson(
                    name='extract_tr',
                    # JSON side car is accessed by an attribute of the primary image
                    in_file=primary_image.json,
                    field='tr'))

            wf.add(
                ExtractFromJson(
                    name='extract_st',
                    # JSON side car is accessed by an attribute of the primary image
                    in_file=primary_image.json,
                    x=wf.extract_tr.lzout.out,
                    field='SliceTiming'))

            return wf.extract_tr.lzout.out, wf.extract_st.lzout.out

The "row_frequency" (see :ref:`data_spaces` and :ref:`data_columns`) of a pipeline,
(whether it is run per-session, per-subject, per-timepoint, etc... for example)
is determined by the row_frequency of its output columns. Therefore, all columns
derived from a single pipeline need to have the same row row_frequency. If the
row_frequency of an input column provided to the builder method is higher than that
of the pipeline then the lazy field provided will point to a list (sorted by the
axis IDs they are combined over) rather than a single value. If the row_frequency
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
            row_frequency='recording')
        rain: float = column(
            desc="Daily rain measurements at different locations",
            row_frequency='recording')
        avg_rainfall: float  = column(
            desc="Average rainfall for a given location",
            row_frequency='station')
        delta_rain: float = column(
            desc="Deviation from average rainfall for a given month"
            row_frequency='recording')

        # Pipeline is of 'per-station' row_frequency due to row_frequency of output column
        # 'avg_rainfall'
        @pipeline(avg_rainfall)
        # 'rain' arg is a lazy-field to a list[float] over all dates since the
        # row_frequency of the 'rain' column ('recording') is higher than
        # the pipeline's row_frequency ('station')
        def average_rainfall_pipeline(self, wf: pydra.Workflow, rain: list[float]):

            wf.add(
                average(
                    name='average_rain',
                    measurements=rainfall))

            return wf.average_rain.lzout.out

        # Pipeline is of 'per-recording' row_frequency due to delta_rainfall
        # output column
        @pipeline(delta_rain)
        def delta_pipeline(self, wf: pydra.Workflow, rain: float,  avg_rainfall: float):

            pipeline.add(
                delta(
                    name="delta_rain",
                    measurements=rain,
                    average=avg_rainfall))

            return wf.delta_rain.lzout.out


.. _analysis_outputs:

Output methods
~~~~~~~~~~~~~~

"Output methods" take derivatives and produce the visualisations or tables to be
included in publications or reports. Since these methods typically rely on
graphical libraries, they are executed on the local workstation/row and
therefore should not contain any heavy computations. The feature that
differentiates them from a regular method is that they are accessible from the
CLI

.. code-block:: console

    $ arcana derive output 'file///data/my-dataset' connectivity_matrix_plot \
      --save '~/Documents/papers/my-connectivity-paper/' \
      --option figsize 10,10

The ``arcana.core.mark.output`` decorator is used to specify an output method
and the outputs that are generated by it. Output methods should take the
directory to save the outputs in as its first argument and use keyword
arguments for "options" of the method following that. The save directory
should have a default of ``None``, and display the results in the case that it
isn't provided.


.. code-block:: python

    import matplotlib.pyplot as plt
    from arcana.data.spaces.medimage import Clinical

    @analysis(Clinical)
    class ExampleAnalysis2():

        ...

        @output
        def connectivity_matrix_plot(self, save_dir: str=None, figsize: tuple[float]=(5, 5)):
            """Plots the connectivity matrix as an image
            """
            plt.figure(figsize=figsize)
            plt.imshow(self['connectivity_matrix'].data)
            if save_dir:
                plt.savefig(save_dir)
            else:
                plt.show()


.. Advanced
.. --------

.. .. warning::
..     Under construction

.. In every software framework, there are always corner cases that are
.. more complicated than the basic logic can handle. In designing
.. informatics frameworks, these challenges often arise when attempting to write
.. portable workflows, due to slight differences in the data and and end goals of
.. the application. This is particularly true in academia, where novelty is a key
.. criteria. To address these requirements, this section introduces some more
.. complex concepts, which can be used to customise and combine analysis methods
.. into powerful new classes: conditional pipelines (:ref:`conditional_pipelines`),
.. class inheritance (:ref:`inheritance`) and sub-analyses (:ref:`subanalyses`).


.. .. _conditional_pipelines:

.. Conditionals
.. ~~~~~~~~~~~~


.. * conditions + symbolic logic
.. * resolution order

.. .. _inheritance:

.. Inheritance
.. ~~~~~~~~~~~


.. * overriding methods
.. * accessing columns from base classes
.. * mixins

.. .. _subanalyses:

.. Sub-analyses
.. ~~~~~~~~~~~~


.. * How to define sub-analyses
.. * sub-analysis arrays (e.g. for fMRI tasks)


.. _analysis_examples:

Examples
--------

Toy example
~~~~~~~~~~~

A toy example analysis class, that has two text-file source columns, ``file1`` and ``file2``,
and one text-file sink column ``concatenated``. The ``concatenated`` column is considered a
sink because the ``concat_pipeline`` method is marked with a ``pipeline`` decorator specifying
it as an output.

Pydra LazyFields linked to the source columns of ``file1`` and ``file2``, and the
value provided to the ``duplicates`` parameter, will be automagically
provided to the ``concat_pipeline`` method during the construction of the workflow that
will be used generate ``concatenated``.

.. code-block:: python

    @analysis(Samples)
    class Concat:

        # Source columns
        file1: Text = column("an arbitrary text file")
        file2: Text = column("another arbitrary text file")

        # Sink columns
        concatenated: Text = column("the output of concatenating file1 and file2")

        # Parameters
        duplicates: int = parameter(
            "the number of times to duplicate the concatenation", default=1
        )

        @pipeline(concatenated)
        def concat_pipeline(self, wf, file1: Text, file2: Text, duplicates: int):
            """Concatenates the contents of `file1` with the contents of `file2` to produce
            a new text file. The concatenation can be repeated multiple times within
            the produced text file by specifying the number of repeats to the `duplicates`
            parameter
            """

            wf.add(
                concatenate(
                    name="concat", in_file1=file1, in_file2=file2, duplicates=duplicates
                )
            )

            return wf.concat.lzout.out  # Output Pydra LazyField for concatenated file


Extending via subclasses
~~~~~~~~~~~~~~~~~~~~~~~~

The ``Concat`` class extended to add two additional columns: another source column ``file3``
and another sink column ``doubly_concatenated``.

.. code-block:: python

    @analysis(Samples)
    class ExtendedConcat(Concat):

        # Source columns
        file3: Text = column("Another file to concatenate")

        # Sink columns
        concatenated = inherited_from(Concat)
        doubly_concatenated: Text = column("The doubly concatenated file")

        # Parameters
        duplicates = inherited_from(Concat)

        @pipeline(doubly_concatenated)
        def doubly_concat_pipeline(
            self, wf, concatenated: Text, file3: Text, duplicates: int
        ):

            wf.add(
                concatenate(
                    name="concat",
                    in_file1=concatenated,
                    in_file2=file3,
                    duplicates=duplicates,
                )
            )

            return wf.concat.lzout.out


Adding quality-control checks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This extended class adds in a QC check to make sure the number of lines produced by the
concatenation step matches what is expected.

.. code-block:: python

    @analysis(Samples)
    class ConcatWithCheck(Concat):

        # Sink columns
        concatenated = inherited_from(Concat)

        # Parameters
        duplicates = inherited_from(Concat)

        @check(concatenated, salience=CheckSalience.recommended)
        def check_file3(self, wf, concatenated: Text, duplicates: int):
            """Checks the number of lines in the concatenated file to see whether they
            match what is expected for the number of duplicates specified"""
            @pydra.mark.task
            def num_lines_equals(in_file, num_lines):
                with open(in_file) as f:
                    contents = f.read()
                if len(contents.splitlines()) == num_lines:
                    status = CheckStatus.probable_pass
                else:
                    status = CheckStatus.failed
                return status

            wf.add(
                num_lines_equals(
                    in_file=concatenated, num_lines=2 * duplicates, name="num_lines_check"
                )
            )

            return wf.num_lines_check.out


Optionally overriding pipelines
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Another subclass of ``Concat``, but this time the concatenation is reversed
if "reversed" is provided to the ``order`` parameter by optionally overriding the
pipeline that produces ``concatenated``


.. code-block:: python

    @analysis(Samples)
    class OverridenConcat(Concat):

        # Source columns
        file1: Zip = inherited_from(Concat)
        file2: Text = inherited_from(Concat)

        # Sinks columns
        concatenated: Text = inherited_from(Concat)

        # Parameters
        duplicates = inherited_from(Concat, default=2)  # default value changed because we can
        order: str = parameter(
            "perform the concatenation in reverse order, i.e. file2 and then file1",
            choices=["forward", "reversed"],
            default="forward",
        )

        @pipeline(
            concatenated,
            condition=value_of(order) == "reversed",
        )
        def reverse_concat_pipeline(
            self, wf, file1: Text, file2: Text, duplicates: int
        ):

            wf.add(
                concatenate_reverse(
                    name="concat", in_file1=file1, in_file2=file2, duplicates=duplicates
                )
            )

            return wf.concat.lzout.out


Using switches for dependent steps
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The contents of the files in the ``concatenated`` column are multiplied the value
passed to the arbitrary ``multiplier`` parameter if the contents of the input
files ``file1`` and ``file2`` are numeric for the corresponding row as determined by the
``inputs_are_numeric`` switch.

.. code-block:: python

    @analysis(Samples)
    class ConcatWithSwitch(Concat):

        # Source columns
        file1: Zip = inherited_from(Concat)
        file2: Text = inherited_from(Concat)

        # Sink columns
        concatenated: Text = inherited_from(Concat)
        multiplied: Text = column("contents of the concatenated files are multiplied")

        # Parameters
        multiplier: int = parameter(
            "the multiplier used to apply", salience=ps.arbitrary
        )

        @switch
        def inputs_are_numeric(self, wf, file1: Text, file2: Text):

            wf.add(contents_are_numeric(in_file=file1, name="check_file1"))

            wf.add(contents_are_numeric(in_file=file2, name="check_file2"))

            @pydra.mark.task
            def boolean_and(val1, val2) -> bool:
                return val1 and val2

            wf.add(
                boolean_and(
                    val1=wf.check_file1.out, val2=wf.check_file2.out, name="bool_and"
                )
            )

            return wf.bool_and.out

        @pipeline(multiplied, condition=inputs_are_numeric)
        def multiply_pipeline(self, wf, concatenated, multiplier):

            wf.add(
                multiply_contents(
                    name="concat", in_file=concatenated, multiplier=multiplier
                )
            )

            return wf.concat.lzout.out

.. .. code-block:: python
..     :linenos:

..     @analysis(ExampleDataSpace)
..     class ExampleAnalysis():

..         recorded_datafile: ZippedDir  = column(
..             desc=("Datafile acquired from an example scanner. Contains key "
..                   "data to analyse"),
..             salience='primary')
..         recorded_metadata: Json = column(
..             desc="Metadata accompanying the recorded data",
..             salience='primary')
..         preprocessed: ZippedDir = column(
..             desc="Preprocessed data file, corrected for distortions",
..             salience='qa')
..         derived_image: Png = column(
..             desc="Map of the processed data",
..             salience='supplementary')
..         summary_metric: float = column(
..             desc="A summary metric extracted from the derived image",
..             salience='output')
..         contrast: float = parameter(
..             default=0.5,
..             desc="Contrast of derived image",
..             salience='arbitrary')
..         kernel_fwhms: list[float] = parameter(
..             default=[0.5, 0.3, 0.1],
..             desc=("Kernel full-width-at-half-maxium values for iterative "
..                   "smoothing in preprocessing"),
..             salience='dependent')
