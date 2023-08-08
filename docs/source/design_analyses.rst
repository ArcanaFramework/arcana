.. _design_analyses:

Designing Analyses
==================

An great way to contribute to the development of Arcana is to implement new
analysis classes or extend existing ones. The architecture of analysis
classes is intended to facilitate the implementation of generic analysis suites
for wide-spread use, which can then be tailored to meet the specific requirements
of particular research studies via class inheritance (see :ref:`inheritance`).

This page builds upon the description of analysis-class design
introduced in :ref:`analysis_classes`. The basic building blocks of the design
are described in detail in the :ref:`Basics` section, while more advanced
concepts involved in extending existing classes are covered in the :ref:`Advanced`
section.


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

        recorded_datafile: DatFile  = column(
            desc=("Datafile acquired from an example scanner. Contains key "
                  "data to analyse"))
        recorded_metadata: Json = column(
            desc="Metadata accompanying the recorded data")
        preprocessed: Zip[Text] = column(
            desc="Preprocessed data file, corrected for distortions")
        derived_image: Png = column(
            desc="Map of the processed data")
        summary_metric: field.Decimal = column(
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

    from fileformats.field import Decimal
    from fileformats.medimage import DicomCollection
    from arcana.common import Clinical
    from arcana.core import mark
    from arcana.core.tasks.misc import ExtractFromJson
    from arcana.core.data.salience import ColumnSalience as ds


    @mark.analysis(Clinical)
    class AnotherExampleAnalysis():

        primary_image: DicomCollection = mark.column(
            desc="The primary image set to be analysed",
            salience=ds.primary)
        repetition_time: Decimal = mark.column(
            "The repetition time of the MR sequence used",
            salience=ds.debug)
        slice_timing_interval: Decimal = mark.column(
            "The time interval between slices",
            salience=ds.debug)

        @mark.pipeline(repetition_time, slice_timing_interval)
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
    from arcana.weather.data import Weather  # See example in Data spaces section


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

    $ arcana derive output '/data/my-dataset' connectivity_matrix_plot \
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
    from arcana.medimage.data import Clinical

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


Advanced
--------

In every software framework, there are always corner cases that are
more complicated than the basic logic can handle. In designing
informatics frameworks, these challenges often arise when attempting to write
portable workflows, due to slight differences in the data and and end goals of
the application. This is particularly true in academia, where novelty is a key
criteria. To address these requirements, this section introduces some more
complex concepts, which can be used to customise and combine analysis methods
into powerful new classes: class inheritance (:ref:`inheritance`),
conditional pipelines (:ref:`conditional_pipelines`),
quality-control checks (:ref:`quality_control`) and sub-analyses (:ref:`subanalyses`).


.. _inheritance:

Inheritance
~~~~~~~~~~~

Given a toy example analysis class that has two text-file source columns, ``file1`` and
``file2``. The ``concat_pipeline`` builds a workflow that generates data for the sink
column ``concatenated`` and can be modified by the ``duplicates`` parameter.


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


The ``Concat`` class can be subclassed to create the ``ExtendedConcat`` class, which adds
one additional source column ``file3`` and another sink column ``doubly_concatenated``.
Data for ``doubly_concatenated`` is generated by the ``doubly_concat_pipeline``.

.. code-block:: python

    @analysis(Samples)
    class ExtendedConcat(Concat):

        # Source columns
        file3: Text = column("Another file to concatenate")

        # Sink columns
        concatenated = inherit()
        doubly_concatenated: Text = column("The doubly concatenated file")

        # Parameters
        duplicates = inherit(default=3)

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

Because the ``concatenated`` column and ``duplicates`` parameter are used in the
``doubly_concat_pipeline``, they are explicitly referenced in the subclass using the
``inherit_from`` function. Note, that this is enforced due a design decision to make it
clear where columns and parameters are defined when reading the code. Columns that
aren't explicitly referenced in the class (e.g. ``file1`` and ``file2``) can be omitted
from the subclass definition (but will still be present in the subclass). When
explicitly inheriting columns and parameters it is possible to override their attributes,
such as the default value for a given parameter (see ``duplicates`` in above example).


.. _conditional_pipelines:

Conditionals and switches
~~~~~~~~~~~~~~~~~~~~~~~~~

There are cases where different analysis methods need to be applied depending on the
requirements of a particular study or to deal with idiosyncrasies of a particular
dataset. There are two mechanisms for handling such cases in Arcana: "condition
expressions" and "switches".

Both condition expressions and switches are referenced within the ``@pipeline`` decorator.
When a condition expression or switch is set on a pipeline builder, that pipeline will
be used to generate data for a sink column only when certain criteria are met. If the criteria
aren't met, then either the default pipeline builder (one without either a switch or
condition expression) will be used if it is present or an "not produced" error will be
raised instead.

The difference between a condition expression and a switch is that a condition
expression is true or false over a whole dataset given a specific parameterisation,
whereas a switch can be true or false for different rows of the dataset depending on
the nature of the input data.

Condition expressions are specified as using the functions ``value_of(parameter)``
and ``is_provided(column)`` as placeholders for parameter values or whether a column
specification in the analysis is linked to a column in the dataset or not. In the
following example, a condition is used to enable the user whether ``concatenated``
should be generated by the ``concat_pipeline`` method (default) or
the ``reverse_concat_pipeline`` by setting the value of the ``order`` parameter.


.. code-block:: python

    @analysis(Samples)
    class OverridenConcat(Concat):

        # Source columns
        file1: Zip[Text] = inherit()
        file2: Text = inherit()

        # Sinks columns
        concatenated: Text = inherit()

        # Parameters
        duplicates = inherit(default=2)  # default value changed because we can
        order: str = parameter(
            "perform the concatenation in reverse order, i.e. file2 and then file1",
            choices=["forward", "reversed"],
            default="forward",
        )

        @pipeline(
            concatenated,
            condition=(value_of(order) == "reversed"),
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


Switches are defined in methods of the analysis class using the ``@switch`` decorator
and are similar pipeline builders in that they add nodes to a Pydra workflow passed to the
first argument. The sole output field of a switch must contain either be a boolean or
string, which specifies which branch of processing is to be performed. The switch
method is then passed to the ``@pipeline`` decorator via the ``switch`` keyword. If
the switch returns a string then the value passed to the ``switch`` keyword must be
tuple, with the first element the switch method and the second the value of the string
that will activate that branch of the pipeline to be run.

In the following example, the contents of the files in the ``concatenated`` column are
multiplied the value passed to the arbitrary ``multiplier`` parameter if the contents of
the input files ``file1`` and ``file2`` are numeric for the corresponding row as
determined by the ``inputs_are_numeric`` switch.

.. code-block:: python

    @analysis(Samples)
    class ConcatWithSwitch(Concat):

        # Source columns
        file1: Zip[Text] = inherit()
        file2: Text = inherit()

        # Sink columns
        concatenated: Text = inherit()
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

        @pipeline(multiplied, switch=inputs_are_numeric)
        def multiply_pipeline(self, wf, concatenated, multiplier):

            wf.add(
                multiply_contents(
                    name="concat", in_file=concatenated, multiplier=multiplier
                )
            )

            return wf.concat.lzout.out


.. _quality_control:

Quality-control checks
~~~~~~~~~~~~~~~~~~~~~~

When running complex analyses it is important to inspect generated derivatives
to make sure the workflows completed properly. In Arcana, it is possible to semi-automate
this process by adding quality-control "checks" to an analysis class.

In the following example the number of lines produced by the concatation step is checked
to see if it matches the number expected given the value of the ``duplicates`` parameter.

.. code-block:: python

    @analysis(Samples)
    class ConcatWithCheck(Concat):

        # Sink columns
        concatenated = inherit()

        # Parameters
        duplicates = inherit()

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


.. _subanalyses:

Sub-analyses
~~~~~~~~~~~~

When dealing with separate data streams that can be largely analysed in parallel
(e.g. multiple MRI contrasts), it can be convenient to combine multiple analyses tailored
to each stream into a single conglomerate analysis. This pattern can implemented in
Arcana using ``subanalysis`` attributes.

The type annotation of the ``subanalysis`` attribute specifies the analysis to be performed,
and the keyword arguments of specify mappings from the column specs and parameters
in the global namespace of the outer class to the namespace of the subanalysis. With these
mappings, source columns linked to specs in the global namespace can be passed to
the subanalysis, and sink columns generated by pipelines in the global namespace
can be linked to any column within the subanalysis.

The ``mapped_from`` function is used to map columns and parameters from subanalyses into
the global namespace, and takes two arguments, the name of the subanalysis and the name
of the column/parameter to map. By mapping a column/parameter into the global namespace
from one subanalysis and then mapping it back into another subanalysis the designer
can be stitched together. For example, the cortical surface reconstruction column from
a subanalysis for analysing anatomical MRI images could be mapped to a source column
in another subanalysis for analysing white matter tracts diffusion-weighted contrast
MRI images in order to constrain the potential endpoints of the tracts.

In the following example, two of the classes defined above, ``ExtendedConcat`` and
``ConcatWithSwitch`` are stitched together, so that the ``multiplied`` output column of
``ConcatWithSwitch`` is passed to the ``file3`` input column of ``ExtendedConcat``.
The ``duplicates`` parameter in each subanalysis are linked together so they are always
consistent by mapping it from the ``ExtendedConcat`` subanalysis to the global namespace
and then back into the ``ConcatWithSwitch``.

.. code-block:: python

    @analysis(Samples)
    class _ConcatWithSubanalyses:

        # Source columns mapped from "sub1" subanalysis so they can be shared across
        # both sub-analyses. Note that they could just as easily have been mapped from
        # "sub1" or recreated from scratch and mapped into both
        file1 = map_from("sub1", "file1")
        file2 = map_from("sub1", "file2")

        # Sink columns generated within the subanalyses mapped back out to the global
        # namespace so they can be mapped into the other subanalysis
        concat_and_multiplied = map_from("sub2", "multiplied")

        # Link the duplicates parameter across both subanalyses so it is always the same
        # by mapping a global parameter into both subanalyses
        common_duplicates = map_from(
            "sub1", "duplicates", default=5, salience=ps.check
        )

        # Additional parameters such as "multiplier" can be accessed within the subanalysis
        # class after the analysis class has been initialised using the 'sub2.multiplier'

        sub1: ExtendedConcat = subanalysis(
            "sub-analysis to add the 'doubly_concat' pipeline",
            # Feed the multiplied sink column from sub2 into the source column file3 of
            # the extended class
            file3=concat_and_multiplied,
        )
        sub2: ConcatWithSwitch = subanalysis(
            "sub-analysis to add the 'multiply' pipeline",
            file1=file1,
            file2=file2,
            # Use the concatenated generated by sub1 to avoid running it twice
            duplicates=common_duplicates,
        )


.. * sub-analysis arrays (e.g. for fMRI tasks)


.. .. _analysis_examples:

.. Examples
.. --------
