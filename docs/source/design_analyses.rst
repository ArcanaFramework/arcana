.. _design_analyses:

Designing Analyses
==================

.. warning::
    Under construction

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
covered in :ref:`inheritance` and :ref:`subanalyses` sections respectively.


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
or ``arcana menu`` CLI command. For large analysis classes with many column specs
this list could become overwhelming, so there is option to set the "salience"
of a column to a member of the :class:`.DataSalience` enum.

* primary - Primary input data, e.g. raw data or data reconstructed on the scanner 
* publication - Results that would typically be used as main outputs in publications 
* supplementary - Derivatives that would typically only be provided in supplementary material 
* qa - Derivatives that would typically be only kept for quality assurance of analysis workflows 
* debug - Derivatives that would typically only need to be checked when debugging analysis workflows 
* temp - Data only temporarily stored to pass between pipelines 

The row frequency of the column (e.g. per-session, per-subject, per-group, etc...
see :ref:`data_spaces` and :ref:`data_columns`) is specified by the ``frequency``
keyword argument, and should match the data space (see :ref:`data_spaces`)
provided to the :func:`arcana.core.mark.analysis` class decorator.

Descriptions and saliences can also be set for parameter attributes, where the
saliences are drawn from :class:`.ParamSalience` enum.


* default


.. _pipeline_constructors:

Pipeline constructors
---------------------

* frequency
* conditions, overloading
* accessing side-cars


.. _analysis_outputs:

Outputs
-------

* Outputs are for publication 


.. _inheritance:

Inheritance
-----------

* overriding methods
* accessing columns from base classes
* mixins

.. _subanalyses:

Sub-analyses
------------

* How to define sub-analyses
* sub-analysis arrays (e.g. for fMRI tasks)


Comprehensive example
---------------------

.. code-block:: python

    from pydra.tasks.mrtrix3.preprocess import FslPreproc
    from arcana.core.mark import Pipeline, analysis, column, pipeline
    from arcana.core.enum import DataSalience as ds
    from arcana.data.spaces.medicalimaging import ClinicalTrial
    from arcana.data.formats.medicalimaging import (
      DwiImage, NiftiGzXD, MrtrixIF, MrtrixTF)
  
  
    @analysis(ClinicalTrial)
    class DwiAnalysis():
  
        # Define the columns for the dataset.
        dw_images: DwiImage = column(
            "Reconstructed diffusion-weighted images acquired from scanner",
            salience=ds.primary)
        reverse_phase: DwiImage = column(
            "Reverse-phase encoded used to correct for phase-encoding distortions",
            salience=ds.primary)
        preprocessed: NiftiGzXD = column(
            "Preprocesed and corrected diffusion-weighted images", salience=ds.debug)
        wm_odf: MrtrixIF = column(
            "White matter orientation distributions", salience=ds.debug)
        afd: MrtrixIF = column(
            "Apparent fibre orientations", salience=ds.publication)
        global_tracks: MrtrixTF = column(
            "Tracking of white matter tracts across brain", salience=ds.publication)
  
        # Define a pipeline constructor method to generate the 'preprocessed'
        # derivative.
        @pipeline(preprocessed)
        def preprocess(self,
                       pipeline: Pipeline,
                       dw_images: NiftiGzXD,
                       reverse_phase: NiftiGzXD):
  
            # Add tasks to the pipeline using Pydra workflow syntax
            pipeline.add(
                FslPreproc(
                    name='preprocess',
                    in_file=dwi_images
                    reverse_phase=reverse_phase))
  
            pipeline.set_output(('preprocessed', pipeline.preprocess.out_file))
        