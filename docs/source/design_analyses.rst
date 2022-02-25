.. _design_analyses:

Designing Analysis classes
==========================


.. warning::
    Under construction



.. code-block:: python

  from pydra.tasks.mrtrix3.preprocess import FslPreproc
  from arcana.core.mark import Pipeline, analysis, column, pipeline
  from arcana.core.enum import DataSalience as ds
  from arcana.data.formats.medicalimaging import (
    DwiImage, NiftiGzXD, MrtrixIF, MrtrixTF)


  @analysis
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
            name='preprocess',
            task=FslPreproc(
              in_file=dwi_images
              reverse_phase=reverse_phase))

          pipeline.set_output(('preprocessed', pipeline.preprocess.out_file))
      