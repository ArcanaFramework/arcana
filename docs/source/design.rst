Design
======

Arcana_ is designed with an object-oriented philosophy, with
the acquired and derived data sets along with the analysis pipelines
used to derive the derived data sets encapsulated within "Analysis" classes.

The Arcana_ package itself only provides the abstract *Analysis* and
*MultiAnalysis* base classes, which are designed to be sub-classed to provide
specialised classes representing the analysis that can be performed on specific
types of data (e.g. BoldAnalysis, PetAnalysis). These specific classes can then
be sub-classed further into classes that are specific to a particular analysis,
and integrate complete analysis workflows from preprocessing to statistics.