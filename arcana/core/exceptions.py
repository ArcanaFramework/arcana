class ArcanaException(Exception):
    @property
    def msg(self):
        return self.args[0]

    @msg.setter
    def msg(self, msg):
        self.args = (msg,) + self.args[1:]


class ArcanaError(ArcanaException):
    pass


class ArcanaRuntimeError(ArcanaError):
    pass


class ArcanaNotBoundToAnalysisError(ArcanaError):
    pass


class ArcanaVersionError(ArcanaError):
    pass


class ArcanaRequirementNotFoundError(ArcanaVersionError):
    pass


class ArcanaVersionNotDetectableError(ArcanaVersionError):
    pass


class ArcanaEnvModuleNotLoadedError(ArcanaError):
    pass


class ArcanaMissingInputError(ArcanaException):
    pass


class ArcanaProtectedOutputConflictError(ArcanaError):
    pass


class ArcanaCantPickleAnalysisError(ArcanaError):
    pass


class ArcanaRepositoryError(ArcanaError):
    pass


class ArcanaUsageError(ArcanaError):
    pass


class ArcanaCacheError(ArcanaError):
    pass


class ArcanaDesignError(ArcanaError):
    pass


class NamedArcanaError(ArcanaError):
    def __init__(self, name, msg):
        super(NamedArcanaError, self).__init__(msg)
        self.name = name


class ArcanaNameError(NamedArcanaError):
    pass


class ArcanaWrongFrequencyError(NamedArcanaError):
    pass


class ArcanaIndexError(ArcanaError):
    def __init__(self, index, msg):
        super(ArcanaIndexError, self).__init__(msg)
        self.index = index


class ArcanaDataNotDerivedYetError(NamedArcanaError, ArcanaDesignError):
    pass


class ArcanaDataMatchError(ArcanaUsageError):
    pass


class ArcanaPipelinesStackError(ArcanaError):
    pass


class ArcanaMissingDataException(ArcanaPipelinesStackError):
    pass


class ArcanaOutputNotProducedException(ArcanaPipelinesStackError):
    """
    Raised when a given spec is not produced due to switches and inputs
    provided to the analysis
    """


class ArcanaInsufficientRepoDepthError(ArcanaError):
    pass


class ArcanaFileFormatError(ArcanaError):
    pass


class ArcanaLicenseNotFoundError(ArcanaNameError):
    pass


class ArcanaUnresolvableFormatException(ArcanaException):
    pass


class ArcanaFileGroupNotCachedException(ArcanaException):
    pass


class NoMatchingPipelineException(ArcanaException):
    pass


class ArcanaModulesError(ArcanaError):
    pass


class ArcanaModulesNotInstalledException(ArcanaException):
    pass


class ArcanaJobSubmittedException(ArcanaException):
    """
    Signifies that a pipeline has been submitted to a scheduler and
    a return value won't be returned.
    """


class ArcanaNoRunRequiredException(ArcanaException):
    """
    Used to signify when a pipeline doesn't need to be run as all
    required outputs are already present in the store
    """


class ArcanaFileFormatClashError(ArcanaError):
    """
    Used when two mismatching data formats are registered with the same
    name or extension
    """


class ArcanaFormatConversionError(ArcanaError):
    "No converters exist between formats"


class ArcanaConverterNotAvailableError(ArcanaError):
    "The converter required to convert between formats is not"
    "available"


class ArcanaReprocessException(ArcanaException):
    pass


class ArcanaWrongRepositoryError(ArcanaError):
    pass


class ArcanaIvalidParameterError(ArcanaError):
    pass


class ArcanaRequirementVersionsError(ArcanaError):
    pass


class ArcanaXnatCommandError(ArcanaRepositoryError):
    """
    Error in the command file used to access an XNAT repository via the XNAT
    container service.
    """


class ArcanaUriAlreadySetException(ArcanaException):
    """Raised when attempting to set the URI of an item is already set"""


class ArcanaDataTreeConstructionError(ArcanaError):
    "Error in constructing data tree by store find_rows method"


class ArcanaBadlyFormattedIDError(ArcanaDataTreeConstructionError):
    "Error attempting to extract an ID from a tree path using a user provided regex"


class ArcanaWrongDataSpaceError(ArcanaError):
    "Provided row_frequency is not a valid member of the dataset's dimensions"


class ArcanaNoDirectXnatMountException(ArcanaException):
    "Raised when attemptint to access a file-system mount for a row that hasn't been mounted directly"
    pass


class ArcanaEmptyDatasetError(ArcanaException):
    pass


class ArcanaBuildError(ArcanaError):
    pass
