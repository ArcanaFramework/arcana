from enum import Enum


class ColumnSalience(Enum):
    """An enum that holds the salience levels options that can be used when
    specifying data columns. Salience is used to indicate whether it would be best to
    store the data in the data store or whether it can be just stored in
    the local file-system and discarded after it has been used. This choice
    is ultimately specified by the user by defining a salience threshold for
    a store.

    The salience is also used when providing information on what sinks
    are available to avoid cluttering help menus
    """

    primary = (
        100,
        "Primary input data, typically reconstructed by the instrument that "
        "collects them",
    )
    raw = (
        90,
        "Raw data from the scanner that haven't been reconstructed and are "
        "only typically used in advanced analyses",
    )
    publication = (
        80,
        "Results that would typically be used as main outputs in publications",
    )
    supplementary = (
        60,
        "Derivatives that would typically only be provided in "
        "supplementary material",
    )
    qa = (
        40,
        "Derivatives that would typically be only kept for quality "
        "assurance of analysis workflows",
    )
    debug = (
        20,
        "Derivatives that would typically only need to be checked "
        "when debugging analysis workflows",
    )
    temp = (
        0,
        "Data only temporarily stored to pass between pipelines, e.g. that "
        "operate on different row frequencies",
    )

    def __init__(self, level, desc):
        self.level = level
        self.desc = desc

    def __lt__(self, other):
        return self.level < other.level

    def __le__(self, other):
        return self.level <= other.level

    def __str__(self):
        return self.name

    @classmethod
    def default(self):
        return self.supplementary


class ParameterSalience(Enum):
    """An enum that holds the salience levels options that can be used when
    specifying class parameters. Salience is used to indicate whether the
    parameter should show up by default when listing the available parameters
    of an Analysis class in a menu.
    """

    def __str__(self):
        return self.name

    debug = (0, "typically only needed to be altered for debugging")
    recommended = (20, "recommended to keep defaults")
    dependent = (
        40,
        "best value can be dependent on the context of the analysis, but the default "
        "should work for most cases",
    )
    check = (60, "default value should be checked for validity for particular use case")
    arbitrary = (80, "a default is provided, but it is not clear which value is best")
    required = (100, "No sensible default value, should be provided")

    @classmethod
    def default(self):
        return self.recommended


class DataQuality(Enum):
    """The quality of a data item. Can be manually specified or set by
    automatic quality control methods
    """

    usable = 100
    noisy = 75
    questionable = 50
    artefactual = 25
    unusable = 0

    def __str__(self):
        return self.name

    def __eq__(self, other):
        return self.value == other.value

    def __lt__(self, other):
        return self.value < other.value

    def __le__(self, other):
        return self.value <= other.value

    @classmethod
    def default(self):
        return self.questionable


class CheckSalience(Enum):
    """An enum that holds the potential values for signifying how critical a check is to
    run.
    """

    def __str__(self):
        return self.name

    debug = (0, "typically only used to debug alterations to the pipeline")
    potential = (20, "check can be run but not typically necessary")
    prudent = (
        40,
        "it is prudent to run the check the results but you can skip if required",
    )
    recommended = (
        60,
        "recommended to run the check as pipeline fails 1~2% of the time",
    )
    strongly_recommended = (
        80,
        "strongly recommended to run the check as pipeline fails 5~10% of the time",
    )
    required = (100, "Pipeline will often fail, checking the results is required")

    @classmethod
    def default(self):
        return self.recommended


class CheckStatus(Enum):
    """An enum that holds the potential values that signify how likely a pipeline has "
    "failed"""

    def __str__(self):
        return self.name

    failed = (0, "the pipeline has failed")
    probable_fail = (25, "probable that the pipeline has failed")
    unclear = (
        50,
        "cannot ascertain whether the pipeline has failed or not",
    )
    probable_pass = (75, "probable that the pipeline has run successfully")
    passed = (100, "the pipeline has run successfully")

    @classmethod
    def default(self):
        return self.unclear
