from enum import Enum

class DataSalience(Enum):
    """An enum that holds the salience levels options that can be used when
    specifying data. Salience is used to indicate whether it would be best to
    store the data in the data store or whether it can be just stored in
    the local file-system and discarded after it has been used. This choice
    is ultimately specified by the user by defining a salience threshold for
    a store.

    The salience is also used when providing information on what sinks
    are available to avoid cluttering help menus
    """
    
    primary = (100, 'Primary input data, e.g. raw data or data reconstructed on '
               'the scanner')
    publication = (80, "Results that would typically be used as main outputs "
                   "in publications")
    supplementary = (60, 'Derivatives that would typically only be provided in '
                     'supplementary material')
    qa = (40, 'Derivatives that would typically be only kept for quality '
          'assurance of analysis workflows')
    debug = (20, 'Derivatives that would typically only need to be checked '
             'when debugging analysis workflows')
    temp = (0, "Data only temporarily stored to pass between pipelines")

    def __init__(self, level, desc):
        self.level = level
        self.desc = desc

    def __lt__(self, other):
        return self.level < other.level

    def __le__(self, other):
        return self.level <= other.level

    def __str__(self):
        return self.name

    
class DataQuality(Enum):
    """The quality of a data item. Can be manually specified or set by
    automatic quality control methods
    """
    
    usable = 100
    noisy = 75
    questionable = 50
    artefactual = 25
    unusable = 0

    def __eq__(self, other):
        return self.value == other.value

    def __lt__(self, other):
        return self.value < other.value

    def __le__(self, other):
        return self.value <= other.value
