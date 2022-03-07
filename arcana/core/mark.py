from enum import Enum


class ParamSalience(Enum):
    debug = (0, "typically only needed to be altered for debugging")
    recommended = (20, "recommended to keep defaults")
    dependent = (40, "best value can be dependent on the context of the analysis, but the default should work for most cases")
    check = (60, "default value should be checked for validity for particular use case")
    arbitrary = (80, "a default is provided, but it is not clear which value is best")
    required = (100, "No sensible default value, should be provided")
