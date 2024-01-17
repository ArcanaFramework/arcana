from fileformats.core import DataType


class ContentsClassifier(DataType):
    pass


class SpeciesClassifier(ContentsClassifier):

    category = "species"
    category_code = ""


class HumanClassifier(SpeciesClassifier):

    name = "human"
    code = "SCTID:337915000"
