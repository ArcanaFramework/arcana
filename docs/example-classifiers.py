from fileformats.core import DataType


class MedimageClassifier(DataType):
    pass


class SpeciesClassifier(MedimageClassifier):

    category = "species"
    category_code = ""


class HumanClassifier(SpeciesClassifier):

    name = "human"
    code = "SCTID:337915000"
