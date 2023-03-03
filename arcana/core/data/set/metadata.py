from __future__ import annotations
import typing as ty
import attrs
from arcana.core import PACKAGE_NAME, __version__, CODE_URL
from arcana.core.utils.misc import fromdict_converter


class Metadata:
    pass


@attrs.define
class ContainerMetadata(Metadata):

    type: str = None
    tag: str = None
    uri: str = None


@attrs.define
class GeneratorMetadata(Metadata):

    name: str = PACKAGE_NAME
    version: str = __version__
    description: str = f"Empty dataset created by {PACKAGE_NAME}"
    code_url: str = CODE_URL
    container: ContainerMetadata = attrs.field(
        default=None, converter=fromdict_converter(ContainerMetadata)
    )


@attrs.define
class SourceDatasetMetadata(Metadata):

    url: str = None
    doi: str = None
    version: str = None


DEFAULT_README = f"""
This dataset was specified/generated using the Arcana analysis workflows framework
{CODE_URL}. However, no specific README was provided so this template is used instead.

The dataset could have been defined on already existing data, using

    $ arcana dataset ...

or via the Python API
"""


@attrs.define(kw_only=True)
class DatasetMetadata(Metadata):

    name: str = attrs.field(default="Autogenerated-dataset")
    generated_by: ty.List[GeneratorMetadata] = attrs.field(
        converter=fromdict_converter(ty.List[GeneratorMetadata])
    )
    acknowledgements: str = attrs.field(default="Generic BIDS dataset", repr=False)
    authors: ty.List[str] = attrs.field(factory=list, repr=False)
    doi: str = attrs.field(default=None, repr=False)
    funding: ty.List[str] = attrs.field(factory=list, repr=False)
    license: str = attrs.field(default="CC0", repr=False)
    references: ty.List[str] = attrs.field(factory=list)
    how_to_acknowledge: str = attrs.field(default="see licence")
    ethics_approvals: ty.List[str] = attrs.field(factory=list)
    sources: ty.List[SourceDatasetMetadata] = attrs.field(
        factory=list, converter=fromdict_converter(ty.List[SourceDatasetMetadata])
    )
    readme: str = attrs.field(default=DEFAULT_README)
    type: str = attrs.field(default="derivative", repr=False)
    row_keys: list[str] = attrs.field(factory=list)

    @generated_by.default
    def generated_by_default(self):
        return [GeneratorMetadata()]

    def tobids(self):
        dct = {}
        dct["Name"] = self.name
        dct["Acknowledgements"] = self.acknowledgements
        dct["Authors"] = self.authors
        if self.doi:
            dct["DOI"] = self.doi
        dct["Funding"] = self.funding
        dct["License"] = self.license
        dct["References"] = self.references
        dct["HowToAcknowledge"] = self.how_to_acknowledge
        dct["EthicsApprovals"] = self.ethics_approvals
        dct["Readme"] = self.readme
        dct["GeneratedBy"] = [gb.tobids() for gb in self.generated_by]
        dct["Sources"] = [s.tobids() for s in self.sources]
        return dct

    @classmethod
    def frombids(cls, dct):
        return cls(
            name=dct.get("Name"),
            acknowledgements=dct.get("Acknowledgements"),
            authors=dct.get("Authors"),
            doi=dct.get("DOI"),
            funding=dct.get("Funding"),
            license=dct.get("License"),
            references=dct.get("References"),
            how_to_acknowledge=dct.get("HowToAcknowledge"),
            ethics_approvals=dct.get("EthicsApprovals"),
            readme=dct.get("Readme"),
            generated_by=[GeneratorMetadata(gb) for gb in dct["GeneratedBy"]],
            sources=[SourceDatasetMetadata(s) for s in dct["Sources"]],
        )


def metadata_converter(metadata):
    if not metadata:
        metadata = {}
    elif not isinstance(metadata, DatasetMetadata):
        metadata = DatasetMetadata(**metadata)
    return metadata