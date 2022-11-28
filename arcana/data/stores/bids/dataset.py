import attrs
import json
import typing as ty
from dataclasses import dataclass
from pathlib import Path
from arcana import __version__
from arcana.__about__ import PACKAGE_NAME, CODE_URL
from arcana.core.data.set import Dataset
from arcana.data.spaces.medimage import Clinical
from arcana.core.exceptions import ArcanaError, ArcanaEmptyDatasetError
from .structure import Bids


@dataclass
class ContainerMetadata:

    type: str = None
    tag: str = None
    uri: str = None

    def to_dict(self, **kwargs):
        dct = {}
        if self.type:
            dct["Type"] = self.type
        if self.tag:
            dct["Tag"] = self.tag
        if self.uri:
            dct["URI"] = self.uri
        return dct

    @classmethod
    def fromdict(cls, dct):
        if dct is None:
            return None
        return ContainerMetadata(
            type=dct.get("Type"), tag=dct.get("Tag"), uri=dct.get("URI")
        )


@dataclass
class GeneratorMetadata:

    name: str
    version: str = None
    description: str = None
    code_url: str = None
    container: ContainerMetadata = None

    def to_dict(self, **kwargs):
        dct = {"Name": self.name}
        if self.version:
            dct["Version"] = self.version
        if self.description:
            dct["Description"] = self.description
        if self.code_url:
            dct["CodeURL"] = self.code_url
        if self.container:
            dct["Container"] = self.container.to_dict()
        return dct

    @classmethod
    def fromdict(cls, dct):
        return GeneratorMetadata(
            name=dct["Name"],
            version=dct.get("Version"),
            description=dct.get("Description"),
            code_url=dct.get("CodeURL"),
            container=ContainerMetadata.fromdict(dct.get("Container")),
        )


@dataclass
class SourceDatasetMetadata:

    url: str = None
    doi: str = None
    version: str = None

    def to_dict(self, **kwargs):
        dct = {}
        if self.url:
            dct["URL"] = self.url
        if self.doi:
            dct["DOI"] = self.doi
        if self.version:
            dct["Version"] = self.version
        return dct

    @classmethod
    def fromdict(cls, dct):
        if dct is None:
            return None
        return SourceDatasetMetadata(
            url=dct.get("URL"), doi=dct.get("DOI"), version=dct.get("Version")
        )


@attrs.define
class BidsDataset(Dataset):
    """A representation of a "dataset" in Brain Imaging Data Structure (BIDS)
    format
    """

    name: str = attrs.field(default="Autogenerated-dataset")
    participants: ty.Dict[str, ty.Dict[str, str]] = attrs.field(
        factory=dict, repr=False
    )
    acknowledgements: str = attrs.field(default="Generic BIDS dataset", repr=False)
    authors: ty.List[str] = attrs.field(factory=list, repr=False)
    bids_version: str = attrs.field(default="1.0.1", repr=False)
    doi: str = attrs.field(default=None, repr=False)
    funding: ty.List[str] = attrs.field(factory=list, repr=False)
    bids_type: str = attrs.field(default="derivative", repr=False)
    license: str = attrs.field(default="CC0", repr=False)
    references: ty.List[str] = attrs.field(factory=list)
    how_to_acknowledge: str = attrs.field(default="see licence")
    ethics_approvals: ty.List[str] = attrs.field(factory=list)
    generated_by: ty.List[GeneratorMetadata] = attrs.field(factory=list)
    sources: ty.List[SourceDatasetMetadata] = attrs.field(factory=list)
    readme: str = attrs.field(default=None)

    def add_generator_metadata(self, **kwargs):
        self.generated_by.append(GeneratorMetadata(**kwargs))

    def add_source_metadata(self, **kwargs):
        self.sources.append(SourceDatasetMetadata(**kwargs))

    @classmethod
    def load(cls, path):
        if list(Path(path).glob("**/sub-*/ses-*")):
            hierarchy = ["subject", "timepoint"]
        else:
            hierarchy = ["session"]
        dataset = BidsDataset(path, store=Bids(), space=Clinical, hierarchy=hierarchy)
        dataset.load_metadata()
        return dataset

    @classmethod
    def create(
        cls,
        path,
        name,
        subject_ids,
        session_ids=None,
        readme=None,
        authors=None,
        generated_by=None,
        json_edits=None,
        **kwargs,
    ):
        path = Path(path)
        path.mkdir(exist_ok=True, parents=True)
        if session_ids is not None:
            hierarchy = ["subject", "timepoint"]
        else:
            hierarchy = ["session"]
        if generated_by is None:
            generated_by = [
                GeneratorMetadata(
                    name=PACKAGE_NAME,
                    version=__version__,
                    description=f"Empty dataset created by {PACKAGE_NAME}",
                    code_url=CODE_URL,
                )
            ]
        if readme is None:
            readme = "Mock readme\n" * 20
        if authors is None:
            authors = ["Mock A. Author", "Mock B. Author"]
        dataset = BidsDataset(
            path,
            store=Bids(json_edits=json_edits),
            space=Clinical,
            hierarchy=hierarchy,
            name=name,
            generated_by=generated_by,
            readme=readme,
            authors=authors,
            **kwargs,
        )
        # Create rows
        for subject_id in subject_ids:
            if not subject_id.startswith("sub-"):
                subject_id = f"sub-{subject_id}"
            dataset.participants[subject_id] = {}
            if session_ids:
                for session_id in session_ids:
                    if not session_id.startswith("sub-"):
                        session_id = f"ses-{session_id}"
                    row = dataset.add_leaf([subject_id, session_id])
                    Bids.absolute_row_path(row).mkdir(parents=True)
            else:
                row = dataset.add_leaf([subject_id])
                Bids.absolute_row_path(row).mkdir(parents=True, exist_ok=True)
        dataset.save_metadata()
        return dataset

    def is_multi_session(self):
        return len(self.hierarchy) > 1

    def save_metadata(self):
        if not self.participants:
            raise ArcanaEmptyDatasetError(
                "Dataset needs at least one participant before the metadata "
                "can be saved"
            )
        dct = {"Name": self.name, "BIDSVersion": self.bids_version}
        if self.bids_type:
            dct["DatasetType"] = self.bids_type
        if self.license:
            dct["Licence"] = self.license
        if self.authors:
            dct["Authors"] = self.authors
        if self.acknowledgements:
            dct["Acknowledgements"] = self.acknowledgements
        if self.how_to_acknowledge:
            dct["HowToAcknowledge"] = self.how_to_acknowledge
        if self.funding:
            dct["Funding"] = self.funding
        if self.ethics_approvals:
            dct["EthicsApprovals"] = self.ethics_approvals
        if self.references:
            dct["ReferencesAndLinks"] = self.references
        if self.doi:
            dct["DatasetDOI"] = self.doi
        if self.bids_type == "derivative":
            dct["GeneratedBy"] = [g.to_dict() for g in self.generated_by]
        if self.sources:
            dct["sourceDatasets"] = [d.to_dict() for d in self.sources]
        with open(self.root_dir / "dataset_description.json", "w") as f:
            json.dump(dct, f, indent="    ")

        with open(self.root_dir / "participants.tsv", "w") as f:
            col_names = list(next(iter(self.participants.values())).keys())
            f.write("\t".join(["participant_id"] + col_names) + "\n")
            for pcpt_id, pcpt_attrs in self.participants.items():
                f.write(
                    "\t".join([pcpt_id] + [pcpt_attrs[c] for c in col_names]) + "\n"
                )

        if self.readme is not None:
            with open(self.root_dir / "README", "w") as f:
                f.write(self.readme)

    def load_metadata(self):
        description_json_path = self.root_dir / "dataset_description.json"
        if not description_json_path.exists():
            raise ArcanaEmptyDatasetError(
                f"Could not find a directory at '{self.id}' containing a "
                "'dataset_description.json' file"
            )
        with open(description_json_path) as f:
            dct = json.load(f)
        self.name = dct["Name"]
        self.bids_version = dct["BIDSVersion"]
        self.bids_type = dct.get("DatasetType")
        self.license = dct.get("Licence")
        self.authors = dct.get("Authors", [])
        self.acknowledgements = dct.get("Acknowledgements")
        self.how_to_acknowledge = dct.get("HowToAcknowledge")
        self.funding = dct.get("Funding", [])
        self.ethics_approvals = dct.get("EthicsApprovals", [])
        self.references = dct.get("ReferencesAndLinks", [])
        self.doi = dct.get("DatasetDOI")
        if self.bids_type == "derivative":
            if "GeneratedBy" not in dct:
                raise ArcanaError(
                    "'GeneratedBy' field required for 'derivative' type " " datasets"
                )
            self.generated_by = [
                GeneratorMetadata.fromdict(d) for d in dct["GeneratedBy"]
            ]
        if "sourceDatasets" in dct:
            self.sources = [
                SourceDatasetMetadata.fromdict(d) for d in dct["sourceDatasets"]
            ]

        self.participants = {}
        with open(self.root_dir / "participants.tsv") as f:
            cols = f.readline()[:-1].split("\t")
            while True:
                line = f.readline()[:-1]
                if not line:
                    break

                d = dict(zip(cols, line.split("\t")))
                self.participants[d.pop("participant_id")] = d

        readme_path = self.root_dir / "README"
        if readme_path.exists():
            with open(readme_path) as f:
                self.readme = f.read()
        else:
            self.readme = None
