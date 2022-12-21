from __future__ import annotations
import typing as ty
from pathlib import Path
from itertools import chain
import re
import logging
import shlex
import shutil
import attrs
import yaml
from urllib.parse import urlparse
from deepdiff import DeepDiff
from neurodocker.reproenv import DockerRenderer
from arcana.core import __version__
from arcana.core.utils.serialize import (
    ObjectConverter,
    ObjectListConverter,
    ClassResolver,
)
from arcana.core.data.type import BaseDirectory
from ..command.base import ContainerCommand
from .base import ArcanaImage
from .components import ContainerAuthor, License, KnownIssue


logger = logging.getLogger("arcana")


@attrs.define(kw_only=True)
class App(ArcanaImage):
    """A container image that contains a command with specific inputs and outputs to run.

    Parameters
    ----------
    name : str
        name of the package/pipeline
    version : str
        version of the package/pipeline
    org : str
        the organisation the image will be tagged within
    base_image : BaseImage, optional
        the base image to build from
    packages : Packages, optional
        the package manager used to install system packages (should match OS on base image)
    registry : str, optional
        the container registry the image is to be installed at
    info_url : str
        the url of a documentation page describing the package
    authors : list[ContainerAuthor]
        list of authors of the package
    description : str
        single line description to be when referring to the pipeline in UIs
    command : ContainerCommand
        description of the command that is to be run within the image
    licenses : list[dict[str, str]], optional
        specification of licenses required by the commands in the container. Each dict
        should contain the 'name' of the license and the 'destination' it should be
        installed inside the container.
    build_iteration : str, optional
        version of the specification relative to the package version, i.e. if the package
        version hasn't been updated but the specification has been altered, the spec
        version should be updated (otherwise builds will fail). The spec version should
        reset to "0" if the package version is updated.
    long_description : str
        Multi-line description of the pipeline used in documentation
    known_issues : dict
        Any known issues with the pipeline. To be used in auto-doc generation
    loaded_from : Path
        the file the spec was loaded from, if applicable
    """

    SPEC_PATH = "/arcana-spec.yaml"
    IN_DOCKER_ARCANA_HOME_DIR = "/arcana-home"

    SUBPACKAGE = "deploy"

    info_url: str = attrs.field()
    authors: ty.List[ContainerAuthor] = attrs.field(
        converter=ObjectListConverter(ContainerAuthor),
        metadata={"serializer": ObjectListConverter.asdict},
    )
    description: str
    command: ContainerCommand = attrs.field(converter=ObjectConverter(ContainerCommand))
    licenses: list[License] = attrs.field(
        factory=dict,
        converter=ObjectListConverter(License),
        metadata={"serializer": ObjectListConverter.asdict},
    )
    known_issues: list[KnownIssue] = attrs.field(
        factory=list,
        converter=ObjectListConverter(KnownIssue),
        metadata={"serializer": ObjectListConverter.asdict},
    )
    long_description: str = ""
    loaded_from: Path = attrs.field(default=None, metadata={"asdict": False})
    arcana_version: str = __version__

    def __attrs_post_init__(self):

        # Set back-references to this image in the command spec
        self.command.image = self

    @info_url.validator
    def info_url_validator(self, _, info_url):
        parsed = urlparse(info_url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(
                f"Could not parse info url '{info_url}', please include URL scheme"
            )

    def add_entrypoint(self, dockerfile: DockerRenderer, build_dir: Path):

        command_line = (
            self.command.activate_conda_cmd() + "arcana deploy pipeline-entrypoint"
        )

        dockerfile.entrypoint(shlex.split(command_line))

    def construct_dockerfile(self, build_dir: Path, **kwargs) -> DockerRenderer:
        """Constructs a dockerfile that wraps a with dependencies

        Parameters
        ----------
        build_dir : Path
            Path to the directory the Dockerfile will be written into copy any local
            files to
        **kwargs
            Passed onto the ArcanaImage.construct_dockerfile() method

        Returns
        -------
        DockerRenderer
            Neurodocker Docker renderer to construct dockerfile from
        """

        dockerfile = super().construct_dockerfile(build_dir, **kwargs)

        self.install_licenses(
            dockerfile,
            build_dir,
        )

        self.insert_spec(dockerfile, build_dir)

        self.add_entrypoint(dockerfile, build_dir)

        return dockerfile

    def install_licenses(
        self,
        dockerfile: DockerRenderer,
        build_dir: Path,
    ):
        """Generate Neurodocker instructions to install licenses within the container
        image

        Parameters
        ----------
        dockerfile : DockerRenderer
            the neurodocker renderer to append the install instructions to
        build_dir : Path
            path to build dir
        """
        # Copy licenses into build directory
        license_build_dir = build_dir / "licenses"
        license_build_dir.mkdir()
        for lic in self.licenses:
            if lic.store_in_image:
                if lic.source:
                    build_path = license_build_dir / lic.name
                    shutil.copyfile(lic.source, build_path)
                    dockerfile.copy(
                        source=[str(build_path.relative_to(build_dir))],
                        destination=str(lic.destination),
                    )
                else:
                    logger.warning(
                        "License file for '%s' was not provided, will attempt to download "
                        "from '%s' dataset-level column or site-wide license dataset at "
                        "runtime",
                        lic.name,
                        lic.column_name(lic.name),
                    )

    def insert_spec(self, dockerfile: DockerRenderer, build_dir):
        """Generate Neurodocker instructions to save the specification inside the built
        image to be used when running the command and comparing against future builds

        Parameters
        ----------
        dockerfile : DockerRenderer
            the neurodocker renderer to append the install instructions to
        spec : dict
            the specification used to build the image
        build_dir : Path
            path to build dir
        """
        self.save(build_dir / "arcana-spec.yaml")
        dockerfile.copy(source=["./arcana-spec.yaml"], destination=self.SPEC_PATH)

    def save(self, yml_path: Path):
        """Saves the specification to a YAML file that can be loaded again

        Parameters
        ----------
        yml_path : Path
            path to file to save the spec to
        """
        yml_dct = self.asdict()
        yml_dct["type"] = ClassResolver.tostr(self, strip_prefix=False)
        with open(yml_path, "w") as f:
            yaml.dump(yml_dct, f)

    @classmethod
    def load(
        cls,
        yml: ty.Union[Path, dict],
        root_dir: Path = None,
        license_paths: dict[str, Path] = None,
        licenses_to_download: set[str] = None,
        **kwargs,
    ):
        """Loads a deploy-build specification from a YAML file

        Parameters
        ----------
        yml : Path or dict
            path to the YAML file to load or loaded dictionary
        root_dir : Path, optional
            path to the root directory from which a tree of specs are being loaded from.
            The name of the root directory is taken to be the organisation the image
            belongs to, and all nested directories above the YAML file will be joined by
            '.' and prepended to the name of the loaded spec.
        license_paths : dict[str, Path], optional
            Licenses that are provided at build time to be included in the image.
        licenses_to_download : set[str], optional
            Licenses that are to be downloaded at runtime. If `license_paths` is not
            None (i.e. how to access required licenses are to be specified) then required
            licenses that are not in license_paths need to be explicitly listed in
            `licenses_to_download` otherwise an error is raised
        **kwargs
            additional keyword arguments that override/augment the values loaded from
            the spec file

        Returns
        -------
        Self
            The loaded spec object
        """

        if isinstance(yml, str):
            yml = Path(yml)
        if isinstance(yml, Path):
            yml_dict = cls._load_yaml(yml)
            if type(yml_dict) is not dict:
                raise ValueError(f"{yml!r} didn't contain a dict!")

            if "name" not in yml_dict:
                if root_dir is not None:
                    yml_dict["name"] = ".".join(
                        yml.relative_to(root_dir).parent.parts + (yml.stem,)
                    )
                else:
                    yml_dict["name"] = yml.stem

            if "org" not in yml_dict:
                if root_dir is not None:
                    yml_dict["org"] = root_dir.name
                else:
                    yml_dict["org"] = None

            yml_dict["loaded_from"] = yml.absolute()
        else:
            yml_dict = yml

        yml_dict.pop("type", None)  # Remove "type" from dict if present

        # Override/augment loaded values from spec
        yml_dict.update(kwargs)

        image = cls(**yml_dict)

        # Explicitly override directive in loaded spec to store license in the image

        if license_paths is not None:
            for lic in image.licenses:
                if lic.name in licenses_to_download:
                    lic.store_in_image = False
                if lic.store_in_image:
                    try:
                        lic.source = license_paths[lic.name]
                    except KeyError:
                        raise RuntimeError(
                            f"{lic.name} license has not been provided when it is "
                            "specified to be stored in the image"
                        )

        return image

    @classmethod
    def _load_yaml(cls, yaml_file: ty.Union[Path, str]):
        def yaml_join(loader, node):
            seq = loader.construct_sequence(node)
            return "".join([str(i) for i in seq])

        # Add special constructors to handle joins and concatenations within the YAML
        yaml.SafeLoader.add_constructor(tag="!join", constructor=yaml_join)
        with open(yaml_file, "r") as f:
            dct = yaml.load(f, Loader=yaml.SafeLoader)
        return dct

    @classmethod
    def load_tree(cls, root_dir: Path, **kwargs) -> list:
        """Walk the given directory structure and load all specs found within it

        Parameters
        ----------
        root_dir : Path
            path to the base directory
        """
        if root_dir.is_file():
            return [cls.load(root_dir, **kwargs)]
        specs = []
        for path in chain(root_dir.rglob("*.yml"), root_dir.rglob("*.yaml")):
            if not any(p.startswith(".") for p in path.parts):

                logging.info("Found container image specification file '%s'", path)
                specs.append(cls.load(path, root_dir=root_dir, **kwargs))

        return specs

    def autodoc(self, doc_dir, flatten: bool):
        header = {
            "title": self.name,
            "weight": 10,
        }

        if self.loaded_from:
            header["source_file"] = str(self.loaded_from)

        if flatten:
            out_dir = doc_dir
        else:
            assert isinstance(doc_dir, Path)

            out_dir = doc_dir.joinpath(*self.name.split(".")[:-1])

            assert doc_dir in out_dir.parents or out_dir == doc_dir

            out_dir.mkdir(parents=True, exist_ok=True)

        with open(f"{out_dir}/{self.name}.md", "w") as f:
            f.write("---\n")
            yaml.dump(header, f)
            f.write("\n---\n\n")

            f.write("## Package Info\n")
            tbl_info = MarkdownTable(f, "Key", "Value")
            tbl_info.write_row("Name", self.name)
            tbl_info.write_row("App version", self.version)
            tbl_info.write_row(
                "Build iteration", self.build_iteration if self.build_iteration else "0"
            )
            tbl_info.write_row("Base image", escaped_md(self.base_image.reference))
            tbl_info.write_row(
                "Maintainer", f"{self.authors[0].name} ({self.authors[0].email})"
            )
            tbl_info.write_row("Info URL", self.info_url)
            tbl_info.write_row("Short description", self.description)
            for known_issue in self.known_issues:
                tbl_info.write_row("Known issues", known_issue.url)

            short_desc = self.long_description or self.description
            f.write(f"\n{short_desc}\n\n")

            if self.licenses:
                f.write("### Required licenses\n")

                tbl_lic = MarkdownTable(f, "Name", "URL", "Description")
                for lic in self.licenses:
                    tbl_lic.write_row(
                        lic.name,
                        escaped_md(lic.info_url),
                        lic.description.strip(),
                    )

                f.write("\n")

            f.write("## Command\n")

            tbl_cmd = MarkdownTable(f, "Key", "Value")

            # if self.command.configuration is not None:
            #     config = self.command.configuration
            #     # configuration keys are variable depending on the workflow class
            tbl_cmd.write_row("Task", ClassResolver.tostr(self.command.task))
            freq_name = (
                self.command.row_frequency.name
                if not isinstance(self.command.row_frequency, str)
                else re.match(r".*\[(\w+)\]", self.command.row_frequency).group(1)
            )
            tbl_cmd.write_row("Operates on", freq_name)

            f.write("#### Inputs\n")
            tbl_inputs = MarkdownTable(
                f, "Name", "Data type", "Stored data type default", "Description"
            )
            if self.command.inputs is not None:
                for inpt in self.command.inputs:
                    tbl_inputs.write_row(
                        escaped_md(inpt.name),
                        self._data_format_html(inpt.datatype),
                        self._data_format_html(inpt.default_column.datatype),
                        inpt.help_string,
                    )
                f.write("\n")

            f.write("#### Outputs\n")
            tbl_outputs = MarkdownTable(
                f, "Name", "Data type", "Stored data type default", "Description"
            )
            if self.command.outputs is not None:
                for outpt in self.command.outputs:
                    tbl_outputs.write_row(
                        escaped_md(outpt.name),
                        self._data_format_html(outpt.datatype),
                        self._data_format_html(outpt.default_column.datatype),
                        outpt.help_string,
                    )
                f.write("\n")

            if self.command.parameters is not None:
                f.write("#### Parameters\n")
                tbl_params = MarkdownTable(f, "Name", "Data type", "Description")
                for param in self.command.parameters:
                    tbl_params.write_row(
                        escaped_md(param.name),
                        escaped_md(ClassResolver.tostr(param.datatype)),
                        param.help_string,
                    )
                f.write("\n")

    def compare_specs(self, other, check_version=True):
        """Compares two build specs against each other and returns the difference

        Parameters
        ----------
        s1 : dict
            first spec
        s2 : dict
            second spec
        check_version : bool
            check the arcana version used to generate the specs

        Returns
        -------
        DeepDiff
            the difference between the specs
        """

        sdict = self.asdict()
        odict = other.asdict()

        def prep(s):
            dct = {
                k: v
                for k, v in s.items()
                if (not k.startswith("_") and (v or isinstance(v, bool)))
            }
            if check_version:
                if "arcana_version" not in dct:
                    dct["arcana_version"] = __version__
            else:
                del dct["arcana_version"]
            return dct

        diff = DeepDiff(prep(sdict), prep(odict), ignore_order=True)
        return diff

    # @classmethod
    # def load_in_image(cls, spec_path: Path = SPEC_PATH):
    #     yml_dct = cls._load_yaml(spec_path)
    #     klass = ClassResolver(cls)(yml_dct.pop("type"))
    #     return klass.load(yml_dct)

    @classmethod
    def _data_format_html(cls, datatype):

        if isinstance(datatype, str):
            module, name = datatype.split(":")
            name = name.lower()
            text = f"{name} (from '{module}' extension)"
        else:
            if ext := getattr(datatype, "ext", None):
                text = f"{datatype.desc} (`.{ext}`)"
            elif getattr(datatype, "is_dir", None) and datatype is not BaseDirectory:
                text = f"{datatype.desc} (directory)"
            else:
                text = datatype.desc

            name = datatype.__name__.lower()

        return (
            f'<span data-toggle="tooltip" data-placement="bottom" title="{name}" '
            f'aria-label="{name}">{text}</span>'
        )

    DOCKERFILE_README_TEMPLATE = """
        The following Docker image was generated by Arcana v{} to enable the
        commands to be run in the XNAT container service. See
        https://raw.githubusercontent.com/Australian-Imaging-Service/arcana/main/LICENSE
        for licence.

        {}

        """


class MarkdownTable:
    def __init__(self, f, *headers: str) -> None:
        self.headers = tuple(headers)

        self.f = f
        self._write_header()

    def _write_header(self):
        self.write_row(*self.headers)
        self.write_row(*("-" * len(x) for x in self.headers))

    def write_row(self, *cols: str):
        cols = list(cols)
        if len(cols) > len(self.headers):
            raise ValueError(
                f"More entries in row ({len(cols)} than columns ({len(self.headers)})"
            )

        # pad empty column entries if there's not enough
        cols += [""] * (len(self.headers) - len(cols))

        # TODO handle new lines in col
        self.f.write(
            "|" + "|".join(str(col).replace("|", "\\|") for col in cols) + "|\n"
        )


def escaped_md(value: str) -> str:
    if not value:
        return ""
    return f"`{value}`"
