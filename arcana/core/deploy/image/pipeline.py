import typing as ty
from pathlib import Path
from itertools import chain
import logging
import shutil
import attrs
import yaml
from urllib.parse import urlparse
from deepdiff import DeepDiff
from neurodocker.reproenv import DockerRenderer
from arcana import __version__
from arcana.core.utils import ListDictConverter
from arcana.data.formats import Directory
from ..command import ContainerCommand
from .base import ContainerImage
from .components import (
    ContainerAuthor,
    License,
)


logger = logging.getLogger("arcana")


@attrs.define(kw_only=True)
class PipelineImage(ContainerImage):
    """
    name : str
        name of the package/pipeline
    version : str
        version of the package/pipeline
    org : str
        the organisation the image will be tagged within
    info_url : str
        the url of a documentation page describing the package
    authors : list[ContainerAuthor]
        list of authors of the package
    commands : list[ContainerCommand]
        list of available commands that are installed within the image
    base_image : str, optional
        the base image to build from
    package_manager : str, optional
        the package manager used to install system packages (should match OS on base image)
    python_packages:  Iterable[PipSpec or dict[str, str] or tuple[str, str]], optional
        Name and version of the Python PyPI packages to add to the image (in
        addition to Arcana itself)
    system_packages: Iterable[str], optional
        Name and version of operating system packages (see Neurodocker) to add
        to the image
    licenses : list[dict[str, str]], optional
        specification of licenses required by the commands in the container. Each dict
        should contain the 'name' of the license and the 'destination' it should be
        installed inside the container.
    spec_version : str, optional
        version of the specification relative to the package version, i.e. if the package
        version hasn't been updated but the specification has been altered, the spec
        version should be updated (otherwise builds will fail). The spec version should
        reset to "0" if the package version is updated.
    registry : str, optional
        the container registry the image is to be installed at
    """

    info_url: str = attrs.field()
    spec_version: str = attrs.field(default=0, converter=str)
    authors: ty.List[ContainerAuthor] = attrs.field(
        converter=ListDictConverter(ContainerAuthor)
    )
    commands: ty.List[ContainerCommand] = attrs.field(
        converter=ListDictConverter(ContainerCommand)
    )
    licenses: ty.List[License] = attrs.field(
        factory=list, converter=ListDictConverter(License)
    )
    loaded_from: Path = None

    def __attrs_post_init__(self):

        # Set back-references to this image in the command specs
        for cmd_spec in self.commands:
            cmd_spec.image = self

    @info_url.validator
    def info_url_validator(self, _, info_url):
        parsed = urlparse(info_url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(
                f"Could not parse info url '{info_url}', please include URL scheme"
            )

    @property
    def full_version(self):
        return (
            f"{self.version}-{self.spec_version}" if self.spec_version else self.version
        )

    def construct_dockerfile(self, build_dir: Path, **kwargs) -> DockerRenderer:
        """Constructs a dockerfile that wraps a with dependencies

        Parameters
        ----------
        build_dir : Path
            Path to the directory the Dockerfile will be written into copy any local
            files to
        **kwargs
            Passed onto the ContainerImage.construct_dockerfile() method

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

        self.write_spec(dockerfile, build_dir)

        return dockerfile

    def install_licenses(
        self,
        dockerfile: DockerRenderer,
        build_dir: Path,
    ):
        """Generate Neurodocker instructions to install licenses within the container image

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
            if lic.source is not None:
                build_path = license_build_dir / lic.name
                shutil.copyfile(lic.source, build_path)
                dockerfile.copy(
                    source=[str(build_path.relative_to(build_dir))],
                    destination=lic.destination,
                )
            else:
                logger.warning(
                    "License file for '%s' was not provided, will attempt to download "
                    "from %s%s dataset-level column at runtime",
                    lic.name,
                    lic.name,
                    self.LICENSE_SUFFIX,
                )

    def write_spec(self, dockerfile: DockerRenderer, build_dir):
        """Generate Neurodocker instructions to install README file inside the docker
        image

        Parameters
        ----------
        dockerfile : DockerRenderer
            the neurodocker renderer to append the install instructions to
        spec : dict
            the specification used to build the image
        build_dir : Path
            path to build dir
        """
        dct = attrs.asdict(self, filter=lambda a, v: not isinstance(v, type(self)))
        with open(build_dir / "arcana-spec.yaml", "w") as f:
            yaml.dump(dct, f)
        dockerfile.copy(source=["./arcana-spec.yaml"], destination=self.SPEC_PATH)

    @classmethod
    def load(
        cls,
        yaml_path: Path,
        root_dir: Path = None,
        licenses: dict[str, Path] = None,
        **kwargs,
    ):
        """Loads a deploy-build specification from a YAML file

        Parameters
        ----------
        yaml_path : Path
            path to the YAML file to load
        root_dir : Path, optional
            path to the root directory from which a tree of specs are being loaded from.
            The name of the root directory is taken to be the organisation the image
            belongs to, and all nested directories above the YAML file will be joined by
            '.' and prepended to the name of the loaded spec.
        licenses : dict[str, Path], optional
            Licenses that are provided at build time to be included in the image
        **kwargs
            additional keyword arguments that override/augment the values loaded from
            the spec file

        Returns
        -------
        Self
            The loaded spec object
        """

        def concat(loader, node):
            seq = loader.construct_sequence(node)
            return "".join([str(i) for i in seq])

        # Add special constructors to handle joins and concatenations within the YAML
        yaml.SafeLoader.add_constructor(tag="!join", constructor=concat)
        # yaml.SafeLoader.add_constructor(tag="!concat", constructor=concat)

        with open(yaml_path, "r") as f:
            dct = yaml.load(f, Loader=yaml.SafeLoader)

        if type(dct) is not dict:
            raise ValueError(f"{yaml_path!r} didn't contain a dict!")

        if root_dir is not None:
            dct["name"] = ".".join(
                yaml_path.relative_to(root_dir).parent.parts + (yaml_path.stem,)
            )
            dct["org"] = root_dir.name
        else:
            dct["name"] = yaml_path.stem
            dct["org"] = None
        dct["loaded_from"] = yaml_path.absolute()

        # Override/augment loaded values from spec
        dct.update(kwargs)

        return cls(**dct)

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
            tbl_info.write_row("Package version", self.version)
            tbl_info.write_row("Spec version", self.version)
            tbl_info.write_row("Base image", escaped_md(self.base_image))
            tbl_info.write_row(
                "Maintainer", f"{self.authors[0].name} ({self.authors[0].email})"
            )
            tbl_info.write_row("Info URL", self.info_url)

            f.write("\n")

            if self.licenses:
                f.write("### Required licenses\n")

                tbl_lic = MarkdownTable(f, "URL", "Info")
                for lic in self.licenses:
                    tbl_lic.write_row(
                        escaped_md(lic.info_url),
                        lic.description,
                    )

                f.write("\n")

            f.write("## Commands\n")

            for cmd in self.commands:

                f.write(f"### {cmd.name}\n")

                short_desc = cmd.long_description or cmd.description
                f.write(f"{short_desc}\n\n")

                tbl_cmd = MarkdownTable(f, "Key", "Value")
                tbl_cmd.write_row("Short description", cmd.description)
                # if cmd.configuration is not None:
                #     config = cmd.configuration
                #     # configuration keys are variable depending on the workflow class
                tbl_cmd.write_row("Operates on", cmd.row_frequency.name)

                for known_issue in cmd.known_issues:
                    tbl_cmd.write_row("Known issues", known_issue.url)

                f.write("#### Inputs\n")
                tbl_inputs = MarkdownTable(f, "Name", "Format", "Description")
                if cmd.inputs is not None:
                    for inpt in cmd.inputs:
                        tbl_inputs.write_row(
                            escaped_md(inpt.name),
                            self._data_format_html(inpt.stored_format),
                            inpt.description,
                        )
                    f.write("\n")

                f.write("#### Outputs\n")
                tbl_outputs = MarkdownTable(f, "Name", "Format", "Description")
                if cmd.outputs is not None:
                    for outpt in cmd.outputs:
                        tbl_outputs.write_row(
                            escaped_md(outpt.name),
                            self._data_format_html(outpt.stored_format),
                            outpt.description,
                        )
                    f.write("\n")

                if cmd.parameters is not None:
                    f.write("#### Parameters\n")
                    tbl_params = MarkdownTable(f, "Name", "Data type", "Description")
                    for param in cmd.parameters:
                        tbl_params.write_row(
                            escaped_md(param.name),
                            escaped_md(param.type),
                            param.description,
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

        sdict = attrs.asdict(self, recurse=True)
        odict = attrs.asdict(other, recurse=True)

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

    @classmethod
    def _data_format_html(cls, format):

        if ext := getattr(format, "ext", None):
            text = f"{format.desc} (`.{ext}`)"
        elif getattr(format, "is_dir", None) and format is not Directory:
            text = f"{format.desc} (Directory)"
        else:
            text = format.desc

        return f'<span data-toggle="tooltip" data-placement="bottom" title="{format.desc}" aria-label="{format.desc}">{text}</span>'

    DOCKERFILE_README_TEMPLATE = """
        The following Docker image was generated by Arcana v{} to enable the
        commands to be run in the XNAT container service. See
        https://raw.githubusercontent.com/Australian-Imaging-Service/arcana/main/LICENSE
        for licence.

        {}

        """

    SPEC_PATH = "/arcana-spec.yaml"
    IN_DOCKER_ARCANA_HOME_DIR = "/arcana-home"


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
