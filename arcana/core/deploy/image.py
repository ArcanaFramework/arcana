import setuptools.sandbox
import typing as ty
from pathlib import Path
import json
import tempfile
from itertools import chain
import logging
from datetime import datetime
from copy import copy
import shutil
from natsort import natsorted
import attrs
import docker
import yaml
from deepdiff import DeepDiff
from neurodocker.reproenv import DockerRenderer
from arcana import __version__
from arcana.core.utils import set_cwd, ListDictConverter, resolve_class
from arcana.__about__ import PACKAGE_NAME, python_versions
from arcana.exceptions import ArcanaBuildError
from .command import ContainerCommandSpec
from arcana.data.formats import Directory
from .utils import (
    PipSpec,
    local_package_location,
    DOCKER_HUB,
    MarkdownTable,
    escaped_md,
)


logger = logging.getLogger("arcana")


@attrs.define
class ContainerAuthor:

    name: str
    email: str


@attrs.define
class SystemPackage:

    name: str
    version: str


@attrs.define
class LicenseSpec:

    name: str
    destination: str
    description: str
    link: str


@attrs.define
class NeurodockerPackage:

    name: str
    version: str


@attrs.define
class ContainerImageSpec:
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
    commands : list[ContainerCommandSpec]
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
    package_templates : Iterable[dict[str, str]]
        Neurodocker package installation templates to be installed inside the image. A
        dictionary containing the 'name' and 'version' of the template along
        with any additional keyword arguments required by the template
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
    builtin_licenses : dict[str, Path]
        licenses provided that can be built into the image (i.e. only if the
        licensing terms permit), which are provided at build time. The remaining licenses
        specified in the build will be downloaded at runtime either from the root node
        of the dataset or the "site-wide-license dataset"
    site_licenses_dataset : tuple[str, str, str]

    """

    DEFAULT_BASE_IMAGE = "ubuntu:kinetic"
    DEFAULT_PACKAGE_MANAGER = "apt"

    name: str
    version: str
    info_url: str
    authors: ty.List[ContainerAuthor] = attrs.field(
        converter=ListDictConverter(ContainerAuthor)
    )
    commands: ty.List[ContainerCommandSpec] = attrs.field(
        converter=ListDictConverter(ContainerCommandSpec)
    )
    base_image: str = DEFAULT_BASE_IMAGE
    package_manager: str = DEFAULT_PACKAGE_MANAGER
    org: str = None
    python_packages: ty.List[PipSpec] = attrs.field(
        factory=list, converter=ListDictConverter(PipSpec)
    )
    system_packages: ty.List[SystemPackage] = (
        attrs.field(factory=list, converter=ListDictConverter(SystemPackage)),
    )
    package_templates: ty.List[NeurodockerPackage] = attrs.field(
        factory=list, converter=ListDictConverter(NeurodockerPackage)
    )
    licenses: ty.List[LicenseSpec] = attrs.field(
        factory=list, converter=ListDictConverter(LicenseSpec)
    )
    spec_version: str = attrs.field(default=0, converter=str)
    registry: str = DOCKER_HUB
    builtin_licenses: dict[str, Path] = attrs.field(factory=dict)
    site_licenses_dataset: tuple[str, str, str] = None
    loaded_from: Path = None

    def __attrs_post_init__(self):

        # Set back-references to this image in the command specs
        for cmd_spec in self.commands:
            cmd_spec.image = self

    @property
    def tag(self):
        return f"{self.path}:{self.full_version}"

    @property
    def full_version(self):
        ver = str(self.version)
        if self.spec_version is not None:
            ver += f"-{self.spec_version}"
        return ver

    @property
    def path(self):
        prefix = self.registry + "/" if self.registry != DOCKER_HUB else ""
        org_str = self.org + "/" if self.org else ""
        return (prefix + org_str + self.name).lower()

    def make(
        self,
        build_dir: Path = None,
        generate_only: bool = False,
        **kwargs,
    ):
        """Makes the container image from the spec: generates the Dockerfile and then
        builds it.

        Parameters
        ----------
        build_dir : Path, optional
            _description_, by default None
        """

        if build_dir is None:
            build_dir = tempfile.mkdtemp()
        build_dir = Path(build_dir)

        dockerfile = self.construct_dockerfile(build_dir, **kwargs)

        if not generate_only:
            self.build(dockerfile, build_dir, image_tag=self.tag)

    @classmethod
    def build(cls, dockerfile: DockerRenderer, build_dir: Path, image_tag: str):
        """Builds the dockerfile in the specified build directory

        Parameters
        ----------
        dockerfile : DockerRenderer
            Neurodocker renderer to build
        build_dir : Path
            path of the build directory
        image_tag : str
            Docker image tag to assign to the built image
        """

        # Save generated dockerfile to file
        out_file = build_dir / "Dockerfile"
        out_file.parent.mkdir(exist_ok=True, parents=True)
        with open(str(out_file), "w") as f:
            f.write(dockerfile.render())
        logger.info("Dockerfile for '%s' generated at %s", image_tag, str(out_file))

        dc = docker.from_env()
        try:
            dc.images.build(path=str(build_dir), tag=image_tag)
        except docker.errors.BuildError as e:
            build_log = "\n".join(ln.get("stream", "") for ln in e.build_log)
            raise RuntimeError(
                f"Building '{image_tag}' from '{str(build_dir)}/Dockerfile' "
                f"failed with the following errors:\n\n{build_log}"
            )
        logging.info("Successfully built docker image %s", image_tag)

    def construct_dockerfile(
        self,
        build_dir: Path,
        builtin_licenses: ty.Iterable[ty.Tuple[str, str or Path]] = (),
        use_local_packages: bool = False,
        pypi_fallback: bool = False,
        arcana_install_extras: ty.List[str] = (),
        readme: str = None,
        labels: ty.Dict[str, str] = None,
    ) -> DockerRenderer:
        """Constructs a dockerfile that wraps a with dependencies

        Parameters
        ----------
        build_dir : Path
            Path to the directory the Dockerfile will be written into copy any local
            files to
        labels : ty.Dict[str, str], optional
            labels to be added to the image
        arcana_install_extras : Iterable[str], optional
            Extras for the Arcana package that need to be installed into the
            dockerfile (e.g. tests)
        readme : str, optional
            Description of the container to put in a README
        use_local_packages: bool, optional
            Use the python package versions that are installed within the
            current environment, i.e. instead of pulling from PyPI. Useful during
            development and testing
        pypi_fallback : bool, optional
            whether to fallback to packages installed on PyPI when versions of
            local packages don't match installed
        builtin_licenses : list[tuple[str, str or Path]]
            licenses provided at build time to be installed inside the container image.
            A list of 'name' and 'source path' pairs.

        Returns
        -------
        DockerRenderer
            Neurodocker Docker renderer to construct dockerfile from
        """

        if not build_dir.is_dir():
            raise ArcanaBuildError(
                f"Build dir '{str(build_dir)}' is not a valid directory"
            )

        dockerfile = self.init_dockerfile(self.package_manager, self.base_image)

        self.install_system_packages(dockerfile)

        self.install_package_templates(dockerfile)

        self.install_python(
            dockerfile,
            build_dir,
            use_local_packages=use_local_packages,
            pypi_fallback=pypi_fallback,
        )

        # Arcana is installed separately from the other Python packages, partly so
        # the dependency Docker layer can be cached in dev and partly so it can be
        # treated differently if required in the future
        self.install_arcana(
            dockerfile,
            build_dir,
            install_extras=arcana_install_extras,
            use_local_package=use_local_packages,
        )

        self.install_licenses(
            dockerfile,
            # {k: v["destination"] for k, v in licenses.items()},
            builtin_licenses,
            build_dir,
        )

        self.write_readme(dockerfile, readme, build_dir)

        self.write_spec(dockerfile, build_dir)

        self.add_labels(dockerfile, labels)

        return dockerfile

    @classmethod
    def init_dockerfile(cls, package_manager, base_image):
        dockerfile = DockerRenderer(package_manager).from_(base_image)
        dockerfile.install(["git", "ssh-client", "vim"])
        return dockerfile

    def add_labels(self, dockerfile, labels):
        # dockerfile._parts.append(
        #     "LABEL "
        #     + " \\\n      ".join(f"{k}={json.dumps(v)}" for k, v in labels.items())
        # )
        dockerfile.labels({k: json.dumps(v) for k, v in labels.items()})

    def install_python(
        self,
        dockerfile: DockerRenderer,
        packages: ty.Iterable[PipSpec],
        build_dir: Path,
        use_local_packages: bool = False,
        pypi_fallback: bool = False,
    ):
        """Generate Neurodocker instructions to install an appropriate version of
        Python and the required Python packages

        Parameters
        ----------
        dockerfile : DockerRenderer
            the neurodocker renderer to append the install instructions to
        packages : ty.Iterable[PipSpec]
            the python packages (with optional extras) that need to be installed
        build_dir : Path
            the path to the build directory
        arcana_install_extras : Iterable[str]
            Optional extras (i.e. as defined in "extras_require" in setup.py) required
            for the arcana package
        use_local_packages: bool, optional
            Use the python package versions that are installed within the
            current environment, i.e. instead of defaulting to the release from PyPI.
            Useful during development and testing
        pypi_fallback : bool, optional
            Whether to fall back to PyPI version when local version doesn't match
            requested

        Returns
        -------
        list[list[str, list[str, str]]]
            neurodocker instructions to install python and required packages
        """

        # # Split out and merge any extras specifications (e.g. "arcana[test]")
        # between dependencies of the same package
        # Add arcana dependency
        packages = PipSpec.unique(packages, remove_arcana=True)

        dockerfile.add_registered_template(
            "miniconda",
            version="latest",
            env_name=self.CONDA_ENV,
            env_exists=False,
            conda_install=" ".join(
                ["python=" + natsorted(python_versions)[-1], "numpy", "traits"]
            ),
            pip_install=" ".join(
                self.pip_spec2str(
                    p, dockerfile, build_dir, use_local_packages, pypi_fallback
                )
                for p in packages
            ),
        )

    def install_system_packages(
        self, dockerfile: DockerRenderer, packages: ty.Iterable[str]
    ):
        """Generate Neurodocker instructions to install systems packages in dockerfile

        Parameters
        ----------
        dockerfile : DockerRenderer
            the neurodocker renderer to append the install instructions to
        system_packages : Iterable[str]
            the packages to install on the operating system
        """
        dockerfile.install(packages)

    def install_package_templates(
        self,
        dockerfile: DockerRenderer,
        package_templates: ty.Iterable[ty.Dict[str, str]],
    ):
        """Install custom packages from Neurodocker package_templates

        Parameters
        ----------
        dockerfile : DockerRenderer
            the neurodocker renderer to append the install instructions to
        package_templates : Iterable[dict[str, str]]
            Neurodocker installation package_templates to be installed inside the image. A
            dictionary containing the 'name' and 'version' of the template along
            with any additional keyword arguments required by the template
        """
        for kwds in package_templates:
            kwds = copy(
                kwds
            )  # so we can pop the name and leave the original dictionary intact
            dockerfile.add_registered_template(kwds.pop("name"), **kwds)

    def install_licenses(
        dockerfile: DockerRenderer,
        licenses_spec: ty.List[ty.Dict[str, str]],
        to_install: Path,
        build_dir: Path,
    ):
        """Generate Neurodocker instructions to install licenses within the container image

        Parameters
        ----------
        dockerfile : DockerRenderer
            the neurodocker renderer to append the install instructions to
        licenses_spec : dict[str, str]
            specification of the licenses required by the commands in the container. The
            keys of the dictionary are the license names and the values are the
            destination path of the license within the image.
        to_install : dict[str, str or Path]
            licenses provided at build time to be installed inside the container image.
            A list of 'name' and 'source path' pairs.
        build_dir : Path
            path to build dir
        """
        # Copy licenses into build directory
        license_build_dir = build_dir / "licenses"
        license_build_dir.mkdir()
        for name, src in to_install:
            build_path = license_build_dir / name
            shutil.copyfile(src, build_path)
            dockerfile.copy(
                source=[str(build_path.relative_to(build_dir))],
                destination=licenses_spec[name],
            )

    def write_spec(self, dockerfile: DockerRenderer, spec, build_dir):
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
        with open(build_dir / "arcana-spec.yaml", "w") as f:
            yaml.dump(spec, f)
        dockerfile.copy(source=["./arcana-spec.yaml"], destination=self.SPEC_PATH)

    @classmethod
    def install_arcana(
        cls,
        dockerfile: DockerRenderer,
        build_dir: Path,
        install_extras: ty.Iterable = (),
        use_local_package: bool = False,
    ):
        """Install the Arcana Python package into the Dockerfile

        Parameters
        ----------
        dockerfile : DockerRenderer
            the Neurdocker renderer
        build_dir : Path
            the directory the Docker image is built from
        install_extras : list[str]
            list of "install extras" (options) to specify when installing Arcana
            (e.g. 'test')
        use_local_package : bool
            Use local installation of arcana
        """
        pip_str = cls.pip_spec2str(
            PipSpec(PACKAGE_NAME, extras=install_extras),
            dockerfile,
            build_dir,
            use_local_packages=use_local_package,
            pypi_fallback=False,
        )
        dockerfile.run(
            f'bash -c "source activate {cls.CONDA_ENV} \\\n'
            f'&& python -m pip install --pre --no-cache-dir {pip_str}"'
        )

    @classmethod
    def write_readme(cls, dockerfile: DockerRenderer, description, build_dir):
        """Generate Neurodocker instructions to install README file inside the docker
        image

        Parameters
        ----------
        dockerfile : DockerRenderer
            the neurodocker renderer to append the install instructions to
        description : str
            a description of what the pipeline does, to be inserted in a README file
            in the Docker image
        build_dir : Path
            path to build dir
        """
        if description is None:
            description = ""
        else:
            description = "\n" + description + "\n"
        with open(build_dir / "README.md", "w") as f:
            f.write(cls.DOCKERFILE_README_TEMPLATE.format(__version__, description))
        dockerfile.copy(source=["./README.md"], destination="/README.md")

    @classmethod
    def pip_spec2str(
        cls,
        pip_spec: PipSpec,
        dockerfile: DockerRenderer,
        build_dir: Path,
        use_local_packages: bool,
        pypi_fallback: bool,
    ) -> str:
        """Generates a string to be passed to `pip` in order to install a package
        from a "pip specification" object

        Parameters
        ----------
        pip_spec : PipSpec
            specification of the package to install
        dockerfile : DockerRenderer
            Neurodocker Docker renderer object used to generate the Dockerfile
        build_dir : Path
            path to the directory the Docker image will be built in
        use_local_packages : bool
            whether to prefer local version of package (instead of PyPI version)

        Returns
        -------
        str
            string to be passed to `pip` installer
        """
        # Copy the local development versions of Python dependencies into the
        # docker image if present, instead of relying on the PyPI version,
        # which might be missing local changes and bugfixes (particularly in testing)
        if use_local_packages:
            pip_spec = local_package_location(pip_spec, pypi_fallback=pypi_fallback)
        if pip_spec.file_path:
            if pip_spec.version or pip_spec.url:
                raise ArcanaBuildError(
                    "Cannot specify a package by `file_path`, `version` and/or " "`url`"
                )
            pkg_build_path = cls.copy_sdist_into_build_dir(
                pip_spec.file_path, build_dir
            )
            pip_str = "/" + cls.PYTHON_PACKAGE_DIR + "/" + pkg_build_path.name
            dockerfile.copy(
                source=[str(pkg_build_path.relative_to(build_dir))], destination=pip_str
            )
        elif pip_spec.url:
            if pip_spec.version:
                raise ArcanaBuildError(
                    "Cannot specify a package by `url` and `version`"
                )
            pip_str = pip_spec.url
        else:
            pip_str = pip_spec.name
        if pip_spec.extras:
            pip_str += "[" + ",".join(pip_spec.extras) + "]"
        if pip_spec.version:
            pip_str += "==" + pip_spec.version
        return pip_str

    @classmethod
    def copy_sdist_into_build_dir(cls, local_installation: Path, build_dir: Path):
        """Create a source distribution from a locally installed "editable" python package
        and copy it into the build dir so it can be installed in the Docker image

        Parameters
        ----------
        package_name : str
            the name of the package (how it will be called in the docker image)
        local_installation : Path
            path to the local installation
        build_dir : Path
            path to the build directory

        Returns
        -------
        Path
            the path to the source distribution within the build directory
        """
        if not (local_installation / "setup.py").exists():
            raise ArcanaBuildError(
                "Can only copy local copy of Python packages that contain a 'setup.py' "
                f"not {local_installation}"
            )

        # Move existing 'dist' directory out of the way
        dist_dir = local_installation / "dist"
        if dist_dir.exists():
            moved_dist = local_installation / (
                "dist." + datetime.strftime(datetime.now(), "%Y%m%d%H%M%S")
            )
            shutil.move(local_installation / "dist", moved_dist)
        else:
            moved_dist = None
        try:
            # Generate source distribution using setuptools
            with set_cwd(local_installation):
                setuptools.sandbox.run_setup(
                    "setup.py", ["sdist", "--formats", "gztar"]
                )
            # Copy generated source distribution into build directory
            sdist_path = next((local_installation / "dist").iterdir())
            build_dir_pkg_path = build_dir / cls.PYTHON_PACKAGE_DIR / sdist_path.name
            build_dir_pkg_path.parent.mkdir(exist_ok=True)
            shutil.copy(sdist_path, build_dir_pkg_path)
        finally:
            # Put original 'dist' directory back in its place
            shutil.rmtree(local_installation / "dist", ignore_errors=True)
            if moved_dist:
                shutil.move(moved_dist, local_installation / "dist")

        return build_dir_pkg_path

    @classmethod
    def load(cls, yaml_path: Path, root_dir: Path = None, **kwargs):
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
                yaml_path.relative_to(root_dir).parent.parts + [yaml_path.stem]
            )
            dct["org"] = root_dir.name
        else:
            dct["name"] = yaml_path.stem
            dct["org"] = None

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

            out_dir = doc_dir.joinpath(self._relative_dir)

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
            tbl_info.write_row("Maintainer", self.maintainer)
            tbl_info.write_row("Info URL", self.info_url)

            f.write("\n")

            if self.licenses:
                f.write("### Required licenses\n")

                tbl_lic = MarkdownTable(f, "Source file", "Info")
                for lic in self.licenses:
                    tbl_lic.write_row(
                        escaped_md(lic.get("source", None)),
                        lic.get("info", ""),
                    )

                f.write("\n")

            f.write("## Commands\n")

            for cmd in self.commands:

                f.write(f"### {cmd['name']}\n")

                short_desc = cmd.get("long_description", None) or cmd.description
                f.write(f"{short_desc}\n\n")

                tbl_cmd = MarkdownTable(f, "Key", "Value")
                tbl_cmd.write_row("Short description", cmd.description)
                # if cmd.configuration is not None:
                #     config = cmd.configuration
                #     # configuration keys are variable depending on the workflow class
                tbl_cmd.write_row("Operates on", cmd.row_frequency.title())

                if cmd.known_issues is not None:
                    if cmd.known_issues.get("url"):
                        tbl_cmd.write_row("Known issues", cmd.known_issues["url"])
                    # Leaving room to extend known_issues further, e.g., an inplace list of issues

                f.write("#### Inputs\n")
                tbl_inputs = MarkdownTable(f, "Name", "Format", "Description")
                if cmd.inputs is not None:
                    for inpt in cmd.inputs:
                        tbl_inputs.write_row(
                            escaped_md(inpt.name),
                            self._format_html(inpt.stored_format),
                            inpt.description,
                        )
                    f.write("\n")

                f.write("#### Outputs\n")
                tbl_outputs = MarkdownTable(f, "Name", "Format", "Description")
                if cmd.outputs is not None:
                    for outpt in cmd.outputs:
                        tbl_outputs.write_row(
                            escaped_md(outpt.name),
                            self._format_html(outpt.stored_format),
                            outpt.get("description", ""),
                        )
                    f.write("\n")

                if cmd.parameters is not None:
                    f.write("#### Parameters\n")
                    tbl_params = MarkdownTable(f, "Name", "Data type", "Description")
                    for param in cmd.parameters:
                        tbl_params.write_row(
                            escaped_md(param.name),
                            escaped_md(param.type),
                            param.get("description", ""),
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
    def _format_html(cls, format):
        if not format:
            return ""
        if ":" not in format:
            return escaped_md(format)

        resolved = resolve_class(format, prefixes=["arcana.data.formats"])
        desc = getattr(resolved, "desc", resolved.__name__)

        if ext := getattr(resolved, "ext", None):
            text = f"{desc} (`.{ext}`)"
        elif getattr(resolved, "is_dir", None) and resolved is not Directory:
            text = f"{desc} (Directory)"
        else:
            text = desc

        return f'<span data-toggle="tooltip" data-placement="bottom" title="{format}" aria-label="{format}">{text}</span>'

    DOCKERFILE_README_TEMPLATE = """
        The following Docker image was generated by arcana v{} to enable the
        commands to be run in the XNAT container service. See
        https://raw.githubusercontent.com/Australian-Imaging-Service/arcana/main/LICENSE
        for licence.

        {}

        """
    PYTHON_PACKAGE_DIR = "python-packages"
    SPEC_PATH = "/arcana-spec.yaml"
    IN_DOCKER_ARCANA_HOME_DIR = "/arcana-home"
    CONDA_ENV = "arcana"
