from __future__ import annotations
import typing as ty

# import hashlib
from pathlib import PurePath, Path
import json
import tempfile
import logging
from copy import copy
import shutil
from inspect import isclass, isfunction
from build import ProjectBuilder
import attrs
import docker
from neurodocker.reproenv import DockerRenderer
from arcana.core import __version__
from arcana.core import PACKAGE_NAME
from arcana.core.utils.serialize import (
    ClassResolver,
    ObjectConverter,
)
from arcana.core.utils.misc import (
    DOCKER_HUB,
)
from arcana.core.data.space import DataSpace
from arcana.core.exceptions import ArcanaBuildError
from .components import Packages, BaseImage, PipPackage, CondaPackage

logger = logging.getLogger("arcana")


@attrs.define(kw_only=True)
class ArcanaImage:
    """
    Base class with Arcana installed within it from which all container image
    specifications can inherit from

    name : str
        name of the package/pipeline
    version : str
        version of the package/pipeline
    build_iteration : str
        will be appended to the tag of the built images. Used to specify that the build
        specification has been updated but the underlying software is the same
    org : str
        the organisation the image will be tagged within
    base_image : arcana.core.deploy.image.components.BaseImage, optional
        the base image to build from
    packages : arcana.core.deploy.image.components.Packages
        System (OS), PyPI, Conda and Neurodocker packages/templates to be installed
        in the image
    package_templates : Iterable[dict[str, str]]
        Neurodocker package installation templates to be installed inside the image. A
        dictionary containing the 'name' and 'version' of the template along
        with any additional keyword arguments required by the template
    registry : str, optional
        the container registry the image is to be installed at
    readme : str, optional
        text to include in the image README file
    labels : dict[str, str]
    """

    name: str
    version: str = attrs.field(converter=str)
    build_iteration: str = None
    base_image: BaseImage = attrs.field(
        default=BaseImage(), converter=ObjectConverter(BaseImage)
    )
    packages: Packages = attrs.field(
        default=Packages(), converter=ObjectConverter(Packages)
    )
    org: str = None
    registry: str = DOCKER_HUB
    readme: str = None
    labels: dict[str, str] = None

    @property
    def reference(self):
        return f"{self.path}:{self.tag}"

    @property
    def tag(self):
        return (
            f"{self.version}-{self.build_iteration}"
            if self.build_iteration
            else self.version
        )

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
        if build_dir.exists():
            shutil.rmtree(build_dir)
        build_dir.mkdir()

        dockerfile = self.construct_dockerfile(build_dir, **kwargs)

        if not generate_only:
            self.build(dockerfile, build_dir, image_tag=self.reference)

    def construct_dockerfile(
        self,
        build_dir: Path,
        use_local_packages: bool = False,
        pypi_fallback: bool = False,
        arcana_install_extras: ty.List[str] = (),
        **kwargs,
    ) -> DockerRenderer:
        """Constructs a dockerfile that wraps a with dependencies

        Parameters
        ----------
        build_dir : Path
            Path to the directory the Dockerfile will be written into copy any local
            files to
        use_local_packages: bool, optional
            Use the python package versions that are installed within the
            current environment, i.e. instead of pulling from PyPI. Useful during
            development and testing
        pypi_fallback : bool, optional
            whether to fallback to packages installed on PyPI when versions of
            local packages don't match installed
        arcana_install_extras : Iterable[str], optional
            Extras for the Arcana package that need to be installed into the
            dockerfile (e.g. tests)
        readme : str, optional
            Description of the container to put in a README
        labels : ty.Dict[str, str], optional
            labels to be added to the image

        Returns
        -------
        DockerRenderer
            Neurodocker Docker renderer to construct dockerfile from
        """

        if not build_dir.is_dir():
            raise ArcanaBuildError(
                f"Build dir '{str(build_dir)}' is not a valid directory"
            )

        dockerfile = self.init_dockerfile()

        self.install_system_packages(dockerfile)

        self.install_package_templates(dockerfile)

        self.install_python(
            dockerfile,
            build_dir,
            use_local_packages=use_local_packages,
            pypi_fallback=pypi_fallback,
            arcana_install_extras=arcana_install_extras,
        )

        self.write_readme(dockerfile, build_dir)

        self.add_labels(dockerfile)

        return dockerfile

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

    def init_dockerfile(self):
        dockerfile = DockerRenderer(self.base_image.package_manager).from_(
            self.base_image.reference
        )
        return dockerfile

    def add_labels(self, dockerfile, labels=None):
        if labels is None:
            labels = self.labels
        if labels:
            dockerfile.labels({k: json.dumps(v).strip('"') for k, v in labels.items()})

    def install_python(
        self,
        dockerfile: DockerRenderer,
        build_dir: Path,
        use_local_packages: bool = False,
        arcana_install_extras: ty.Iterable = (),
        pypi_fallback: bool = False,
    ):
        """Generate Neurodocker instructions to install an appropriate version of
        Python and the required Python packages

        Parameters
        ----------
        dockerfile : DockerRenderer
            the neurodocker renderer to append the install instructions to
        build_dir : Path
            the path to the build directory
        arcana_install_extras : Iterable[str]
            Optional extras (i.e. as defined in "extras_require" in setup.py) required
            for the arcana package
        use_local_packages: bool, optional
            Use the python package versions that are installed within the
            current environment, i.e. instead of defaulting to the release from PyPI.
            Useful during development and testing
        arcana_install_extras : list[str]
            list of "install extras" (options) to specify when installing Arcana
            (e.g. 'test')
        pypi_fallback : bool, optional
            Whether to fall back to PyPI version when local version doesn't match
            requested

        Returns
        -------
        list[list[str, list[str, str]]]
            neurodocker instructions to install python and required packages
        """

        pip_specs = PipPackage.unique(
            self.packages.pip + [PipPackage(PACKAGE_NAME, extras=arcana_install_extras)]
        )

        pip_strs = []
        for pip_spec in pip_specs:
            if use_local_packages:
                pip_spec = pip_spec.local_package_location(pypi_fallback=pypi_fallback)
            pip_strs.append(self.pip_spec2str(pip_spec, dockerfile, build_dir))

        conda_pkg_names = set(p.name for p in self.packages.conda)
        conda_strs = []
        if "python" not in conda_pkg_names:
            conda_strs.append("python")
        for pkg_name in CondaPackage.REQUIRED:
            if pkg_name not in conda_pkg_names:
                conda_strs.append(pkg_name)

        conda_strs.extend(
            f"{p.name}={p.version}" if p.version is not None else p.name
            for p in self.packages.conda
        )

        dockerfile.add_registered_template(
            "miniconda",
            version="latest",
            env_name=self.CONDA_ENV,
            env_exists=False,
            conda_install=" ".join(conda_strs),
            pip_install=" ".join(pip_strs),
        )

    def install_system_packages(self, dockerfile: DockerRenderer):
        """Generate Neurodocker instructions to install systems packages in dockerfile

        Parameters
        ----------
        dockerfile : DockerRenderer
            the neurodocker renderer to append the install instructions to
        system_packages : Iterable[str]
            the packages to install on the operating system
        """
        pkg_strs = [
            f"{p.name}={p.version}" if p.version else p.name
            for p in self.packages.system
        ]
        dockerfile.install(pkg_strs)

    def install_package_templates(
        self,
        dockerfile: DockerRenderer,
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
        for template in self.packages.neurodocker:
            kwds = attrs.asdict(template)
            kwds = copy(
                kwds
            )  # so we can pop the name and leave the original dictionary intact
            dockerfile.add_registered_template(kwds.pop("name"), **kwds)

    @classmethod
    def pip_spec2str(
        cls,
        pip_spec: PipPackage,
        dockerfile: DockerRenderer,
        build_dir: Path,
    ) -> str:
        """Generates a string to be passed to `pip` in order to install a package
        from a "pip specification" object

        Parameters
        ----------
        pip_spec : PipPackage
            specification of the package to install
        dockerfile : DockerRenderer
            Neurodocker Docker renderer object used to generate the Dockerfile
        build_dir : Path
            path to the directory the Docker image will be built in

        Returns
        -------
        str
            string to be passed to `pip` installer
        """
        # Copy the local development versions of Python dependencies into the
        # docker image if present, instead of relying on the PyPI version,
        # which might be missing local changes and bugfixes (particularly in testing)
        # if use_local_packages:
        #     pip_spec = pip_spec.local_package_location(pypi_fallback=pypi_fallback)
        if pip_spec.file_path:
            if pip_spec.version or pip_spec.url:
                raise ArcanaBuildError(
                    "Cannot specify a package by `file_path`, `version` and/or " "`url`"
                )
            # pkg_build_path = cls.copy_sdist_into_build_dir(
            #     pip_spec.file_path, build_dir
            # )
            # Create a source distribution tarball to be installed within the docker
            # image
            sdist_dir = build_dir / cls.PYTHON_PACKAGE_DIR
            builder = ProjectBuilder(pip_spec.file_path)
            pkg_build_path = Path(builder.build("sdist", sdist_dir))
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

    # @classmethod
    # def copy_sdist_into_build_dir(cls, local_installation: Path, build_dir: Path):
    #     """Create a source distribution from a locally installed "editable" python package
    #     and copy it into the build dir so it can be installed in the Docker image

    #     Parameters
    #     ----------
    #     package_name : str
    #         the name of the package (how it will be called in the docker image)
    #     local_installation : Path
    #         path to the local installation
    #     build_dir : Path
    #         path to the build directory

    #     Returns
    #     -------
    #     Path
    #         the path to the source distribution within the build directory
    #     """
    #     sdist_dir = build_dir / cls.PYTHON_PACKAGE_DIR
    #     built = build_package(local_installation, sdist_dir, ["sdist"])
    #     return sdist_dir / built[0]

    def write_readme(self, dockerfile: DockerRenderer, build_dir):
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
        with open(build_dir / "README.md", "w") as f:
            f.write(self.DOCKERFILE_README_TEMPLATE.format(__version__, self.readme))
        dockerfile.copy(source=["./README.md"], destination="/README.md")

    def asdict(self):
        """Return a serialized version of the pipeline image specification that can be
        written to file"""

        def filter(attr, value):
            return not isinstance(value, type(self)) and attr.metadata.get(
                "asdict", True
            )

        def serializer(_, attr, value):
            if attr is not None and "serializer" in attr.metadata:
                value = attr.metadata["serializer"](
                    value,
                    value_serializer=serializer,
                    filter=filter,
                )
            elif isinstance(value, DataSpace):
                if hasattr(self, "command") and self.command.DATA_SPACE:
                    value = str(value)
                else:
                    value = value.tostr()
            elif isinstance(value, PurePath):
                # TODO: need better handling of saving checksums
                # if value.exists():
                #     fhash = hashlib.md5()
                #     with open(value, "rb") as f:
                #         # Calculate hash in chunks so we don't run out of memory for
                #         # large files.
                #         for chunk in iter(lambda: f.read(HASH_CHUNK_SIZE), b""):
                #             fhash.update(chunk)
                #     value = "checksum:" + fhash.hexdigest()
                # else:
                value = str(value)
            elif isclass(value) or isfunction(value):
                value = ClassResolver.tostr(value, strip_prefix=False)
            return value

        return attrs.asdict(self, value_serializer=serializer, filter=filter)

    DOCKERFILE_README_TEMPLATE = """
        The following Docker image was generated by Arcana v{} (https://arcana.readthedocs.io)

        {}

        """

    PYTHON_PACKAGE_DIR = "python-packages"
    CONDA_ENV = "arcana"
