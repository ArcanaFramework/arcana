import setuptools.sandbox
import typing as ty
from pathlib import Path
import json
import tempfile
import logging
from datetime import datetime
from copy import copy
import shutil
from natsort import natsorted
import attrs
import docker
from neurodocker.reproenv import DockerRenderer
from arcana.core.utils import set_cwd, ListDictConverter, DOCKER_HUB
from arcana.__about__ import python_versions
from arcana.exceptions import ArcanaBuildError
from .components import PipSpec, SystemPackage, NeurodockerPackage

logger = logging.getLogger("arcana")


@attrs.define(kw_only=True)
class BaseContainerImage:
    """
    Base class from which all container image specifications inherit from

    name : str
        name of the package/pipeline
    version : str
        version of the package/pipeline
    org : str
        the organisation the image will be tagged within
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
    registry : str, optional
        the container registry the image is to be installed at
    """

    DEFAULT_BASE_IMAGE = "ubuntu:kinetic"
    DEFAULT_PACKAGE_MANAGER = "apt"

    name: str
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
    registry: str = DOCKER_HUB

    @property
    def tag(self):
        return f"{self.path}:{self.full_version}"

    @property
    def full_version(self):
        return self.version

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

    def init_dockerfile(self):
        dockerfile = DockerRenderer(self.package_manager).from_(self.base_image)
        dockerfile.install(["git", "ssh-client", "vim"])
        return dockerfile

    def add_labels(self, dockerfile, labels):
        if labels is not None:
            dockerfile.labels({k: json.dumps(v).strip('"') for k, v in labels.items()})

    def install_python(
        self,
        dockerfile: DockerRenderer,
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
        packages = PipSpec.unique(self.python_packages, remove_arcana=True)

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

    def install_system_packages(self, dockerfile: DockerRenderer):
        """Generate Neurodocker instructions to install systems packages in dockerfile

        Parameters
        ----------
        dockerfile : DockerRenderer
            the neurodocker renderer to append the install instructions to
        system_packages : Iterable[str]
            the packages to install on the operating system
        """
        dockerfile.install(self.system_packages)

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
        for kwds in self.package_templates:
            kwds = copy(
                kwds
            )  # so we can pop the name and leave the original dictionary intact
            dockerfile.add_registered_template(kwds.pop("name"), **kwds)

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
            pip_spec = pip_spec.local_package_location(pypi_fallback=pypi_fallback)
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

    PYTHON_PACKAGE_DIR = "python-packages"
    CONDA_ENV = "arcana"
