import typing as ty
from pathlib import Path
import tempfile
from copy import copy
import logging
import shutil
import neurodocker as nd
from natsort import natsorted
import docker
from arcana import __version__
from arcana.__about__ import PACKAGE_NAME, python_versions
from arcana.exceptions import ArcanaBuildError
from .utils import PipSpec, local_package_location


logger = logging.getLogger('arcana')


def build_docker_image(image_tag: str,
                       build_dir: Path=None,
                       **kwargs):
    """Executes the full build workflow, from generating the Dockerfile to
    calling Docker to build it

    Parameters
    ----------
    image_tag : str
        _description_
    build_dir : Path, optional
        _description_, by default None
    """
    if build_dir is None:
        build_dir = tempfile.mkdtemp()
    build_dir = Path(build_dir)

    nd_specs = generate_neurodocker_specs(build_dir, **kwargs)

    render_dockerfile(nd_specs, build_dir)

    docker_build(build_dir, image_tag)


def generate_neurodocker_specs(
        build_dir: Path,
        base_image: str="debian:bullseye",
        python_packages: ty.Iterable[ty.Tuple[str, str]]=(),
        system_packages: ty.Iterable[ty.Iterable[ty.Tuple[str, str]]]=(),
        labels: ty.Dict[str, str]=None,
        package_manager: str='apt',
        arcana_install_extras: ty.Iterable[str]=(),
        readme: str=None,
        use_local_packages: bool=False):
    """Constructs a dockerfile that wraps a with dependencies

    Parameters
    ----------
    build_dir : Path
        Path to the directory the Dockerfile will be written into copy any local
        files to
    python_packages:  Iterable[tuple[str, str]], optional
        Name and version of the Python PyPI packages to add to the image (in
        addition to Arcana itself)
    base_image : str, optional
        The base image to build from
    system_packages: Iterable[str], optional
        Name and version of operating system packages (see Neurodocker) to add
        to the image
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

    Returns
    -------
    dict[str, Any]
        path to the build directory containing the Dockerfile and any supporting
        files to be copied in the image
    """
    if not build_dir.is_dir():
        raise ArcanaBuildError(f"Build dir '{str(build_dir)}' is not a valid directory")

    instructions = [
        ["base", base_image],
        ["install", ["git", "ssh-client", "vim"]]]  # git and ssh-client to install dev python packages, VIM for debugging

    instructions.extend(
        install_system_packages(system_packages))

    instructions.extend(
        install_python(python_packages, build_dir, arcana_install_extras,
                       use_local_packages=use_local_packages))

    if readme:
        instructions.append(insert_readme(readme, build_dir))

    if labels:
        instructions.append(["label", labels])

    neurodocker_specs = {
        "pkg_manager": package_manager,
        "instructions": instructions}

    return neurodocker_specs


def render_dockerfile(neurodocker_specs, build_dir):
    """Renders a Docker image from Neurodocker specs

    Parameters
    ----------
    neurodocker_specs : dict
        specifications for NeuroDocker build
    build_dir : Path
        path to build directory that the specs were created for (i.e. needs to
        be the one provided to `generate_neurodocker_specs`)
    """

    dockerfile = nd.Dockerfile(neurodocker_specs).render()

    # Save generated dockerfile to file
    out_file = build_dir / 'Dockerfile'
    out_file.parent.mkdir(exist_ok=True, parents=True)
    with open(str(out_file), 'w') as f:
        f.write(dockerfile)
    logger.info("Dockerfile generated at %s", out_file)


def docker_build(build_dir: Path, image_tag: str):
    """Builds the dockerfile in the specified build directory

    Parameters
    ----------
    build_dir : Path
        path of the build directory
    image_tag : str
        Docker image tag to assign to the built image
    """
    dc = docker.from_env()
    dc.images.build(path=str(build_dir), tag=image_tag)
    logging.info("Successfully built docker image %s", image_tag)


def install_python(packages: ty.Iterable[PipSpec], build_dir: Path,
                   arcana_install_extras: ty.Iterable=(),
                   use_local_packages: bool=False):
    """Generate Neurodocker instructions to install an appropriate version of
    Python and the required Python packages

    Parameters
    ----------
    python_packages : ty.Iterable[PipSpec]
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

    Returns
    -------
    list[list[str, list[str, str]]]
        neurodocker instructions to install python and required packages
    """

    # # Split out and merge any extras specifications (e.g. "arcana[test]")
    # between dependencies of the same package
    # Add arcana dependency
    
    packages = PipSpec.unique(
        packages + [PipSpec(name=PACKAGE_NAME, version=__version__,
                            extras=arcana_install_extras)])

    # Copy the local development versions of Python dependencies into the
    # docker image if present, instead of relying on the PyPI version,
    # which might be missing local changes and bugfixes (particularly in testing)
    pip_strs = []  # list of packages to install using pip
    instructions = []
    for pip_spec in packages:
        if use_local_packages:
            pip_spec = local_package_location(pip_spec)
        if pip_spec.file_path:
            if pip_spec.version or pip_spec.url:
                raise ArcanaBuildError(
                    "Cannot specify a package by `file_path`, `version` and/or "
                    "`url`")
            pkg_build_path = copy_package_into_build_dir(
                pip_spec.name, pip_spec.file_path, build_dir)
            pip_str = '/python-packages/' + pip_spec.name
            instructions.append(
                ['copy', [str(pkg_build_path.relative_to(build_dir)), pip_str]])
        elif pip_spec.url:
            if pip_spec.version:
                raise ArcanaBuildError(
                    "Cannot specify a package by `url` and `version`")
            pip_str = pip_spec.url
        else:
            pip_str = pip_spec.name
        if pip_spec.extras:
            pip_str += '[' + ','.join(pip_spec.extras) + ']'
        if pip_spec.version:
            pip_str += '==' + pip_spec.version
        pip_strs.append(pip_str)

    instructions.append(
        ["miniconda", {
            "create_env": "arcana",
            "install_python": [
                "python=" + natsorted(python_versions)[-1],
                "numpy",
                "traits",
                "dcm2niix",
                "mrtrix3"],
            "conda_opts": "--channel mrtrix3",
            "pip_install": pip_strs}])  

    return instructions


def install_system_packages(packages: ty.Iterable[str]):
    """Generate Neurodocker instructions to install systems packages in dockerfile

    Parameters
    ----------
    system_packages : Iterable[str]
        the packages to install on the operating system

    Returns
    -------
    list[str, list[str]]
        Neurodocker instructions to install the system packages
    """
    instructions = []
    for pkg in packages:
        install_properties = {}
        if isinstance(pkg, str):
            pkg_name = pkg
            install_properties['version'] = 'master'
        else:
            pkg_name = pkg[0]
            if len(pkg) > 1 and pkg[1] != '.':
                install_properties['version'] = pkg[1]
            if len(pkg) > 2:
                install_properties['method'] = pkg[2]   
        instructions.append([pkg_name, install_properties])
    return instructions


def insert_readme(description, build_dir):
    """Generate Neurodocker instructions to install README file inside the docker
    image

    Parameters
    ----------
    description : str
        a description of what the pipeline does, to be inserted in a README file
        in the Docker image
    build_dir : Path
        path to build dir

    Returns
    -------
    list[str, list[str]]
        Neurodocker instructions to install the system packages
    """
    if description is None:
        description = ''
    else:
        description = '\n' + description + '\n'
    with open(build_dir / 'README.md', 'w') as f:
        f.write(DOCKERFILE_README_TEMPLATE.format(
            __version__, description))
    return ['copy', ['./README.md', '/README.md']]


def copy_package_into_build_dir(package_name: str, local_installation: Path,
                                build_dir: Path):
    """Copies a local installation of a package into the build directory

    Parameters
    ----------
    package_name : str
        the name of the package (how it will be called in the docker image)
    local_installation : Path
        path to the local installation
    build_dir : Path
        path to the build directory
    """
    pkg_build_path = build_dir / 'python-packages' / package_name
    if pkg_build_path.exists():
        shutil.rmtree(pkg_build_path)
    # Copy source tree into build dir minus any cache files and paths
    # included in the gitignore
    patterns_to_ignore = list(PATTERNS_TO_NOT_COPY_INTO_BUILD)
    ignore_paths = [Path(p) for p in PATHS_TO_NOT_COPY_INTO_BUILD]
    if (local_installation / '.gitignore').exists():
        with open(local_installation / '.gitignore') as f:
            gitignore = f.read().splitlines()
        patterns_to_ignore.extend(
            p for p in gitignore if not p.startswith('/'))
        ignore_paths.extend(
            Path(p[1:]) for p in gitignore if p.startswith('/'))
    ignore_patterns = shutil.ignore_patterns(*patterns_to_ignore)
    def ignore_paths_and_patterns(directory, contents):
        to_ignore = ignore_patterns(directory, contents)
        to_ignore.update(
            c for c in contents if Path(directory) / c in ignore_paths)
        return to_ignore
    shutil.copytree(local_installation, pkg_build_path,
                    ignore=ignore_paths_and_patterns)
    return pkg_build_path


DOCKERFILE_README_TEMPLATE = """
    The following Docker image was generated by arcana v{} to enable the
    commands to be run in the XNAT container service. See
    https://raw.githubusercontent.com/Australian-Imaging-Service/arcana/main/LICENSE
    for licence.

    {}
    
    """

PATHS_TO_NOT_COPY_INTO_BUILD = ('conftest.py', 'debug-build')
PATTERNS_TO_NOT_COPY_INTO_BUILD = ('*.pyc', '__pycache__', '.pytest_cache')
