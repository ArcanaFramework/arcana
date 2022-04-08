
import typing as ty
from pathlib import Path
import tempfile
from copy import copy
import re
import json
from collections import defaultdict
import logging
import site
import shutil
import tempfile
import pkg_resources
import neurodocker as nd
from natsort import natsorted
import docker
from arcana import __version__
from arcana.__about__ import install_requires, PACKAGE_NAME, python_versions
from arcana.core.utils import pkg_from_module
from arcana.exceptions import (ArcanaBuildError)


logger = logging.getLogger('arcana')


def generate_neurodocker_specs(
        build_dir: Path,
        python_packages: ty.Iterable[ty.Tuple[str, str]]=(),
        base_image: str="debian:bullseye",
        sys_packages: ty.Iterable[ty.Iterable[ty.Tuple[str, str]]]=(),
        labels: ty.Dict[str, str]=None,
        package_manager: str='apt',
        arcana_extras: ty.Iterable[str]=(),
        description: str=None):
    """Constructs a dockerfile that wraps a with dependencies

    Parameters
    ----------
    build_dir: Path
        Path to the directory the Dockerfile will be written into copy any local
        files to
    python_packages: Iterable[tuple[str, str]], optional
        Name and version of the Python PyPI packages to add to the image
    base_image: str, optional
        The base image to build from
    sys_packages: Iterable[str], optional
        Name and version of the system packages (see Neurodocker) to add to the image
    labels : ty.Dict[str, str], optional
        labels to be added to the image
    arcana_extras, Iterable[str], optional
        Extras for the Arcana package that need to be installed into the
        dockerfile (e.g. tests)
    description: str, optional
        Description of the container to put in a README

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
        ["install", ["git", "vim", "ssh-client"]]]

    instructions.extend(install_system_packages(sys_packages))

    instructions.extend(
        install_python(python_packages, build_dir, arcana_extras))

    if description:
        instructions.append(insert_readme(description, build_dir))

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
    


def build_docker_image(build_dir: Path, image_tag: str):
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


def xnat_command_ref_copy_cmd(xnat_commands, build_dir):
    """_summary_

    Parameters
    ----------
    xnat_commands : _type_
        _description_
    build_dir : _type_
        _description_

    Returns
    -------
    _type_
        _description_
    """
    # Copy command JSON inside dockerfile for ease of reference
    cmds_dir = build_dir / 'xnat_commands'
    cmds_dir.mkdir()
    for cmd in xnat_commands:
        fname = cmd.get('name', 'command') + '.json'
        with open(build_dir / fname, 'w') as f:
            json.dump(cmd, f, indent='    ')
    return ['copy', ['./xnat_commands', '/xnat_commands']]


def install_python(python_packages, build_dir, arcana_extras=()):
    """Generate Neurodocker instructions to install an appropriate version of
    Python and the required Python packages

    Parameters
    ----------
    python_packages : Iterable[str]
        the python packages (with optional extras) that need to be installed
    arcana_extras : Iterable[str]
        Optional extras (i.e. as defined in "extras_require" in setup.py) required
        for the arcana package
    build_dir : Path
        the path to the build directory

    Returns
    -------
    _type_
        _description_

    Raises
    ------
    ArcanaBuildError
        _description_
    """

    # # Split out and merge any extras specifications (e.g. "arcana[test]")
    # between dependencies of the same package
    pkg_extras = defaultdict(list)
    # Add arcana dependency
    pkg_extras[PACKAGE_NAME].extend(arcana_extras)
    for pkg_spec in python_packages:
        pkg_name, extra_spec = re.match(r'([^\[]+)(\[[^\]]+\])?', pkg_spec).groups()
        if extra_spec is None:
            extras = []
        else:
            extras = [e.strip() for e in extra_spec[1:-1].split(',')]
        pkg_extras[pkg_name].extend(extras)

    # Copy the local development versions of Python dependencies into the
    # docker image if present, instead of relying on the PyPI version,
    # which might be missing local changes and bugfixes (particularly in testing)
    site_pkg_locs = [Path(p).resolve() for p in site.getsitepackages()]
    resolved_packages = []
    instructions = []
    for pkg_name, extras in pkg_extras.items():
        try:
            pkg = next(p for p in pkg_resources.working_set
                        if p.project_name == pkg_name)
        except StopIteration:
            raise ArcanaBuildError(
                f"Did not find {pkg_name} in installed working set:\n"
                + "\n".join(sorted(
                    p.key + '/' + p.project_name
                    for p in pkg_resources.working_set)))
        pkg_loc = Path(pkg.location).resolve()
        # Determine whether installed version of requirement is locally
        # installed (and therefore needs to be copied into image) or can
        # be just downloaded from PyPI
        pkg_version_str = ''
        if pkg_loc not in site_pkg_locs:
            # Copy package into Docker image and instruct pip to install from
            # that copy
            copy_package_into_build_dir(pkg_name, pkg_loc, build_dir)
            pip_address = '/python-packages/' + pkg_name
            instructions.append(['copy', ['./' + pkg_name, pip_address]])
        else:
            # Check to see whether package is installed via "direct URL" instead
            # of through PyPI
            direct_url_path = Path(pkg.egg_info) / 'direct_url.json'
            if direct_url_path.exists():
                with open(direct_url_path) as f:
                    url_spec = json.load(f)
                pip_address = url_spec['url']
                if 'vcs' in url_spec:
                    pip_address = url_spec['vcs'] + '+' + pip_address
                if 'commit_id' in url_spec:
                    pip_address += '@' + url_spec['commit_id']
            else:
                pip_address = pkg.key
                pkg_version_str = f"=={pkg.version}"
        if extras:
            pip_address += '[' + ','.join(extras) + ']'
        pip_address += pkg_version_str
        resolved_packages.append(pip_address)

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
            "pip_install": resolved_packages}])  

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
    description : _type_
        _description_
    build_dir : _type_
        _description_

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


def create_wrapper_image(pkg_name: str,
                         commands: ty.List[ty.Dict[str, ty.Any]],
                         pkg_version: str,
                         authors: ty.List[ty.Tuple[str, str]],
                         info_url: str,
                         docker_org: str,
                         docker_registry: str,
                         wrapper_version: str=None,
                         **kwargs):
    """Creates a Docker image containing one or more XNAT commands ready
    to be installed.

    Parameters
    ----------
    pkg_name
        Name of the package as a whole
    commands
        List of command specifications (in dicts) to be installed on the
        image, see `generate_xnat_command` for valid args (dictionary keys).
    pkg_version
        Version of the package the commands are drawn from (could be 3.0.3
        for MRtrix3 for example)
    authors
        Names and emails of the maintainers of the wrapper pipeline
    info_url
        The URL of the package website explaining the analysis software
        and what it does
    docker_org
        The docker organisation the image will uploaded to
    docker_registry
        The Docker registry the image will be uploaded to
    wrapper_version
        The version of the wrapper specific to the pkg version. It will be
        appended to the package version, e.g. 0.16.2 -> 0.16.2--1
    **kwargs:
        Passed on to `generate_dockerfile` method
    """

    full_version = str(pkg_version)
    if wrapper_version is not None:
        full_version += f"-{wrapper_version}"
    image_tag = f"{docker_org}/{pkg_name.lower().replace('-', '_')}:{full_version}"

    xnat_commands = []
    python_packages = kwargs.pop('python_packages', [])
    for cmd_spec in commands:

        cmd_name = cmd_spec.pop('name', pkg_name)

        xnat_cmd = XnatViaCS.generate_xnat_command(
            pipeline_name=cmd_name,
            info_url=info_url,
            image_tag=image_tag,
            version=full_version,
            registry=docker_registry,
            **cmd_spec)

        cmd_pkg = pkg_from_module([cmd_spec['pydra_task'].split(':')[0]])
        if cmd_pkg.key not in [p.split('[')[0] for p in python_packages]:
            python_packages.append(cmd_pkg.key)

        xnat_commands.append(xnat_cmd)

    build_dir = XnatViaCS.generate_dockerfile(
        labels={'org.nrg.commands': json.dumps(xnat_commands),
                'maintainer': maintainer},
        maintainer=authors[0][1],
        python_packages=python_packages,
        **kwargs)


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
    pkg_build_path = build_dir / package_name
    if pkg_build_path.exists():
        shutil.rmtree(build_dir / package_name)
    # Copy source tree into build dir minus any cache files and paths
    # included in the gitignore
    patterns_to_ignore = copy(PATTERNS_TO_NOT_COPY_INTO_BUILD)
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


DOCKERFILE_README_TEMPLATE = """
    The following Docker image was generated by arcana v{} to enable the
    {} commands to be run in the XNAT container service.
    {}
    
    """

PATHS_TO_NOT_COPY_INTO_BUILD = ('conftest.py', 'debug-build')
PATTERNS_TO_NOT_COPY_INTO_BUILD = ('*.pyc', '__pycache__', '.pytest_cache')    