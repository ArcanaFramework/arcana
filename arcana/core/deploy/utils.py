
import typing as ty
from pathlib import Path
import json
import site
import pkg_resources
from arcana import __version__
from arcana.exceptions import (ArcanaBuildError)
import os
import yaml
from dataclasses import dataclass, field as dataclass_field
from arcana import __version__


@dataclass
class PipSpec():
    """Specification of a Python package"""

    version: str = None
    url: str = None
    file_path: str = None
    extras: ty.List[str] = dataclass_field(default_factory=list)


def load_yaml_spec(path, base_dir=None):
    def concat(loader, node):
        seq = loader.construct_sequence(node)
        return ''.join([str(i) for i in seq])

    def slice(loader, node):
        list, start, end = loader.construct_sequence(node)
        return list[start:end]

    def sliceeach(loader, node):
        _, start, end = loader.construct_sequence(node)
        return [
            loader.construct_sequence(x)[start:end] for x in node.value[0].value
        ]

    yaml.SafeLoader.add_constructor(tag='!join', constructor=concat)
    yaml.SafeLoader.add_constructor(tag='!concat', constructor=concat)
    yaml.SafeLoader.add_constructor(tag='!slice', constructor=slice)
    yaml.SafeLoader.add_constructor(tag='!sliceeach', constructor=sliceeach)

    with open(path, 'r') as f:
        data = yaml.load(f, Loader=yaml.SafeLoader)

    # frequency = data.get('frequency', None)
    # if frequency:
    #     # TODO: Handle other frequency types, are there any?
    #     data['frequency'] = Clinical[frequency.split('.')[-1]]

    data['_relative_dir'] = os.path.dirname(os.path.relpath(path, base_dir)) if base_dir else ''
    data['_module_name'] = os.path.basename(path).rsplit('.', maxsplit=1)[0]

    return data


def walk_spec_paths(spec_path: Path) -> ty.Iterable[Path]:
    """Walk a directory structure and return all YAML specs found with it

    Parameters
    ----------
    spec_path : Path
        path to the directory
    """
    if spec_path.is_file():
        yield spec_path
    else:
        for path in spec_path.rglob('*.yml'):
            yield path


def installed_package_locations(
        packages: ty.Iterable[str or pkg_resources.DistInfoDistribution]):
    """Detect the installed locations of the packages, including development
    versions.

    Parameters
    ----------
    packages: Iterable[str or pkg_resources.DistInfoDistribution]
        the packages (or names of) the versions to detect

    Returns
    -------
    dict[str, PipSpec]
        the pip specifications corresponding to the 
    """
    site_pkg_locs = [Path(p).resolve() for p in site.getsitepackages()]
    pip_specs = {}
    for pkg in packages:
        if isinstance(pkg, str):
            parts = pkg.split('==')
            pkg_name = parts[0]
            pkg_version = parts[1] if len(parts) == 2 else None
            try:
                pkg = next(p for p in pkg_resources.working_set
                           if p.project_name == pkg_name)
            except StopIteration:
                raise ArcanaBuildError(
                    f"Did not find {pkg_name} in installed working set:\n"
                    + "\n".join(sorted(
                        p.key + '/' + p.project_name
                        for p in pkg_resources.working_set)))
            if pkg_version and pkg.version != pkg_version:
                raise ArcanaBuildError(
                    f"Requested package {pkg_version} does not match installed "
                    f"{pkg.version}")
        pkg_loc = Path(pkg.location).resolve()
        # Determine whether installed version of requirement is locally
        # installed (and therefore needs to be copied into image) or can
        # be just downloaded from PyPI
        if pkg_loc not in site_pkg_locs:
            # Copy package into Docker image and instruct pip to install from
            # that copy
            pip_spec = PipSpec(file_path='/python-packages/' + pkg.key)
        else:
            # Check to see whether package is installed via "direct URL" instead
            # of through PyPI
            direct_url_path = Path(pkg.egg_info) / 'direct_url.json'
            if direct_url_path.exists():
                with open(direct_url_path) as f:
                    url_spec = json.load(f)
                url = url_spec['url']
                if 'vcs' in url_spec:
                    url = url_spec['vcs'] + '+' + url
                if 'commit_id' in url_spec:
                    url += '@' + url_spec['commit_id']
                pip_spec = PipSpec(url=url)
            else:
                pip_spec = PipSpec(version=pkg.version)
        pip_specs[pkg.key] = pip_spec
    return pip_specs