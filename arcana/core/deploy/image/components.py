import typing as ty
from pathlib import Path
import json
import pkg_resources
import logging
from urllib.parse import urlparse
import site
import attrs
from arcana.__about__ import PACKAGE_NAME
from arcana.core.exceptions import ArcanaBuildError
from arcana.core.utils import ObjectListConverter, named_objects2dict

logger = logging.getLogger("arcana")


@attrs.define
class ContainerAuthor:

    name: str
    email: str


@attrs.define
class KnownIssue:

    url: str


@attrs.define(kw_only=True)
class BaseImage:

    name: str = "ubuntu"
    tag: str = "kinetic"  # FIXME: should revert back to jammy after tests pass
    package_manager: str = attrs.field()

    @property
    def reference(self):
        return f"{self.name}:{self.tag}"

    @package_manager.default
    def package_manager_default(self):
        if self.name in ("fedora", "centos"):
            package_manager = "yum"
        else:
            package_manager = "apt"
        return package_manager

    @package_manager.validator
    def package_manager_validator(self, _, package_manager):
        if package_manager not in ("yum", "apt"):
            raise ValueError(
                f"Unsupported package manager '{package_manager}' provided. Only 'apt' "
                "and 'yum' package managers are currently supported"
            )


@attrs.define
class License:

    name: str = attrs.field()
    destination: str = attrs.field()
    description: str = attrs.field()
    info_url: str = attrs.field()
    source: Path = attrs.field(
        default=None, converter=lambda x: Path(x) if x is not None else None
    )

    @info_url.validator
    def info_url_validator(self, _, info_url):
        parsed = urlparse(info_url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(
                f"Could not parse info url '{info_url}', please include URL scheme"
            )

    # FIXME: this doesn't work inside images
    # @source.validator
    # def source_validator(self, _, source):
    #     if source is not None and not source.exists():
    #         raise ValueError(
    #             f"Source file for {self.name} license, '{str(source)}', does not exist"
    #         )

    @classmethod
    def column_name(self, name):
        """The column name (and resource name) for the license if it is to be downloaded
        from the source dataset"""
        return name + self.COLUMN_SUFFIX

    COLUMN_SUFFIX = "_LICENSE"


@attrs.define
class PipPackage:
    """Specification of a Python package"""

    name: str
    version: str = attrs.field(
        default=None, converter=lambda v: str(v) if v is not None else None
    )
    url: str = None
    file_path: str = None
    extras: ty.List[str] = attrs.field(factory=list)

    @classmethod
    def unique(cls, pip_specs: ty.Iterable, remove_arcana: bool = False):
        """Merge a list of Pip install specs so each package only appears once

        Parameters
        ----------
        pip_specs : ty.Iterable[PipPackage]
            the pip specs to merge
        remove_arcana : bool
            remove arcana if present from the merged list

        Returns
        -------
        list[PipPackage]
            the merged pip specs

        Raises
        ------
        ArcanaError
            if there is a mismatch between two entries of the same package
        """
        dct = {}
        for pip_spec in pip_specs:
            if isinstance(pip_spec, dict):
                pip_spec = PipPackage(**pip_spec)
            if pip_spec.name == PACKAGE_NAME and remove_arcana:
                continue
            try:
                prev_spec = dct[pip_spec.name]
            except KeyError:
                dct[pip_spec.name] = pip_spec
            else:
                if (
                    prev_spec.version != pip_spec.version
                    or prev_spec.url != pip_spec.url
                    or prev_spec.file_path != pip_spec.file_path
                ):
                    raise RuntimeError(
                        f"Cannot install '{pip_spec.name}' due to conflict "
                        f"between requested versions, {pip_spec} and {prev_spec}"
                    )
                prev_spec.extras.extend(pip_spec.extras)
        return list(dct.values())

    def local_package_location(self, pypi_fallback: bool = False):
        """Detect the installed locations of the packages, including development
        versions.

        Parameters
        ----------
        package : [PipPackage]
            the packages (or names of) the versions to detect
        pypi_fallback : bool, optional
            Fallback to PyPI version if requested version isn't installed locally

        Returns
        -------
        PipPackage
            the pip specification for the installation location of the package
        """

        # if isinstance(pip_spec, str):
        #     parts = pip_spec.split("==")
        #     pip_spec = PipPackage(
        #         name=parts[0], version=(parts[1] if len(parts) == 2 else None)
        #     )
        try:
            pkg = next(
                p for p in pkg_resources.working_set if p.project_name == self.name
            )
        except StopIteration:
            if pypi_fallback:
                logger.info(
                    f"Did not find local installation of package {self.name} "
                    "falling back to installation from PyPI"
                )
                return self
            raise ArcanaBuildError(
                f"Did not find {self.name} in installed working set:\n"
                + "\n".join(
                    sorted(
                        p.key + "/" + p.project_name for p in pkg_resources.working_set
                    )
                )
            )
        if (
            self.version
            and (
                not (pkg.version.endswith(".dirty") or self.version.endswith(".dirty"))
            )
            and pkg.version != self.version
        ):
            msg = (
                f"Requested package {self.name}=={self.version} does "
                "not match installed " + pkg.version
            )
            if pypi_fallback:
                logger.warning(msg + " falling back to installation from PyPI")
                return self
            raise ArcanaBuildError(msg)
        pkg_loc = Path(pkg.location).resolve()
        # Determine whether installed version of requirement is locally
        # installed (and therefore needs to be copied into image) or can
        # be just downloaded from PyPI
        if pkg_loc not in site_pkg_locs:
            # Copy package into Docker image and instruct pip to install from
            # that copy
            local_spec = PipPackage(
                name=self.name, file_path=pkg_loc, extras=self.extras
            )
        else:
            # Check to see whether package is installed via "direct URL" instead
            # of through PyPI
            direct_url_path = Path(pkg.egg_info) / "direct_url.json"
            if direct_url_path.exists():
                with open(direct_url_path) as f:
                    url_spec = json.load(f)
                url = url_spec["url"]
                vcs_info = url_spec.get(
                    "vcs_info", url_spec
                )  # Fallback to trying to find VCS info in the base url-spec dict
                if url.startswith("file://"):
                    local_spec = PipPackage(
                        name=self.name,
                        file_path=url[len("file://") :],
                        extras=self.extras,
                    )
                else:
                    vcs_info = url_spec.get("vcs_info", url_spec)
                    if "vcs" in vcs_info:
                        url = vcs_info["vcs"] + "+" + url
                    if "commit_id" in vcs_info:
                        url += "@" + vcs_info["commit_id"]
                    local_spec = PipPackage(name=self.name, url=url, extras=self.extras)
            else:
                local_spec = PipPackage(
                    name=self.name, version=pkg.version, extras=self.extras
                )
        return local_spec


@attrs.define
class SystemPackage:

    name: str
    version: str = None


@attrs.define
class NeurodockerTemplate:

    name: str
    version: str


@attrs.define
class CondaPackage:

    name: str
    version: str = None

    REQUIRED = ["numpy", "traits"]  # FIXME: Not sure if traits is actually required


def python_package_converter(packages):
    """
    Split out and merge any extras specifications (e.g. "arcana[test]")
    between dependencies of the same package
    """
    return PipPackage.unique(
        ObjectListConverter(PipPackage)(packages), remove_arcana=True
    )


@attrs.define
class Packages:

    system: list[SystemPackage] = attrs.field(
        factory=list,
        converter=ObjectListConverter(SystemPackage),
        metadata={"asdict": named_objects2dict},
    )
    pip: list[PipPackage] = attrs.field(
        factory=list,
        converter=python_package_converter,
        metadata={"asdict": named_objects2dict},
    )
    conda: list[CondaPackage] = attrs.field(
        factory=list,
        converter=ObjectListConverter(CondaPackage),
        metadata={"asdict": named_objects2dict},
    )
    neurodocker: list[NeurodockerTemplate] = attrs.field(
        factory=list,
        converter=ObjectListConverter(NeurodockerTemplate),
        metadata={"asdict": named_objects2dict},
    )


site_pkg_locs = [Path(p).resolve() for p in site.getsitepackages()]
