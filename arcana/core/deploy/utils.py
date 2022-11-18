import typing as ty
from pathlib import Path, PosixPath
import json
import site
import tempfile
import tarfile
import logging
import pkg_resources
from dataclasses import dataclass, field as dataclass_field
import docker
from arcana.__about__ import PACKAGE_NAME
from arcana.exceptions import ArcanaBuildError
from arcana.exceptions import ArcanaError


logger = logging.getLogger("arcana")


@dataclass
class PipSpec:
    """Specification of a Python package"""

    name: str
    version: str = None
    url: str = None
    file_path: str = None
    extras: ty.List[str] = dataclass_field(default_factory=list)

    def __post_init__(self):
        if self.version and not isinstance(self.version, str):
            self.version = str(self.version)

    @classmethod
    def unique(cls, pip_specs: ty.Iterable, remove_arcana: bool = False):
        """Merge a list of Pip install specs so each package only appears once

        Parameters
        ----------
        pip_specs : ty.Iterable[PipSpec]
            the pip specs to merge
        remove_arcana : bool
            remove arcana if present from the merged list

        Returns
        -------
        list[PipSpec]
            the merged pip specs

        Raises
        ------
        ArcanaError
            if there is a mismatch between two entries of the same package
        """
        dct = {}
        for pip_spec in pip_specs:
            if isinstance(pip_spec, dict):
                pip_spec = PipSpec(**pip_spec)
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
                    raise ArcanaError(
                        f"Cannot install '{pip_spec.name}' due to conflict "
                        f"between requested versions, {pip_spec} and {prev_spec}"
                    )
                prev_spec.extras.extend(pip_spec.extras)
        return list(dct.values())


def local_package_location(pip_spec: PipSpec, pypi_fallback: bool = False):
    """Detect the installed locations of the packages, including development
    versions.

    Parameters
    ----------
    package : [PipSpec]
        the packages (or names of) the versions to detect
    pypi_fallback : bool, optional
        Fallback to PyPI version if requested version isn't installed locally

    Returns
    -------
    PipSpec
        the pip specification for the installation location of the package
    """

    if isinstance(pip_spec, str):
        parts = pip_spec.split("==")
        pip_spec = PipSpec(
            name=parts[0], version=(parts[1] if len(parts) == 2 else None)
        )
    try:
        pkg = next(
            p for p in pkg_resources.working_set if p.project_name == pip_spec.name
        )
    except StopIteration:
        if pypi_fallback:
            logger.info(
                f"Did not find local installation of package {pip_spec.name} "
                "falling back to installation from PyPI"
            )
            return pip_spec
        raise ArcanaBuildError(
            f"Did not find {pip_spec.name} in installed working set:\n"
            + "\n".join(
                sorted(p.key + "/" + p.project_name for p in pkg_resources.working_set)
            )
        )
    if (
        pip_spec.version
        and (
            not (pkg.version.endswith(".dirty") or pip_spec.version.endswith(".dirty"))
        )
        and pkg.version != pip_spec.version
    ):
        msg = (
            f"Requested package {pip_spec.name}=={pip_spec.version} does "
            "not match installed " + pkg.version
        )
        if pypi_fallback:
            logger.warning(msg + " falling back to installation from PyPI")
            return pip_spec
        raise ArcanaBuildError(msg)
    pkg_loc = Path(pkg.location).resolve()
    # Determine whether installed version of requirement is locally
    # installed (and therefore needs to be copied into image) or can
    # be just downloaded from PyPI
    if pkg_loc not in site_pkg_locs:
        # Copy package into Docker image and instruct pip to install from
        # that copy
        pip_spec = PipSpec(
            name=pip_spec.name, file_path=pkg_loc, extras=pip_spec.extras
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
                pip_spec = PipSpec(
                    name=pip_spec.name,
                    file_path=url[len("file://") :],
                    extras=pip_spec.extras,
                )
            else:
                vcs_info = url_spec.get("vcs_info", url_spec)
                if "vcs" in vcs_info:
                    url = vcs_info["vcs"] + "+" + url
                if "commit_id" in vcs_info:
                    url += "@" + vcs_info["commit_id"]
                pip_spec = PipSpec(name=pip_spec.name, url=url, extras=pip_spec.extras)
        else:
            pip_spec = PipSpec(
                name=pip_spec.name, version=pkg.version, extras=pip_spec.extras
            )
    return pip_spec


def extract_file_from_docker_image(
    image_tag, file_path: PosixPath, out_path: Path = None
) -> Path:
    """Extracts a file from a Docker image onto the local host

    Parameters
    ----------
    image_tag : str
        the name/tag of the image to extract the file from
    file_path : PosixPath
        the path to the file inside the image

    Returns
    -------
    Path
        path to the extracted file
    """
    tmp_dir = Path(tempfile.mkdtemp())
    if out_path is None:
        out_path = tmp_dir / "extracted-dir"
    dc = docker.from_env()
    try:
        dc.api.pull(image_tag)
    except docker.errors.APIError as e:
        if e.response.status_code in (404, 500):
            return None
        else:
            raise
    else:
        container = dc.containers.get(dc.api.create_container(image_tag)["Id"])
        try:
            tarfile_path = tmp_dir / "tar-file.tar.gz"
            with open(tarfile_path, mode="w+b") as f:
                try:
                    stream, _ = dc.api.get_archive(container.id, str(file_path))
                except docker.errors.NotFound:
                    pass
                else:
                    for chunk in stream:
                        f.write(chunk)
                    f.flush()
        finally:
            container.remove()
        with tarfile.open(tarfile_path) as f:
            f.extractall(out_path)
    return out_path


def escaped_md(value: str) -> str:
    if not value:
        return ""
    return f"`{value}`"


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


DOCKER_HUB = "docker.io"
site_pkg_locs = [Path(p).resolve() for p in site.getsitepackages()]
