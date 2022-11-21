from pathlib import Path, PosixPath
import tempfile
import tarfile
import logging
import docker


logger = logging.getLogger("arcana")


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
    Path or None
        path to the extracted file or None if image doesn't exist
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
