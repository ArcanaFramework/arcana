import json
import tempfile
import attrs
from pathlib import Path
from .base import ArcanaImage


@attrs.define
class Metapackage(ArcanaImage):

    manifest: dict

    def construct_dockerfile(
        self,
        build_dir: Path = None,
        use_local_packages=False,
    ):

        if build_dir is None:
            build_dir = Path(tempfile.mkdtemp())

        dockerfile = self.init_dockerfile()

        self.install_python(
            dockerfile, build_dir, use_local_packages=use_local_packages
        )

        with open(build_dir / "manifest.json", "w") as f:
            json.dump(self.manifest, f)

        dockerfile.copy(["./manifest.json"], "/manifest.json")

        dockerfile.entrypoint(
            [
                "conda",
                "run",
                "--no-capture-output",
                "-n",
                "arcana",
                "arcana",
                "ext",
                "xnat",
                "pull-images",
                "/manifest.json",
            ]
        )

        return dockerfile
