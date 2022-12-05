from pathlib import Path
import shlex
import attrs
from neurodocker.reproenv import DockerRenderer
from arcana.core.deploy.image import CommandImage


@attrs.define
class PipelineImage(CommandImage):
    def add_entrypoint(self, dockerfile: DockerRenderer, build_dir: Path):
        dockerfile.entrypoint(shlex.split(self.command.command_line()))
