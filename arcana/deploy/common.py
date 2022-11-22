from pathlib import Path
import shlex
import attrs
from neurodocker.reproenv import DockerRenderer
from arcana.core.deploy.image import BasePipelineImage


@attrs.define
class PipelineImage(BasePipelineImage):
    def add_entrypoint(self, dockerfile: DockerRenderer, build_dir: Path):
        dockerfile.entrypoint(shlex.split(self.commands[0].command_line()))
