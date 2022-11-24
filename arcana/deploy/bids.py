from pathlib import Path
import shlex
import attrs
from neurodocker.reproenv import DockerRenderer
from arcana.core.deploy.image import ContainerImageWithCommand


@attrs.define
class BidsApp(ContainerImageWithCommand):
    def add_entrypoint(self, dockerfile: DockerRenderer, build_dir: Path):
        dockerfile.entrypoint(shlex.split(self.command.command_line()))

    ENTRYPOINT = """!#/usr/bin/env bash

INPUT=$1
OUTPUT=$2
if [ "$3" == "participant" ]; then
    FREQ=session
elif [ "$3" == "group" ]; then
    FREQ=dataset
else
    echo "Unrecognised analysis level '$3'"
    exit(1);
fi

{command_line} bids//$1 $2 --row_frequency $FREQ

    """
