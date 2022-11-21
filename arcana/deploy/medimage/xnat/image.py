from __future__ import annotations
import sys
from pathlib import Path
import json
import attrs
from neurodocker.reproenv import DockerRenderer
from arcana.data.stores.medimage import XnatViaCS
from arcana.core.utils import class_location, ListDictConverter
from arcana.core.data.store import DataStore
from arcana.core.deploy.image import PipelineImage
from .command import XnatCSCommand


@attrs.define(kw_only=True)
class XnatCSImage(PipelineImage):

    commands: list[XnatCSCommand] = attrs.field(
        converter=ListDictConverter(
            XnatCSCommand
        )  # Change the command type to XnatCSCommand subclass
    )

    def construct_dockerfile(
        self,
        build_dir: Path,
        test_config: bool = False,
        **kwargs,
    ):
        """Creates a Docker image containing one or more XNAT commands ready
        to be installed in XNAT's container service plugin

        Parameters
        ----------
        build_dir : Path
            the directory to build the docker image within, i.e. where to write
            Dockerfile and supporting files to be copied within the image
        test_config : bool
            whether to create the container so that it will work with the test
            XNAT configuration (i.e. hard-coding the XNAT server IP)
        **kwargs:
            Passed on to super `construct_dockerfile` method

        Returns
        -------
        DockerRenderer
            the Neurodocker renderer
        Path
            path to build directory
        """

        dockerfile = super().construct_dockerfile(build_dir, **kwargs)

        xnat_commands = [c.make_json() for c in self.commands]

        # Copy the generated XNAT commands inside the container for ease of reference
        self.copy_command_ref(dockerfile, xnat_commands, build_dir)

        self.save_store_config(dockerfile, build_dir, test_config=test_config)

        # Convert XNAT command label into string that can by placed inside the
        # Docker label
        command_label = json.dumps(xnat_commands).replace("$", r"\$")

        self.add_labels(
            dockerfile,
            {"org.nrg.commands": command_label, "maintainer": self.authors[0].email},
        )

        return dockerfile

    def copy_command_ref(self, dockerfile: DockerRenderer, xnat_commands, build_dir):
        """Copy the generated command JSON within the Docker image for future reference

        Parameters
        ----------
        dockerfile : DockerRenderer
            Neurodocker renderer to build
        xnat_commands : list[dict]
            XNAT command JSONs to copy into the Dockerfile for reference
        build_dir : Path
            path to build directory
        """
        # Copy command JSON inside dockerfile for ease of reference
        cmds_dir = build_dir / "xnat_commands"
        cmds_dir.mkdir(exist_ok=True)
        for cmd in xnat_commands:
            fname = cmd.get("name", "command") + ".json"
            with open(cmds_dir / fname, "w") as f:
                json.dump(cmd, f, indent="    ")
        dockerfile.copy(source=["./xnat_commands"], destination="/xnat_commands")

    def save_store_config(
        self, dockerfile: DockerRenderer, build_dir: Path, test_config=False
    ):
        """Save a configuration for a XnatViaCS store.

        Parameters
        ----------
        dockerfile : DockerRenderer
            Neurodocker renderer to build
        build_dir : Path
            the build directory to save supporting files
        test_config : bool
            whether the target XNAT is using the local test configuration, in which
            case the server location will be hard-coded rather than rely on the
            XNAT_HOST environment variable passed to the container by the XNAT CS
        """
        xnat_cs_store_entry = {"class": "<" + class_location(XnatViaCS) + ">"}
        if test_config:
            if sys.platform == "linux":
                ip_address = "172.17.0.1"  # Linux + GH Actions
            else:
                ip_address = "host.docker.internal"  # Mac/Windows local debug
            xnat_cs_store_entry["server"] = "http://" + ip_address + ":8080"
        DataStore.save_entries(
            {"xnat-cs": xnat_cs_store_entry}, config_path=build_dir / "stores.yaml"
        )
        dockerfile.run(command="mkdir -p /root/.arcana")
        dockerfile.run(command=f"mkdir -p {str(XnatViaCS.CACHE_DIR)}")
        dockerfile.copy(
            source=["./stores.yaml"],
            destination=self.IN_DOCKER_ARCANA_HOME_DIR + "/stores.yaml",
        )
        dockerfile.env(ARCANA_HOME=self.IN_DOCKER_ARCANA_HOME_DIR)
