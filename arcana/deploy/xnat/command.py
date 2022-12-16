from __future__ import annotations
import typing as ty
import re
import attrs
from arcana.core.data.type.base import FileGroup
from arcana.core.deploy.command.base import ContainerCommand
from arcana.data.stores.xnat import XnatViaCS
from arcana.data.spaces.medimage import Clinical
from arcana.core.utils.serialize import ClassResolver

if ty.TYPE_CHECKING:
    from .image import XnatCSImage


@attrs.define(kw_only=True)
class XnatCSCommand(ContainerCommand):

    DATA_SPACE = Clinical

    # Hard-code the data_space of XNAT commands to be clinical
    image: XnatCSImage = None

    def make_json(self):
        """Constructs the XNAT CS "command" JSON config, which specifies how XNAT
        should handle the containerised pipeline

        Parameters
        ----------
        name : str
            Name of the container service pipeline
        registry : str
            URI of the Docker registry to upload the image to
        dynamic_licenses : list[tuple[str, str]]
            licenses that need to be downloaded at runtime as they can't be stored within
            the Docker image

        Returns
        -------
        dict
            XNAT container service command specification in JSON-like dict, which can be
            stored within the "org.nrg.commands" label of the container to allow the
            images to be automatically recognised.
        """

        cmd_json = self.init_command_json()

        input_args = self.add_input_fields(cmd_json)

        param_args = self.add_parameter_fields(cmd_json)

        output_args = self.add_output_fields(cmd_json)

        flag_arg = self.add_arcana_flags_field(cmd_json)

        xnat_input_args = self.add_inputs_from_xnat(cmd_json)

        cmd_json["command-line"] = (
            self.activate_conda_cmd()
            + "arcana ext xnat cs-entrypoint xnat-cs//[PROJECT_ID] "
            + " ".join(
                input_args + output_args + param_args + xnat_input_args + [flag_arg]
            )
        )

        return cmd_json

    def init_command_json(self):
        """Initialises the command JSON that specifies to the XNAT Cs how the command
        should be run

        Returns
        -------
        dict[str, *]
            the JSON-like dictionary to specify the command to the XNAT CS
        """
        # Generate the complete configuration JSON
        build_iteration_str = (
            f" ({self.image.build_iteration})"
            if self.image.build_iteration is None
            else ""
        )

        cmd_json = {
            "name": self.name,
            "description": f"{self.name} {self.image.version}{build_iteration_str}: {self.image.description}",
            "label": self.name,
            "schema-version": "1.0",
            "image": self.image.reference,
            "index": self.image.registry,
            "datatype": "docker",
            # "command-line": cmdline,
            "override-entrypoint": True,
            "mounts": [
                {"name": "in", "writable": False, "path": str(XnatViaCS.INPUT_MOUNT)},
                {"name": "out", "writable": True, "path": str(XnatViaCS.OUTPUT_MOUNT)},
                {  # Saves the Pydra-cache directory outside of the container for easier debugging
                    "name": "work",
                    "writable": True,
                    "path": str(XnatViaCS.WORK_MOUNT),
                },
            ],
            "ports": {},
            "inputs": [],  # inputs_json,
            "outputs": [],  # outputs_json,
            "xnat": [
                {
                    "name": self.name,
                    "description": self.image.description,
                    "contexts": [],  # context,
                    "external-inputs": [],  # external_inputs,
                    "derived-inputs": [],  # derived_inputs,
                    "output-handlers": [],  # output_handlers,
                }
            ],
        }

        return cmd_json

    def add_input_fields(self, cmd_json):
        """Adds pipeline inputs to the command JSON

        Parameters
        ----------
        cmd_json : dict
            JSON-like dictionary to be passed to the XNAT container service to specify
            how to run a command

        Returns
        -------
        list[str]
            list of arguments to be appended to the command line
        """
        # Add task inputs to inputs JSON specification
        cmd_args = []
        for inpt in self.inputs:
            replacement_key = f"[{inpt.field.upper()}_INPUT]"
            if issubclass(inpt.datatype, FileGroup):
                desc = f"Match resource [SCAN_TYPE]: {inpt.help_string} "
                input_type = "string"
            else:
                desc = f"Match field ({inpt.datatype.dtype}) [FIELD_NAME]: {inpt.description} "
                input_type = self.COMMAND_INPUT_TYPES.get(inpt.datatype, "string")
            cmd_json["inputs"].append(
                {
                    "name": self.path2xnatname(inpt.name),
                    "description": desc,
                    "type": input_type,
                    "default-value": inpt.config_dict.get("path", ""),
                    "required": False,
                    "user-settable": True,
                    "replacement-key": replacement_key,
                }
            )
            cmd_args.append(f"--input {inpt.name} '{replacement_key}'")

        return cmd_args

    def add_parameter_fields(self, cmd_json):

        # Add parameters as additional inputs to inputs JSON specification
        cmd_args = []
        for param in self.parameters:
            desc = f"Parameter ({param.datatype}): " + param.help_string

            replacement_key = f"[{param.field.upper()}_PARAM]"

            cmd_json["inputs"].append(
                {
                    "name": param.name,
                    "description": desc,
                    "type": self.COMMAND_INPUT_TYPES.get(param.datatype, "string"),
                    "default-value": (param.default if param.default else ""),
                    "required": param.required,
                    "user-settable": True,
                    "replacement-key": replacement_key,
                }
            )
            cmd_args.append(f"--parameter {param.name} '{replacement_key}'")

        return cmd_args

    def add_output_fields(self, cmd_json):

        # Set up output handlers and arguments
        cmd_args = []
        for output in self.outputs:
            out_fname = output.name + (
                "." + output.datatype.ext if output.datatype.ext else ""
            )
            # Set the path to the
            cmd_json["outputs"].append(
                {
                    "name": output.name,
                    "description": f"{output.field} ({ClassResolver.tostr(output.datatype)})",
                    "required": True,
                    "mount": "out",
                    "path": out_fname,
                    "glob": None,
                }
            )
            cmd_json["xnat"][0]["output-handlers"].append(
                {
                    "name": f"{output.name}-resource",
                    "accepts-command-output": output.name,
                    "via-wrapup-command": None,
                    "as-a-child-of": "SESSION",
                    "type": "Resource",
                    "label": output.name,
                    "format": output.datatype.class_name(),
                }
            )
            cmd_args.append(f"--output {output.name} '{output.name}'")

        return cmd_args

    def add_arcana_flags_field(self, cmd_json):

        # Add input for dataset name
        FLAGS_KEY = "#ARCANA_FLAGS#"
        cmd_json["inputs"].append(
            {
                "name": "Arcana_flags",
                "description": "Flags passed to `run-arcana-pipeline` command",
                "type": "string",
                "default-value": (
                    "--plugin serial "
                    "--work /wl "  # NB: work dir moved inside container due to file-locking issue on some mounted volumes (see https://github.com/tox-dev/py-filelock/issues/147)
                    "--dataset-name default "
                    "--loglevel info "
                    f"--export-work {XnatViaCS.WORK_MOUNT}"
                ),
                "required": False,
                "user-settable": True,
                "replacement-key": FLAGS_KEY,
            }
        )

        return FLAGS_KEY

    def add_inputs_from_xnat(self, cmd_json):

        # Define the fixed subject>session dataset hierarchy of XNAT, i.e. the data
        # tree contains two levels, one for subjects and the other for sessions
        cmd_args = ["--dataset-hierarchy subject,session"]

        # Create Project input that can be passed to the command line, which will
        # be populated by inputs derived from the XNAT object passed to the pipeline
        cmd_json["inputs"].append(
            {
                "name": "PROJECT_ID",
                "description": "Project ID",
                "type": "string",
                "required": True,
                "user-settable": False,
                "replacement-key": "[PROJECT_ID]",
            }
        )

        # Access session via Container service args and derive
        if self.row_frequency == Clinical.session:
            # Set the object the pipeline is to be run against
            cmd_json["xnat"][0]["context"] = ["xnat:imageSessionData"]
            # Create Session input that  can be passed to the command line, which
            # will be populated by inputs derived from the XNAT session object
            # passed to the pipeline.
            cmd_json["inputs"].extend(
                [
                    {
                        "name": "SESSION_LABEL",
                        "description": "Imaging session label",
                        "type": "string",
                        "required": True,
                        "user-settable": False,
                        "replacement-key": "[SESSION_LABEL]",
                    },
                    {
                        "name": "SUBJECT_LABEL",
                        "description": "Subject label",
                        "type": "string",
                        "required": True,
                        "user-settable": False,
                        "replacement-key": "[SUBJECT_LABEL]",
                    },
                ]
            )

            # Access the session XNAT object passed to the pipeline
            cmd_json["xnat"][0]["external-inputs"] = [
                {
                    "name": "SESSION",
                    "description": "Imaging session",
                    "type": "Session",
                    "source": None,
                    "default-value": None,
                    "required": True,
                    "replacement-key": None,
                    "sensitive": None,
                    "provides-value-for-command-input": None,
                    "provides-files-for-command-mount": "in",
                    "via-setup-command": None,
                    "user-settable": False,
                    "load-children": True,
                }
            ]
            # Access to project ID and session label from session XNAT object
            cmd_json["xnat"][0]["derived-inputs"] = [
                {
                    "name": "__SESSION_LABEL__",
                    "type": "string",
                    "derived-from-wrapper-input": "SESSION",
                    "derived-from-xnat-object-property": "label",
                    "provides-value-for-command-input": "SESSION_LABEL",
                    "user-settable": False,
                },
                {
                    "name": "__SUBJECT_ID__",
                    "type": "string",
                    "derived-from-wrapper-input": "SESSION",
                    "derived-from-xnat-object-property": "subject-id",
                    "provides-value-for-command-input": "SUBJECT_LABEL",
                    "user-settable": False,
                },
                {
                    "name": "__PROJECT_ID__",
                    "type": "string",
                    "derived-from-wrapper-input": "SESSION",
                    "derived-from-xnat-object-property": "project-id",
                    "provides-value-for-command-input": "PROJECT_ID",
                    "user-settable": False,
                },
            ]

            # Add specific session to process to command line args
            cmd_args.extend(
                [
                    "--ids [SESSION_LABEL]",
                    "--single-row [SUBJECT_LABEL],[SESSION_LABEL]",
                ]
            )

        else:
            raise NotImplementedError(
                "Wrapper currently only supports session-level pipelines"
            )

        return cmd_args

    @classmethod
    def path2xnatname(cls, path):
        return re.sub(r"[^a-zA-Z0-9_]+", "_", path)

    COMMAND_INPUT_TYPES = {bool: "bool", str: "string", int: "number", float: "number"}
    VALID_FREQUENCIES = (Clinical.session, Clinical.dataset)
