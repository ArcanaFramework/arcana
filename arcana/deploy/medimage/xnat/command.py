import typing as ty
import json
import re
import attrs
from arcana.core.data.format import FileGroup
from arcana.core.deploy.command import ContainerCommandSpec
from arcana.data.stores.medimage import XnatViaCS
from arcana.data.spaces.medimage import Clinical


@attrs.define
class XnatContainerCommandSpec(ContainerCommandSpec):
    def make_json(
        self,
        dynamic_licenses: ty.List[ty.Tuple[str, str]] = None,
        site_licenses_dataset: ty.Tuple[str, str, str] = None,
    ):
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
        site_licenses_dataset : tuple[str, str, str], optional
            a special dataset containing site-wide licenses, so that they don't need to be
            downloaded to every project. The value is a 3-tuple containing the
            DATASET_NAME, DATASET_USER and DATASET_PASSWORD

        Returns
        -------
        dict
            XNAT container service command specification in JSON-like dict, which can be
            stored within the "org.nrg.commands" label of the container to allow the
            images to be automatically recognised.
        """
        if dynamic_licenses is None:
            dynamic_licenses = []

        # JSON to define all inputs and parameters to the pipelines
        inputs_json = []

        # Add task inputs to inputs JSON specification
        input_args = []
        for inpt in self.inputs:
            replacement_key = f"[{inpt.task_field.upper()}_INPUT]"
            if issubclass(inpt.format, FileGroup):
                desc = f"Match resource [SCAN_TYPE]: {inpt.description} "
                input_type = "string"
            else:
                desc = f"Match field ({inpt.format.dtype}) [FIELD_NAME]: {inpt.description} "
                input_type = self.COMMAND_INPUT_TYPES.get(inpt.format, "string")
            inputs_json.append(
                {
                    "name": self.path2xnatname(inpt.name),
                    "description": desc,
                    "type": input_type,
                    "default-value": inpt.path,
                    "required": False,
                    "user-settable": True,
                    "replacement-key": replacement_key,
                }
            )
            input_args.append(
                f"--input {inpt.name} {inpt.stored_format.location()} '{replacement_key}' {inpt.task_field} {inpt.format.location()} "
            )

        # Add parameters as additional inputs to inputs JSON specification
        param_args = []
        for param in self.parameters:
            desc = f"Parameter ({param.type}): " + param.description

            replacement_key = f"[{param.task_field.upper()}_PARAM]"

            inputs_json.append(
                {
                    "name": param.name,
                    "description": desc,
                    "type": self.COMMAND_INPUT_TYPES.get(param.type, "string"),
                    "default-value": (param.default if param.default else ""),
                    "required": param.required,
                    "user-settable": True,
                    "replacement-key": replacement_key,
                }
            )
            param_args.append(f"--parameter {param.task_field} '{replacement_key}' ")

        # Set up output handlers and arguments
        outputs_json = []
        output_handlers = []
        output_args = []
        for output in self.outputs:
            label = output.path.split("/")[0]
            out_fname = output.path + (
                "." + output.format.ext if output.format.ext else ""
            )
            # Set the path to the
            outputs_json.append(
                {
                    "name": output.name,
                    "description": f"{output.task_field} ({output.format.location()})",
                    "required": True,
                    "mount": "out",
                    "path": out_fname,
                    "glob": None,
                }
            )
            output_handlers.append(
                {
                    "name": f"{output.name}-resource",
                    "accepts-command-output": output.name,
                    "via-wrapup-command": None,
                    "as-a-child-of": "SESSION",
                    "type": "Resource",
                    "label": label,
                    "format": output.format.class_name(),
                }
            )
            output_args.append(
                f"--output {output.name} {output.stored_format.location()} '{output.path}' {output.task_field} {output.format.location()} "
            )

        # Set up fixed arguments used to configure the workflow at initialisation
        config_args = []
        for cname, cvalue in self.configuration.items():
            cvalue_json = json.dumps(cvalue)  # .replace('"', '\\"')
            config_args.append(f"--configuration {cname} '{cvalue_json}' ")

        # Add input for dataset name
        FLAGS_KEY = "#ARCANA_FLAGS#"
        inputs_json.append(
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

        input_args_str = " ".join(input_args)
        output_args_str = " ".join(output_args)
        param_args_str = " ".join(param_args)
        config_args_str = " ".join(config_args)
        licenses_str = " ".join(
            f"--dynamic-license {n} {v}" for n, v in dynamic_licenses
        )

        cmdline = (
            f"conda run --no-capture-output -n {self.image.CONDA_ENV} "  # activate conda
            f"arcana deploy run-in-image xnat-cs//[PROJECT_ID] {self.name} {self.pydra_task} "  # run pydra task in Arcana
            + input_args_str
            + output_args_str
            + param_args_str
            + config_args_str
            + FLAGS_KEY
            + licenses_str
            + " "
            "--dataset-space medimage:Clinical "
            "--dataset-hierarchy subject,session "
            "--single-row [SUBJECT_LABEL],[SESSION_LABEL] "
            f"--row-frequency {self.row_frequency} "
        )  # pass XNAT API details
        # TODO: add option for whether to overwrite existing pipeline

        # Create Project input that can be passed to the command line, which will
        # be populated by inputs derived from the XNAT object passed to the pipeline
        inputs_json.append(
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
            context = ["xnat:imageSessionData"]
            # Create Session input that  can be passed to the command line, which
            # will be populated by inputs derived from the XNAT session object
            # passed to the pipeline.
            inputs_json.extend(
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
            # Add specific session to process to command line args
            cmdline += " --ids [SESSION_LABEL] "
            # Access the session XNAT object passed to the pipeline
            external_inputs = [
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
            derived_inputs = [
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

        else:
            raise NotImplementedError(
                "Wrapper currently only supports session-level pipelines"
            )

        # Generate the complete configuration JSON
        spec_version_str = (
            f" ({self.image.spec_version})" if self.image.spec_version == "0" else ""
        )
        xnat_command = {
            "name": self.name,
            "description": f"{self.name} {self.image.version}{spec_version_str}: {self.description}",
            "label": self.name,
            "version": self.image.full_version,
            "schema-version": "1.0",
            "image": self.image.tag,
            "index": self.image.registry,
            "type": "docker",
            "command-line": cmdline,
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
            "inputs": inputs_json,
            "outputs": outputs_json,
            "xnat": [
                {
                    "name": self.name,
                    "description": self.description,
                    "contexts": context,
                    "external-inputs": external_inputs,
                    "derived-inputs": derived_inputs,
                    "output-handlers": output_handlers,
                }
            ],
        }

        if self.info_url:
            xnat_command["info-url"] = self.info_url

        return xnat_command

    @classmethod
    def path2xnatname(cls, path):
        return re.sub(r"[^a-zA-Z0-9_]+", "_", path)

    COMMAND_INPUT_TYPES = {bool: "bool", str: "string", int: "number", float: "number"}
    VALID_FREQUENCIES = (Clinical.session, Clinical.dataset)
