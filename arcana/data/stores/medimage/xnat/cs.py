"""
Helper functions for generating XNAT Container Service compatible Docker
containers
"""
import os
import re
import json
import logging
from copy import copy
import typing as ty
from pathlib import Path
import site
import shutil
import tempfile
from numpy import str0
import pkg_resources
from dataclasses import dataclass
import attr
from attr import NOTHING
import neurodocker as nd
from natsort import natsorted
import docker
from arcana import __version__
from arcana.__about__ import install_requires, PACKAGE_NAME, python_versions
from arcana.data.spaces.medimage import Clinical
from arcana.core.data.space import DataSpace
from arcana.core.data.format import FileGroup
from arcana.core.utils import resolve_class, DOCKER_HUB, pkg_from_module
from arcana.exceptions import (
    ArcanaUsageError, ArcanaNoDirectXnatMountException, ArcanaBuildError)
from .api import Xnat

logger = logging.getLogger('arcana')


@attr.s
class XnatViaCS(Xnat):
    """
    Access class for XNAT repositories via the XNAT container service plugin.
    The container service allows the exposure of the underlying file system
    where imaging data can be accessed directly (for performance), and outputs

    Parameters
    ----------
    server : str (URI)
        URI of XNAT server to connect to
    project_id : str
        The ID of the project in the XNAT repository
    cache_dir : str (name_path)
        Path to local directory to cache remote data in
    user : str
        Username with which to connect to XNAT with
    password : str
        Password to connect to the XNAT repository with
    check_md5 : bool
        Whether to check the MD5 digest of cached files before using. This
        checks for updates on the server since the file was cached
    race_cond_delay : int
        The amount of time to wait before checking that the required
        file_group has been downloaded to cache by another process has
        completed if they are attempting to download the same file_group
    """

    INPUT_MOUNT = Path("/input")
    OUTPUT_MOUNT = Path("/output")
    WORK_MOUNT = Path('/work')
    
    frequency: DataSpace = attr.ib(default=Clinical.session)
    node_id: str = attr.ib(default=None)
    input_mount: Path = attr.ib(default=INPUT_MOUNT, converter=Path)
    output_mount: Path = attr.ib(default=OUTPUT_MOUNT, converter=Path)
    server: str = attr.ib()
    user: str = attr.ib()
    password: str = attr.ib()


    alias = 'xnat_via_cs'

    @server.default
    def server_default(self):
        server = os.environ['XNAT_HOST']
        logger.debug("XNAT (via CS) server found %s", server)
        # Convert localhost path to host.docker.internal by default
        match = re.match(r'(https?://)localhost(.*)', server)
        if match:
            server = match.group(1) + 'host.docker.internal' + match.group(2)
            logger.debug("Converted localhost server to %s", server)
        return server

    @user.default
    def user_default(self):
        return os.environ['XNAT_USER']

    @password.default
    def password_default(self):
        return os.environ['XNAT_PASS']

    def get_file_group_paths(self, file_group: FileGroup) -> ty.List[Path]:
        try:
            input_mount = self.get_input_mount(file_group)
        except ArcanaNoDirectXnatMountException:
            # Fallback to API access
            return super().get_file_group_paths(file_group)
        logger.info("Getting %s from %s:%s node via direct access to archive directory",
                    file_group.path, file_group.data_node.frequency,
                    file_group.data_node.id)
        if file_group.uri:
            path = re.match(
                r'/data/(?:archive/)?projects/[a-zA-Z0-9\-_]+/'
                r'(?:subjects/[a-zA-Z0-9\-_]+/)?'
                r'(?:experiments/[a-zA-Z0-9\-_]+/)?(?P<path>.*)$',
                file_group.uri).group('path')
            if 'scans' in path:
                path = path.replace('scans', 'SCANS').replace('resources/', '')
            path = path.replace('resources', 'RESOURCES')
            resource_path = input_mount / path
            if file_group.is_dir:
                # Link files from resource dir into temp dir to avoid catalog XML
                dir_path = self.cache_path(file_group)
                shutil.rmtree(dir_path, ignore_errors=True)
                os.makedirs(dir_path, exist_ok=True)
                for item in resource_path.iterdir():
                    if not item.name.endswith('_catalog.xml'):
                        os.symlink(item, dir_path / item.name)
                fs_paths = [dir_path]
            else:
                fs_paths = list(resource_path.iterdir())
        else:
            logger.debug(
                "No URI set for file_group %s, assuming it is a newly created "
                "derivative on the output mount", file_group)
            stem_path = self.file_group_stem_path(file_group)
            if file_group.is_dir:
                fs_paths = [stem_path]
            else:
                fs_paths = list(stem_path.iterdir())
        return fs_paths

    def put_file_group_paths(self, file_group: FileGroup, fs_paths: ty.List[Path]) -> ty.List[Path]:
        stem_path = self.file_group_stem_path(file_group)
        os.makedirs(stem_path.parent, exist_ok=True)
        cache_paths = []
        for fs_path in fs_paths:
            if file_group.is_dir:
                target_path = stem_path
                shutil.copytree(fs_path, target_path)
            else:
                target_path = file_group.copy_ext(fs_path, stem_path)
                # Upload primary file and add to cache
                shutil.copyfile(fs_path, target_path)
            cache_paths.append(target_path)
        # Update file-group with new values for local paths and XNAT URI
        file_group.uri = (self._make_uri(file_group.data_node)
                          + '/RESOURCES/' + file_group.path)
        logger.info("Put %s into %s:%s node via direct access to archive directory",
                    file_group.path, file_group.data_node.frequency,
                    file_group.data_node.id)
        return cache_paths

    def file_group_stem_path(self, file_group):
        """Determine the paths that derivatives will be saved at"""
        return self.output_mount.joinpath(*file_group.path.split('/'))
    
    def get_input_mount(self, file_group):
        data_node = file_group.data_node
        if self.frequency == data_node.frequency:
            return self.input_mount
        elif self.frequency == Clinical.dataset and data_node.frequency == Clinical.session:
            return self.input_mount / data_node.id
        else:
            raise ArcanaNoDirectXnatMountException

    @classmethod
    def generate_xnat_command(cls,
                              pipeline_name: str,
                              pydra_task: str,
                              image_tag: str,
                              inputs,
                              outputs,
                              description,
                              version,
                              parameters=None,
                              frequency='session',
                              registry=DOCKER_HUB,
                              info_url=None):
        """Constructs the XNAT CS "command" JSON config, which specifies how XNAT
        should handle the containerised pipeline

        Parameters
        ----------
        pipeline_name : str
            Name of the pipeline
        pydra_task
            The module path and name (separated by ':') to the task to execute,
            e.g. australianimagingservice.mri.neuro.mriqc:task
        image_tag : str
            Name + version of the Docker image to be created
        inputs : ty.List[ty.Union[InputArg, tuple]]
            Inputs to be provided to the container (pydra_field, format, dialog_name, frequency).
            'pydra_field' and 'format' will be passed to "inputs" arg of the Dataset.pipeline() method,
            'frequency' to the Dataset.add_source() method and 'dialog_name' is displayed in the XNAT
            UI
        outputs : ty.List[ty.Union[OutputArg, tuple]]
            Outputs to extract from the container (pydra_field, format, output_path).
            'pydra_field' and 'format' will be passed as "outputs" arg the Dataset.pipeline() method,
            'output_path' determines the path the output will saved in the XNAT data tree.
        description : str
            User-facing description of the pipeline
        version : str
            Version string for the wrapped pipeline
        parameters : ty.List[str]
            Parameters to be exposed in the CS command    
        frequency : str
            Frequency of the pipeline to generate (can be either 'dataset' or 'session' currently)
        registry : str
            URI of the Docker registry to upload the image to
        info_url : str
            URI explaining in detail what the pipeline does

        Returns
        -------
        dict
            JSON that can be used 

        Raises
        ------
        ArcanaUsageError
            [description]
        """
        if parameters is None:
            parameters = []
        if isinstance(frequency, str):
            frequency = Clinical[frequency]
        if frequency not in cls.VALID_FREQUENCIES:
            raise ArcanaUsageError(
                f"'{frequency}'' is not a valid option ('"
                + "', '".join(cls.VALID_FREQUENCIES) + "')")

        # Convert tuples to appropriate dataclasses for inputs, outputs and parameters
        inputs = [cls.InputArg(*i) if not isinstance(i, cls.InputArg) else i
                  for i in inputs]
        outputs = [cls.OutputArg(*o) if not isinstance(o, cls.OutputArg) else o
                   for o in outputs]
        parameters = [
            cls.ParamArg(p) if isinstance(p, str) else (
                cls.ParamArg(*p) if not isinstance(p, cls.ParamArg) else p)
            for p in parameters]

        pydra_task = resolve_class(pydra_task)()
        input_specs = dict(f[:2] for f in pydra_task.input_spec.fields)
        # output_specs = dict(f[:2] for f in pydra_task.output_spec.fields)

        # JSON to define all inputs and parameters to the pipelines
        inputs_json = []

        # Add task inputs to inputs JSON specification
        input_args = []
        for inpt in inputs:
            dialog_name = inpt.dialog_name if inpt.dialog_name else inpt.pydra_field
            replacement_key = f'[{dialog_name.upper()}_INPUT]'
            spec = input_specs[inpt.pydra_field]
            
            desc = spec.metadata.get('help_string', '')
            if spec.type in (str, Path):
                desc = (f"Match resource [PATH:STORED_DTYPE]: {desc} ")
                input_type = 'string'
            else:
                desc = f"Match field ({spec.type}) [PATH:STORED_DTYPE]: {desc} "
                input_type = cls.COMMAND_INPUT_TYPES.get(spec.type, 'string')
            inputs_json.append({
                "name": dialog_name,
                "description": desc,
                "type": input_type,
                "default-value": "",
                "required": True,
                "user-settable": True,
                "replacement-key": replacement_key})
            input_args.append(
                f"--input {inpt.pydra_field} {inpt.format} {replacement_key}")

        # Add parameters as additional inputs to inputs JSON specification
        param_args = []
        for param in parameters:
            dialog_name = param.dialog_name if param.dialog_name else param.pydra_field
            spec = input_specs[param.pydra_field]
            desc = f"Parameter ({spec.type}): " + spec.metadata.get('help_string', '')
            required = spec._default is NOTHING
            
            replacement_key = f'[{dialog_name.upper()}_PARAM]'

            inputs_json.append({
                "name": dialog_name,
                "description": desc,
                "type": cls.COMMAND_INPUT_TYPES.get(spec.type, 'string'),
                "default-value": (spec._default if not required else ""),
                "required": required,
                "user-settable": True,
                "replacement-key": replacement_key})
            param_args.append(
                f"--parameter {param.pydra_field} {replacement_key}")

        # Set up output handlers and arguments
        outputs_json = []
        output_handlers = []
        output_args = []
        for output in outputs:
            xnat_path = output.xnat_path if output.xnat_path else output.pydra_field
            label = xnat_path.split('/')[0]
            out_fname = xnat_path + (output.format.ext if output.format.ext else '')
            # output_fname = xnat_path
            # if output.format.ext is not None:
            #     output_fname += output.format.ext
            # Set the path to the 
            outputs_json.append({
                "name": output.pydra_field,
                "description": f"{output.pydra_field} ({output.format})",
                "required": True,
                "mount": "out",
                "path": out_fname,
                "glob": None})
            output_handlers.append({
                "name": f"{output.pydra_field}-resource",
                "accepts-command-output": output.pydra_field,
                "via-wrapup-command": None,
                "as-a-child-of": "SESSION",
                "type": "Resource",
                "label": label,
                "format": output.format.name})
            output_args.append(
                f'--output {output.pydra_field} {output.format} {xnat_path}')

        input_args_str = ' '.join(input_args)
        output_args_str = ' '.join(output_args)
        param_args_str = ' '.join(param_args)

        cmdline = (
            f"conda run --no-capture-output -n arcana "  # activate conda
            f"arcana run {pydra_task} "  # run pydra task in Arcana
            f"[PROJECT_ID] {input_args_str} {output_args_str} {param_args_str} " # inputs, outputs + params
            f"--ignore_blank_inputs "  # Allow input patterns to be blank, just ignore them in that case
            f"--pydra_plugin serial "  # Use serial processing instead of parallel to simplify outputs
            f"--work {cls.WORK_MOUNT} "  # working directory
            f"--store xnat_via_cs {frequency} ")  # pass XNAT API details

        # Create Project input that can be passed to the command line, which will
        # be populated by inputs derived from the XNAT object passed to the pipeline
        inputs_json.append(
            {
                "name": "PROJECT_ID",
                "description": "Project ID",
                "type": "string",
                "required": True,
                "user-settable": False,
                "replacement-key": "[PROJECT_ID]"
            })

        # Access session via Container service args and derive 
        if frequency == Clinical.session:
            # Set the object the pipeline is to be run against
            context = ["xnat:imageSessionData"]
            cmdline += ' [SESSION_LABEL]'  # Pass node-id to XnatViaCS repo
            # Create Session input that  can be passed to the command line, which
            # will be populated by inputs derived from the XNAT session object
            # passed to the pipeline.
            inputs_json.append(
                {
                    "name": "SESSION_LABEL",
                    "description": "Imaging session label",
                    "type": "string",
                    "required": True,
                    "user-settable": False,
                    "replacement-key": "[SESSION_LABEL]"
                })
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
                    "load-children": True}]
            # Access to project ID and session label from session XNAT object
            derived_inputs = [
                {
                    "name": "__SESSION_LABEL__",
                    "type": "string",
                    "derived-from-wrapper-input": "SESSION",
                    "derived-from-xnat-object-property": "label",
                    "provides-value-for-command-input": "SESSION_LABEL",
                    "user-settable": False
                },
                {
                    "name": "__PROJECT_ID__",
                    "type": "string",
                    "derived-from-wrapper-input": "SESSION",
                    "derived-from-xnat-object-property": "project-id",
                    "provides-value-for-command-input": "PROJECT_ID",
                    "user-settable": False
                }]
        
        else:
            raise NotImplementedError(
                "Wrapper currently only supports session-level pipelines")

        # Generate the complete configuration JSON
        xnat_command = {
            "name": pipeline_name,
            "description": description,
            "label": pipeline_name,
            "version": version,
            "schema-version": "1.0",
            "image": image_tag,
            "index": registry,
            "type": "docker",
            "command-line": cmdline,
            "override-entrypoint": True,
            "mounts": [
                {
                    "name": "in",
                    "writable": False,
                    "path": str(cls.INPUT_MOUNT)
                },
                {
                    "name": "out",
                    "writable": True,
                    "path": str(cls.OUTPUT_MOUNT)
                },
                {  # Saves the Pydra-cache directory outside of the container for easier debugging
                    "name": "work",
                    "writable": True,
                    "path": str(cls.WORK_MOUNT)
                }
            ],
            "ports": {},
            "inputs": inputs_json,
            "outputs": outputs_json,
            "xnat": [
                {
                    "name": pipeline_name,
                    "description": description,
                    "contexts": context,
                    "external-inputs": external_inputs,
                    "derived-inputs": derived_inputs,
                    "output-handlers": output_handlers
                }
            ]
        }

        if info_url:
            xnat_command['info-url'] = info_url

        return xnat_command

    @dataclass
    class InputArg():
        pydra_field: str  # Must match the name of the Pydra task input
        format: type
        dialog_name: str = None # The name of the parameter in the XNAT dialog, defaults to the pydra name
        frequency: Clinical = Clinical.session

    @dataclass
    class OutputArg():
        pydra_field: str  # Must match the name of the Pydra task output
        format: type
        xnat_path: str = None  # The path the output is stored at in XNAT, defaults to the pydra name

    @dataclass
    class ParamArg():
        pydra_field: str  # Name of parameter to expose in Pydra task
        dialog_name: str = None  # defaults to pydra_field


    COMMAND_INPUT_TYPES = {
        bool: 'bool',
        str: 'string',
        int: 'number',
        float: 'number'}

    VALID_FREQUENCIES = (Clinical.session, Clinical.dataset)




# def get_existing_docker_tags(docker_registry, docker_org, image_name):
#     result = requests.get(
#         f'https://{docker_registry}/v2/repositories/{docker_org}/{image_name}/tags')
#     return [r['name'] for r in result.json()]
