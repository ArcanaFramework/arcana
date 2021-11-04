"""
Helper functions for generating XNAT Container Service compatible Docker
containers
"""
import os
import re
import json
import logging
from pathlib import Path
import site
import shutil
import tempfile
import pkg_resources
from dataclasses import dataclass
import attr
import cloudpickle as cp
from attr import NOTHING
import neurodocker as nd
from natsort import natsorted
from arcana2.__about__ import install_requires
from arcana2.data.spaces.clinical import Clinical
from arcana2.core.data.type import FileFormat
from arcana2.core.data.space import DataSpace
from arcana2.core.utils import resolve_class, DOCKER_HUB, ARCANA_PIP
from arcana2.exceptions import ArcanaUsageError, ArcanaNoDirectXnatMountException
from arcana2.__about__ import PACKAGE_NAME, python_versions
from .api import Xnat

logger = logging.getLogger('arcana')


@attr.s
class XnatViaCS(Xnat):
    """
    Access class for XNAT repositories via XNAT's built-in container service.
    The container service allows the exposure of the underlying file system
    where imaging data

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


    def get_file_group(self, file_group):
        try:
            input_mount = self.get_input_mount(file_group)
        except ArcanaNoDirectXnatMountException:
            # Fallback to API access
            return super().get_file_group(file_group)
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
            if file_group.datatype.directory:
                # Link files from resource dir into temp dir to avoid catalog XML
                primary_path = self.cache_path(file_group)
                shutil.rmtree(primary_path, ignore_errors=True)
                os.makedirs(primary_path, exist_ok=True)
                for item in resource_path.iterdir():
                    if not item.name.endswith('_catalog.xml'):
                        os.symlink(item, primary_path / item.name)
                side_cars = {}
            else:
                primary_path, side_cars = file_group.datatype.assort_files(
                    resource_path.iterdir())
        else:
            logger.debug(
                "No URI set for file_group %s, assuming it is a newly created "
                "derivative on the output mount", file_group)
            primary_path, side_cars = self.get_output_paths(file_group)
        return primary_path, side_cars

    def put_file_group(self, file_group, fs_path, side_cars):
        primary_path, side_car_paths = self.get_output_paths(file_group)
        if file_group.datatype.directory:
            shutil.copytree(fs_path, primary_path)
        else:
            os.makedirs(primary_path.parent, exist_ok=True)
            # Upload primary file and add to cache
            shutil.copyfile(fs_path, primary_path)
            # Upload side cars and add them to cache
            for sc_name, sc_src_path in side_cars.items():
                shutil.copyfile(sc_src_path, side_car_paths[sc_name])
        # Update file-group with new values for local paths and XNAT URI
        file_group.set_fs_paths(primary_path, side_car_paths)
        file_group.uri = (self._make_uri(file_group.data_node)
                          + '/RESOURCES/' + self.escape_name(file_group.path))
        logger.info("Put %s into %s:%s node via direct access to archive directory",
                    file_group.path, file_group.data_node.frequency,
                    file_group.data_node.id)

    def get_output_paths(self, file_group):
        escaped_name = self.escape_name(file_group.path)
        resource_path = self.output_mount / escaped_name
        side_car_paths = {}
        if file_group.datatype.directory:
            primary_path = resource_path
        else:
            os.makedirs(resource_path, exist_ok=True)
            # Upload primary file and add to cache
            fname = escaped_name + file_group.datatype.extension
            primary_path = resource_path / fname
            # Upload side cars and add them to cache
            for sc_name, sc_ext in file_group.datatype.side_cars.items():
                sc_fname = escaped_name + sc_ext
                sc_fpath = resource_path / sc_fname
                side_car_paths[sc_name] = sc_fpath
        return primary_path, side_car_paths
    
    def get_input_mount(self, file_group):
        data_node = file_group.data_node
        if self.frequency == data_node.frequency:
            return self.input_mount
        elif self.frequency == Clinical.dataset and data_node.frequency == Clinical.session:
            return self.input_mount / data_node.id
        else:
            raise ArcanaNoDirectXnatMountException

    

    @classmethod
    def generate_dockerfile(cls, pydra_task, json_config, maintainer, build_dir,
                            requirements=None, packages=None, extra_labels=None):
        """Constructs a dockerfile that wraps a with dependencies

        Parameters
        ----------
        pydra_task : pydra.Task or pydra.Workflow
            The Pydra Task or Workflow to be run in XNAT container service
        json_config : dict[str, Any]
            The command JSON (as generated by `generate_json_config`) to insert
            into a label of the docker file.
        maintainer : str
            The name and email of the developer creating the wrapper (i.e. you)   
        build_dir : Path
            Path to the directory to create the Dockerfile in and copy any local
            files to
        requirements : list[tuple[str, str]]
            Name and version of the Neurodocker requirements to add to the image
        packages : list[tuple[str, str]]
            Name and version of the Python PyPI packages to add to the image
        registry : str
            URI of the Docker registry to upload the image to
        extra_labels : dict[str, str], optional
            Additional labels to be added to the image

        Returns
        -------
        str
            Generated Dockerfile
        """

        labels = {}
        packages = list(packages)

        if build_dir is None:
            build_dir = tempfile.mkdtemp()
        if requirements is None:
            requirements = []
        if packages is None:
            packages = []

        if maintainer:
            labels["maintainer"] = maintainer

        pipeline_name = pydra_task.name.replace('.', '_').capitalize()

        # Convert JSON into Docker label
        labels['org.nrg.commands'] = '[' + json.dumps(json_config) + ']'
        if extra_labels:
            labels.update(extra_labels)

        instructions = [
            ["base", "debian:bullseye"],
            ["install", ["git", "vim", "ssh-client", "python3", "python3-pip"]]]

        for req in requirements:
            req_name = req[0]
            install_props = {}
            if len(req) > 1 and req[1] != '.':
                install_props['version'] = req[1]
            if len(req) > 2:
                install_props['method'] = req[2]
            instructions.append([req_name, install_props])

        site_pkg_locs = [Path(p).resolve() for p in site.getsitepackages()]

        # Copies the local working copy of arcana and pydra (+sub-packages)
        # into the dockerfile if present instead of relying on the PyPI version,
        # which might be missing bugfixes
        for pkg_name in ['arcana2'] + [re.split(r'[>=]+', p)[0]
                                       for p in install_requires
                                       if p.startswith('pydra')]:
            
            pkg = next(p for p in pkg_resources.working_set
                       if p.key == pkg_name)
            pkg_loc = Path(pkg.location).resolve()
            # Use local installation of arcana
            if pkg_loc not in site_pkg_locs:
                shutil.rmtree(build_dir / pkg_name, ignore_errors=True)
                shutil.copytree(pkg_loc, build_dir / pkg_name)
                pip_address = '/' + pkg_name
                instructions.append(['copy', ['./' + pkg_name, pip_address]])
            else:
                direct_url_path = Path(pkg.egg_info) / 'direct_url.json'
                if direct_url_path.exists():
                    with open(direct_url_path) as f:
                        durl = json.load(f)             
                    pip_address = f"{durl['vcs']}+{durl['url']}@{durl['commit_id']}"
                else:
                    pip_address = f"{pkg.key}=={pkg.version}"
            packages.append(pip_address)

        # instructions.append(['run', 'pip3 install ' + ' '.join(packages)])

        instructions.append(
            ["miniconda", {
                "create_env": "arcana",
                "conda_install": [
                    "python=" + natsorted(python_versions)[-1],
                    "numpy",
                    "traits"],
                "pip_install": packages}])

        if labels:
            instructions.append(["label", labels])

        neurodocker_specs = {
            "pkg_manager": "apt",
            "instructions": instructions}

        dockerfile = nd.Dockerfile(neurodocker_specs).render()

        # Save generated dockerfile to file
        out_file = build_dir / 'Dockerfile'
        out_file.parent.mkdir(exist_ok=True, parents=True)
        with open(str(out_file), 'w') as f:
            f.write(dockerfile)
        logger.info("Dockerfile generated at %s", out_file)

        return dockerfile, build_dir

    @classmethod
    def generate_json_config(cls, pipeline_name, pydra_task, image_tag,
                            inputs, outputs, description, version,
                            parameters=None, frequency=Clinical.session,
                            registry=DOCKER_HUB, info_url=None, debug_output=False):
        """Constructs the XNAT CS "command" JSON config, which specifies how XNAT
        should handle the containerised pipeline

        Parameters
        ----------
        pipeline_name : str
            Name of the pipeline
        pydra_task : Task or Workflow
            The task or workflow to be wrapped for XNAT CS use
        image_tag : str
            Name + version of the Docker image to be created
        inputs : list[InputArg]
            Inputs to be provided to the container
        outputs : list[OutputArg]
            Outputs from the container 
        description : str
            User-facing description of the pipeline
        version : str
            Version string for the wrapped pipeline
        parameters : list[str]
            Parameters to be exposed in the CS command    
        frequency : Clinical
            Frequency of the pipeline to generate (can be either 'dataset' or 'session' currently)
        registry : str
            URI of the Docker registry to upload the image to

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

        # Convert tuples to appropriate dataclasses for inputs and outputs
        inputs = [cls.InputArg(*i) for i in inputs if isinstance(i, tuple)]
        outputs = [cls.OutputArg(*o) for o in outputs if isinstance(o, tuple)]

        field_specs = dict(pydra_task.input_spec.fields)

        # JSON to define all inputs and parameters to the pipelines
        inputs_json = []

        # Add task inputs to inputs JSON specification
        input_args = []
        for inpt in inputs:
            spec = field_specs[inpt.name]
            
            desc = spec.metadata.get('help_string', '')
            if spec.type in (str, Path):
                desc = (f"Scan match: {desc} "
                        "[SCAN_TYPE [ORDER [TAG=VALUE, ...]]]")
                input_type = 'string'
            else:
                desc = f"Field match ({spec.type}): {desc} [FIELD_NAME]"
                input_type = cls.COMMAND_INPUT_TYPES[spec.type]
            inputs_json.append({
                "name": inpt.name,
                "description": desc,
                "type": input_type,
                "default-value": "",
                "required": True,
                "user-settable": True,
                "replacement-key": "[{}_INPUT]".format(inpt.name.upper())})
            input_args.append(
                f'--input {inpt.name} [{inpt.name.upper()}_INPUT]')

        # Add parameters as additional inputs to inputs JSON specification
        param_args = []
        for param in parameters:
            spec = field_specs[param]
            desc = "Parameter: " + spec.metadata.get('help_string', '')
            required = spec._default is NOTHING

            inputs_json.append({
                "name": param,
                "description": desc,
                "type": cls.COMMAND_INPUT_TYPES[spec.type],
                "default-value": (spec._default if not required else ""),
                "required": required,
                "user-settable": True,
                "replacement-key": "[{}_PARAM]".format(param.upper())})
            param_args.append(
                f'--parameter {param} [{param.upper()}_PARAM]')

        # Set up output handlers and arguments
        outputs_json = []
        output_handlers = []
        output_args = []
        for output in outputs:

            output_fname = cls.escape_name(output.name) + output.datatype.extension
            # Set the path to the 
            outputs_json.append({
                "name": output.name,
                "description": f"{output.name} ({output.datatype})",
                "required": True,
                "mount": "out",
                "path": output_fname,
                "glob": None})
            output_handlers.append({
                "name": f"{output.name}-resource",
                "accepts-command-output": output.name,
                "via-wrapup-command": None,
                "as-a-child-of": "SESSION",
                "type": "Resource",
                "label": output.name,
                "format": None})
            output_args.append(
                f'--output {output.name} {cls.OUTPUT_MOUNT}:{output.datatype.name}')

        # Save work directory as session resource if debugging
        if debug_output:  
            outputs_json.append({
                    "name": "work",
                    "description": "Working directory",
                    "required": True,
                    "mount": "work",
                    "path": None,
                    "glob": None})
            output_handlers.append({
                    "name": "work-resource",
                    "accepts-command-output": "work",
                    "via-wrapup-command": None,
                    "as-a-child-of": "SESSION",
                    "type": "Resource",
                    "label": "__work__",
                    "format": None})

        input_args_str = ' '.join(input_args)
        output_args_str = ' '.join(output_args)
        param_args_str = ' '.join(param_args)

        # Unpickle function to get its name
        func = cp.loads(pydra_task.inputs._func)

        cmdline = (
            f"conda run --no-capture-output -n arcana "  # activate conda
            f"arcana run {func.__module__}.{func.__name__} "  # run pydra task in Arcana
            f"[PROJECT_ID] {input_args_str} {output_args_str} {param_args_str} " # inputs, outputs + params
            f"--work {cls.WORK_MOUNT} "  # working directory
            f"--repository xnat_via_cs $XNAT_HOST $XNAT_USER $XNAT_PASS {frequency}")  # pass XNAT API details

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
        json_config = {
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
                {
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
            json_config['info-url'] = info_url

        return json_config


    @dataclass
    class InputArg():
        name: str
        datatype: FileFormat
        frequency: DataSpace

    @dataclass
    class OutputArg():
        name: str
        datatype: FileFormat

    COMMAND_INPUT_TYPES = {
        bool: 'bool',
        str: 'string',
        int: 'number',
        float: 'number'}

    VALID_FREQUENCIES = (Clinical.session, Clinical.dataset)