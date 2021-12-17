"""
Helper functions for generating XNAT Container Service compatible Docker
containers
"""
import os
import re
import json
import logging
from copy import copy
from pathlib import Path
import site
import shutil
import tempfile
import pkg_resources
from dataclasses import dataclass
import attr
from attr import NOTHING
from pydra.engine.task import TaskBase
import neurodocker as nd
from natsort import natsorted
from arcana2.__about__ import install_requires, __version__, PACKAGE_NAME
from arcana2.data.dimensions.clinical import Clinical
from arcana2.core.data.type import FileFormat
from arcana2.core.data.dimensions import DataDimensions
from arcana2.core.utils import resolve_class, DOCKER_HUB
from arcana2.exceptions import ArcanaFileFormatError, ArcanaUsageError, ArcanaNoDirectXnatMountException
from arcana2.__about__ import PACKAGE_NAME, python_versions
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
    
    frequency: DataDimensions = attr.ib(default=Clinical.session)
    node_id: str = attr.ib(default=None)
    input_mount: Path = attr.ib(default=INPUT_MOUNT, converter=Path)
    output_mount: Path = attr.ib(default=OUTPUT_MOUNT, converter=Path)
    server: str = attr.ib()
    user: str = attr.ib()
    password: str = attr.ib()


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
                try:
                    primary_path, side_cars = file_group.datatype.assort_files(
                        resource_path.iterdir())
                except ArcanaFileFormatError as e:
                    e.msg += f" in {file_group} from {resource_path}"
                    raise e
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
                          + '/RESOURCES/' + file_group.path)
        logger.info("Put %s into %s:%s node via direct access to archive directory",
                    file_group.path, file_group.data_node.frequency,
                    file_group.data_node.id)

    def get_output_paths(self, file_group):
        path_parts = file_group.path.split('/')
        resource_path = self.output_mount / '/'.join(path_parts[:-1])
        side_car_paths = {}
        if file_group.datatype.directory:
            primary_path = resource_path / path_parts[-1]
        else:
            os.makedirs(resource_path, exist_ok=True)
            # Upload primary file and add to cache
            fname = path_parts[-1] + file_group.datatype.extension
            primary_path = resource_path / fname
            # Upload side cars and add them to cache
            for sc_name, sc_ext in file_group.datatype.side_cars.items():
                sc_fname = path_parts[-1] + sc_ext
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
    def generate_xnat_command(cls,
                             pipeline_name: str,
                             task_location: str,
                             image_tag: str,
                             inputs,
                             outputs,
                             description,
                             version,
                             parameters=None,
                             frequency=Clinical.session,
                             registry=DOCKER_HUB,
                             info_url=None):
        """Constructs the XNAT CS "command" JSON config, which specifies how XNAT
        should handle the containerised pipeline

        Parameters
        ----------
        pipeline_name : str
            Name of the pipeline
        task_location : str
            The module path to the task to execute
        image_tag : str
            Name + version of the Docker image to be created
        inputs : list[InputArg or tuple]
            Inputs to be provided to the container (pydra_field, datatype, dialog_name, FREQUENCY)
        outputs : list[OutputArg or tuple]
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

        pydra_task = resolve_class(task_location)
        if not isinstance(pydra_task, TaskBase):
            pydra_task = pydra_task()

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
                f"--input {inpt.pydra_field} {inpt.datatype} {replacement_key}")

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
            # output_fname = xnat_path
            # if output.datatype.extension is not None:
            #     output_fname += output.datatype.extension
            # Set the path to the 
            outputs_json.append({
                "name": output.pydra_field,
                "description": f"{output.pydra_field} ({output.datatype})",
                "required": True,
                "mount": "out",
                "path": label,
                "glob": None})
            output_handlers.append({
                "name": f"{output.pydra_field}-resource",
                "accepts-command-output": output.pydra_field,
                "via-wrapup-command": None,
                "as-a-child-of": "SESSION",
                "type": "Resource",
                "label": label,
                "format": output.datatype.name})
            output_args.append(
                f'--output {output.pydra_field} {output.datatype} {xnat_path}')

        input_args_str = ' '.join(input_args)
        output_args_str = ' '.join(output_args)
        param_args_str = ' '.join(param_args)

        cmdline = (
            f"conda run --no-capture-output -n arcana "  # activate conda
            f"arcana run {task_location} "  # run pydra task in Arcana
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


    @classmethod
    def generate_dockerfile(cls,
                            xnat_commands: list[dict[str, str or dict or list]],
                            python_packages: list[tuple[str, str]],
                            maintainer: str,
                            build_dir: Path,
                            base_image: str=None,
                            packages: list[list[str, str]]=None,
                            extra_labels: dict[str, str]=None,
                            package_manager: str=None):
        """Constructs a dockerfile that wraps a with dependencies

        Parameters
        ----------
        xnat_commands 
            The command JSON (as generated by `generate_xnat_command`) to insert
            into a label of the docker file.
        maintainer
            The name and email of the developer creating the wrapper (i.e. you)   
        build_dir
            Path to the directory to create the Dockerfile in and copy any local
            files to
        base_image
            The base image to build from
        packages
            Name and version of the Neurodocker requirements to add to the image
        python_packages
            Name and version of the Python PyPI packages to add to the image
        registry
            URI of the Docker registry to upload the image to
        extra_labels : dict[str, str], optional
            Additional labels to be added to the image

        Returns
        -------
        Path
            Path to directory where Dockerfile and related files were generated
        """

        labels = {}
        packages = list(packages)

        if build_dir is None:
            build_dir = tempfile.mkdtemp()
        if packages is None:
            packages = []
        if python_packages is None:
            python_packages = []
        else:
            python_packages = copy(python_packages)
        if base_image is None:
            base_image = "debian:bullseye"
        if package_manager is None:
            package_manager = 'apt'

        if maintainer:
            labels["maintainer"] = maintainer

        # Convert JSON into Docker label
        if xnat_commands is None:
            xnat_commands = []
        elif not isinstance(xnat_commands, list):
            xnat_commands = [xnat_commands]
        labels['org.nrg.commands'] = json.dumps(xnat_commands)
        if extra_labels:
            labels.update(extra_labels)

        instructions = [
            ["base", base_image],
            ["install", ["git", "vim", "ssh-client", "python3", "python3-pip", "dcm2niix"]]]

        for pkg in packages:
            install_props = {}
            if isinstance(pkg, str):
                pkg_name = pkg
                install_props['version'] = 'master'
            else:
                pkg_name = pkg[0]
                if len(pkg) > 1 and pkg[1] != '.':
                    install_props['version'] = pkg[1]
                if len(pkg) > 2:
                    install_props['method'] = pkg[2]   
            instructions.append([pkg_name, install_props])

        site_pkg_locs = [Path(p).resolve() for p in site.getsitepackages()]

        python_packages = copy(python_packages)
        python_packages.append(PACKAGE_NAME)
        python_packages.extend(re.match(r'([a-zA-Z0-9\-]+)', r).group(1)
                               for r in install_requires)

        # Copies the local development copies of Python dependencies into the
        # docker image if present instead of relying on the PyPI version,
        # which might be missing bugfixes local changes
        resolved_python_packages = []
        for pkg_name in python_packages:
            
            pkg = next(p for p in pkg_resources.working_set
                       if p.key == pkg_name)
            pkg_loc = Path(pkg.location).resolve()
            # Determine whether installed version of requirement is locally
            # installed (and therefore needs to be copied into image) or can
            # be just downloaded from PyPI
            if pkg_loc not in site_pkg_locs:
                shutil.rmtree(build_dir / pkg_name, ignore_errors=True)
                gitignore_path = (pkg_loc / '.gitignore')
                if gitignore_path.exists():
                    with open(gitignore_path) as f:
                        gitignore = f.read().splitlines()
                    absolute_paths = [pkg_loc / p[1:] for p in gitignore
                                      if p.startswith('/')]
                    relative_paths = [p for p in gitignore
                                      if not p.startswith('/')]
                    file_ignore = shutil.ignore_patterns(*relative_paths)
                    def ignore(directory, contents):
                        to_ignore = file_ignore(directory, contents)
                        to_ignore.update(
                            c for c in contents
                            if Path(directory) / c in absolute_paths)
                        # Skip files that shouldn't be copied into the build
                        # directory as they mess up test discovery
                        for fname in cls.DONT_COPY_INTO_BUILD:
                            if fname in contents:
                                to_ignore.add(fname)
                        return to_ignore
                else:
                    ignore = shutil.ignore_patterns('*.pyc', '__pycache__')
                shutil.copytree(pkg_loc, build_dir / pkg_name, ignore=ignore)
                pip_address = '/python-packages/' + pkg_name
                instructions.append(['copy', ['./' + pkg_name, pip_address]])
            else:
                direct_url_path = Path(pkg.egg_info) / 'direct_url.json'
                if direct_url_path.exists():
                    with open(direct_url_path) as f:
                        durl = json.load(f)             
                    pip_address = f"{durl['vcs']}+{durl['url']}@{durl['commit_id']}"
                else:
                    pip_address = f"{pkg.key}=={pkg.version}"
            resolved_python_packages.append(pip_address)

        instructions.append(
            ["miniconda", {
                "create_env": "arcana",
                "conda_install": [
                    "python=" + natsorted(python_versions)[-1],
                    "numpy",
                    "traits"],
                "pip_install": resolved_python_packages}])

        if labels:
            instructions.append(["label", labels])

        # Copy command JSON inside dockerfile for ease of reference
        cmds_dir = build_dir / 'xnat_commands'
        cmds_dir.mkdir()
        for cmd in xnat_commands:
            fname = cmd.get('name', 'command') + '.json'
            with open(build_dir / fname, 'w') as f:
                json.dump(cmd, f, indent='    ')
        instructions.append(['copy', ['./xnat_commands', '/xnat_commands']])

        # if description is None:
        #     description = ''
        # else:
        #     description = '\n' + description + '\n'
        # with open(build_dir / 'README.md', 'w') as f:
        #     f.write(cls.DOCKERFILE_README_TEMPLATE.format(
        #         __version__,
        #                                                   description))
        # instructions.append(['copy', ['./README.md', '/README.md']])

        neurodocker_specs = {
            "pkg_manager": package_manager,
            "instructions": instructions}

        dockerfile = nd.Dockerfile(neurodocker_specs).render()

        # Save generated dockerfile to file
        out_file = build_dir / 'Dockerfile'
        out_file.parent.mkdir(exist_ok=True, parents=True)
        with open(str(out_file), 'w') as f:
            f.write(dockerfile)
        logger.info("Dockerfile generated at %s", out_file)

        return build_dir

    @dataclass
    class InputArg():
        pydra_field: str  # Must match the name of the Pydra task input
        datatype: FileFormat
        dialog_name: str = None # The name of the parameter in the XNAT dialog, defaults to the pydra name
        frequency: Clinical = Clinical.session

    @dataclass
    class OutputArg():
        pydra_field: str  # Must match the name of the Pydra task output
        datatype: FileFormat
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

    DONT_COPY_INTO_BUILD = ['conftest.py', 'debug-build', '__pycache__',
                            '.pytest_cache']


    DOCKERFILE_README_TEMPLATE = """
    The following Docker image was generated by arcana v{} to enable the
    {} commands to be run in the XNAT container service.
    {}
    
    """