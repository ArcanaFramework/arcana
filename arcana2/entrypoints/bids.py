import re
from pathlib import Path
import tempfile
from typing import Sequence
import arcana2.data.types
from arcana2.exceptions import ArcanaUsageError
from arcana2.core.data.type import FileFormat
from arcana2.data.repositories
from arcana2.__about__ import __version__
from arcana2.tasks.bids import construct_bids, extract_bids, bids_app
from arcana2.core.utils import resolve_class, resolve_datatype, list_instances
from .run import RunCmd
from .wrap4xnat import Wrap4XnatCmd


class RunBidsAppCmd(RunCmd):

    cmd_name = 'run-bids'
    desc = ("Runs a BIDS app against a dataset stored in a repository.")


    @classmethod
    def construct_parser(cls, parser):
        cls.construct_app_parser(parser)
        super().construct_parser(parser)
        cls.construct_io_parser(parser)
        parser.add_argument(
            '--ids', nargs='+', default=None,
            help=("IDs of the nodes to process (i.e. for the frequency that "
                  "the app runs at)."))
        parser.add_argument(
            '--container', nargs=2, default=None,
            metavar=('ENGINE', 'IMAGE'),
            help=("The container engine ('docker'|'singularity') and the image"
                  " to run the app in"))
        parser.add_argument(
            '--work', '-w', default=None,
            help=("The location of the directory where the working files "
                  "created during the pipeline execution will be stored"))
        parser.add_argument(
            '--pydra_plugin', default='cf',
            help=("The Pydra plugin with which to process the workflow"))
        parser.add_argument(
            '--dry_run', action='store_true', default=False,
            help=("Set up the workflow to test inputs but don't run the app"))

    @classmethod
    def construct_io_parser(cls, parser):
        parser.add_argument(
            '--input', '-i', action='append', default=[], nargs='+',
            help=cls.INPUT_HELP.format(var_desc=cls.VAR_DESC))
        parser.add_argument(
            '--output', '-o', action='append', default=[], nargs=3,
            metavar=('VAR', 'PRODUCED_DTYPE', 'STORE_AT'),
            help=cls.OUTPUT_HELP.format(var_desc=cls.VAR_DESC))

    @classmethod
    def construct_app_parser(cls, parser):
        parser.add_argument(
            'app',
            help=("The path to a Pydra interface that wraps the app "
                  "convenience the 'pydra.tasks' prefix can be omitted "
                  "(e.g. fsl.preprocess.first.First)"))        
        parser.add_argument(
            '--frequency', '-f', default='session',
            help=("The level at which the analysis is performed. One of (per) "
                  "dataset, group, subject, timepoint, group_timepoint or "
                  "session"))
        parser.add_argument(
            '--parameter', '-p', metavar=('NAME', 'VAL'),
            action='append', default=[], nargs=2,
            help=("Parameter to pass to the app"))

    @classmethod
    def run(cls, args):

        work_dir = Path(args.work) if args.work is not None else Path(tempfile.mkdtemp())

        dataset = cls.get_dataset(args, work_dir)
        inputs = cls.add_input_sources(args, dataset)
        outputs = cls.add_output_sinks(args, dataset)
        frequency = cls.parse_frequency(args)

        pipeline = dataset.new_pipeline(
            name=cls.workflow_name(args),
            inputs=inputs,
            outputs=outputs,
            frequency=frequency)

        cls.construct_pipeline(args, pipeline)

        if not args.dry_run:
            pipeline(ids=args.ids, plugin=args.pydra_plugin)  # , cache_locations=work_dir / PYDRA_CACHE)

        return pipeline

    @classmethod
    def parse_input_args(cls, args):
        for i, inpt in enumerate(args.input):
            nargs = len(inpt)
            if nargs > cls.MAX_INPUT_ARGS:
                raise ArcanaUsageError(
                    f"Input {i} has too many input args, {nargs} instead "
                    f"of max {cls.MAX_INPUT_ARGS} ({inpt})")
            (var, input_datatype_name, pattern, order,
             quality, metadata, freq) = [
                a if a != '*' else None for a in (
                    inpt + [None] * (cls.MAX_INPUT_ARGS - len(inpt)))]
            if not var:
                raise ArcanaUsageError(
                    f"{cls.VAR_ARG} must be provided for input {i} ({inpt})")
            if not input_datatype_name:
                raise ArcanaUsageError(
                    f"An input datatype to match must be provided for input {i} ({inpt})")
            if not pattern:
                raise ArcanaUsageError(
                    f"A path pattern to match must be provided for input {i} ({inpt})")
            input_datatype = resolve_datatype(input_datatype_name)
            pattern, stored_datatype = cls._datatype_from_path(pattern,
                                                               input_datatype)
                
            yield InputArg(var, pattern, input_datatype, stored_datatype, freq,
                           order, metadata, True, quality)

    @classmethod
    def parse_output_args(cls, args):
        for output in args.output:
            var, output_datatype, store_at = output
            # When interface outputs include file format this argument won't
            # be necessary
            output_datatype = resolve_datatype(output_datatype)
            store_at, stored_datatype = cls._datatype_from_path(store_at,
                                                                output_datatype)
            yield OutputArg(name=var, path=store_at,
                            output_datatype=output_datatype,
                            stored_datatype=stored_datatype)

    @classmethod
    def _datatype_from_path(cls, path, default):
        datatype = None
        if ':' in path:
            path, datatype_name = str(path).split(':')
            datatype = resolve_datatype(datatype_name.lower())
        elif '.' in path:
            path = Path(path)
            path_ext = '.'.join(path.suffixes)
            # Strip suffix from path
            path = path.parent / path.stem
            # FIXME: Need a more robust way of determining datatype
            # from output path extension
            for dtype in list_instances(arcana2.data.types, FileFormat):
                if dtype.extension == path_ext:
                    datatype = dtype
                    break
        if datatype is None:
            datatype = default
        return path, datatype

    @classmethod
    def add_input_sources(cls, args, dataset):
        """Parses input arguments into dictionary of DataSources

        Parameters
        ----------
        args : ArgumentParser.namespace
            The parsed arguments from a ArgumentParser.parse_args() method

        Returns
        -------
        list[tuple[str, FileFormat]]
            A sequence of input names and their required formats

        Raises
        ------
        ArcanaUsageError
            If too many arguments are provided to the `--input` arg (max 7)
        ArcanaUsageError
            If the VAR_ARG is not provided to the input
        ArcanaUsageError
            If a path pattern is not provided
        ArcanaUsageError
            If a file_format is not provided
        """
        # Create file-group matchers
        inputs = []
        for arg in cls.parse_input_args(args):
            dataset.add_source(
                name=arg.name,
                path=arg.path,
                datatype=arg.stored_datatype,
                frequency=arg.frequency,
                order=arg.order,
                metadata=arg.metadata,
                is_regex=arg.is_regex,
                quality_threshold=arg.quality_threshold)
            inputs.append((arg.name, arg.input_datatype))
        return inputs

    @classmethod
    def add_output_sinks(cls, args, dataset):
        """Parses output arguments into dictionary of DataSinks

        Parameters
        ----------
        args : ArgumentParser.namespace
            The parsed arguments from a ArgumentParser.parse_args() method

        Returns
        -------
        list[tuple[str, FileFormat]]
            A sequence of input names and the formats they are produced in
        """
        frequency = cls.parse_frequency(args)
        # Create outputs
        outputs = []
        for arg in cls.parse_output_args(args):
            dataset.add_sink(
                name=arg.name,
                path=arg.path,
                datatype=arg.stored_datatype,
                frequency=frequency)
            outputs.append((arg.name, arg.output_datatype))
        return outputs

    @classmethod
    def construct_pipeline(cls, args, pipeline):
        
        task_cls = resolve_class(args.app, prefixes=['pydra.tasks'])

        # Add the app task
        pipeline.add(task_cls(name='app',
                              **cls.parse_parameters(args, task_cls)))

        # Connect source to inputs
        for input in pipeline.input_names:
            setattr(pipeline.app.inputs, input,
                    getattr(pipeline.wf.per_node.source.lzout, input))

        # Connect outputs to sink
        for output in pipeline.output_names:
            pipeline.set_output((output, getattr(pipeline.app.lzout, output)))

        return pipeline

    @classmethod
    def parse_parameters(cls, args, task_cls):
        """Parses the args to be passed to the Pydra task, converting to the
        right types where required.

        Parameters
        ----------
        args : [type]
            The cmd args (as parsed by ArgumentParser)
        task_cls : [type]
            The Pydra task class

        Returns
        -------
        Dict[str, Any]
            Arg names and their values to pass to the app
        """
        app_args = {}
        task = task_cls()
        for name, val in args.parameter:
            try:
                arg_spec = next(s for s in task.input_spec.fields
                                if s[0] == name)
            except StopIteration:
                raise ArcanaUsageError(
                    f"Unrecognised argument '{name}' passed to '--arg' flag. "
                    "Expecting one of '{}'".format(
                        "', '".join(f[0] for f in task.input_spec.fields
                                    if not f[0].startswith('_'))))
            arg_type = arg_spec[1].type
            if arg_type is not str and issubclass(arg_type, Sequence):
                if len(arg_type.__args__) == 1:
                    sub_type = arg_type.__args__[0]
                else:
                    sub_type = str
                val = [sub_type(v) for v in re.split(r'[ ,;]+', val)]
            else:
                try:
                    val = arg_type(val)
                except TypeError:
                    raise ArcanaUsageError(
                        f"Value supplied to '{name}' field in {task.name} "
                        f"cannot be converted to type {arg_spec[1]}") 
            app_args[name] = val
        return app_args

    VAR_ARG = 'INTERFACE_NAME'

    VAR_DESC = f"""
        The {VAR_ARG} is the attribute in the Pydra interface to connect
        the input to.
    """

    @classmethod
    def parse_frequency(cls, args):
        return cls.parse_dataspace(args)[args.frequency]

    @classmethod
    def workflow_name(cls, args):
        return args.app.replace('.', '_')

    @classmethod
    def construct_parser(cls, parser):
        parser.add_argument(
            'entrypoint',
            help=("The entrypoint of the BIDS app"))
        parser.add_argument(
            '--analysis_level', default='participant',
            help=("The level at which the analysis is performed. Either "
                  "'participant' or 'group'"))
        parser.add_argument(
            '--flags', '-f', default='',
            help=("Arbitrary flags to pass onto the BIDS app (enclose in "
                  "quotation marks)"))
        super().construct_parser(parser)

    @classmethod
    def construct_pipeline(cls, args, pipeline):

        pipeline.add(
            construct_bids(
                name='construct_bids',
                input_names=pipeline.input_names))

        pipeline.add(
            bids_app(
                name='app',
                cmd_name=args.app,
                bids_dir=pipeline.construct_bids.lzout.bids_dir,
                analysis_level=args.analysis_level,
                ids=args.ids,
                flags=args.flags))

        pipeline.add(
            extract_bids(
                name='extract_bids',
                bids_dir=pipeline.app.lzout.bids_dir,
                outputs=pipeline.output_names))

        pipeline.set_output()

    @classmethod
    def workflow_name(cls, args):
        return args.container.replace('/', '_')

    @classmethod
    def parse_frequency(cls, args):
        return 'session' if args.analysis_level == 'participant' else 'group'


    VAR_ARG = 'BIDS_PATH'

    VAR_DESC = f"""
        The {VAR_ARG} is the path the that the file/field should be
        located within the constructed BIDS dataset with the file extension
        and subject and session sub-dirs entities omitted, e.g:

            anat/T1w

        for Session 1 of Subject 1 would be placed at the path
            
            sub-01/ses-01/anat/sub-01_ses-01_T1w.nii.gz

        Field datatypes should also specify where they are stored in the
        corresponding JSON side-cars using JSON path syntax, e.g.

            anat/T1w$ImageOrientationPatientDICOM[1]

        will be stored as the second item in the
        'ImageOrientationPatientDICOM' array in the JSON side car at

            sub-01/ses-01/anat/sub-01_ses-01_T1w.json"""


class Wrap4XnatBidsCmd(Wrap4XnatCmd):

    
    cmd_name = 'wrap4xnat-bids'

    desc = ("Create a containerised pipeline from a given set of inputs to "
            "generate specified derivatives")

    @classmethod
    def construct_parser(cls, parser):
        parser.add_argument('interface',
                            help=("The location (on Python path) of the Pydra "
                                  "interface to wrap"))
        parser.add_argument('image_name', metavar='IMAGE',
                            help=("The name of the Docker image, preceded by "
                                  "the registry it will be stored"))
        parser.add_argument('--input', '-i', action='append', default=[],
                            nargs='+',
                            help="Inputs to be used by the app (NAME DATATYPE [FREQUENCY])")
        parser.add_argument('--output', '-o', action='append', default=[],
                            nargs=2, metavar=('NAME', 'DATATYPE'),
                            help="Outputs of the app to stored back in XNAT")
        parser.add_argument('--name', '-n', type=str, default=None,
                            help="A name for the pipeline")
        parser.add_argument('--parameter', '-p', metavar='NAME', action='append',
                            help=("Fixed parameters of the Pydra workflow to "
                                  "expose to the container service"))
        parser.add_argument('--requirement', '-r', nargs='+', action='append',
                            help=("Software requirements to be added to the "
                                  "the docker image using Neurodocker. "
                                  "Neurodocker requirement name, followed by "
                                  "optional version and installation "
                                  "method args (see Neurodocker docs). Use "
                                  "'.' to skip version arg and use the latest "
                                  "available"))
        parser.add_argument('--package', '-k', action='append',
                            help="PyPI packages to be installed in the env")
        parser.add_argument('--frequency', default='clinical.Clinical.session',
                            help=("Whether the resultant container runs "
                                  "against a session or a whole dataset "
                                  "(i.e. project). Can be one of either "
                                  "'session' or 'dataset'"))
        parser.add_argument('--registry', default=DOCKER_HUB,
                            help="The registry the image will be installed in")
        parser.add_argument('--build_dir', default=None,
                            help="The directory to build the dockerfile in")
        parser.add_argument('--maintainer', '-m', type=str, default=None,
                            help="Maintainer of the pipeline")
        parser.add_argument('--description', '-d', default=None,
                            help="A description of what the pipeline does")
        parser.add_argument('--build', default=False, action='store_true',
                            help=("Build the generated Dockerfile"))
        parser.add_argument('--install', default=False, action='store_true',
                            help=("Install the built docker image in the "
                                  "specified registry (implies '--build')"))

    @classmethod
    def run(cls, args):

        frequency = cls.parse_frequency(args)
        
        inputs = cls.parse_input_args(args, frequency)
        outputs = cls.parse_output_args(args)

        extra_labels = {'arcana-wrap4xnat-cmd': ' '.join(sys.argv)}
        pydra_task = cls.parse_interface(args)

        
        build_dir = Path(tempfile.mkdtemp()
                         if args.build_dir is None else args.build_dir)

        image_name = cls.parse_image_name(args)

        pipeline_name = args.name if args.name else pydra_task.name

        # Generate "command JSON" to embed in container to let XNAT know how
        # to run the pipeline
        json_config = XnatViaCS.generate_json_config(
            pipeline_name,
            pydra_task,
            inputs=inputs,
            outputs=outputs,
            description=args.description,
            parameters=args.parameter,
            frequency=frequency,
            registry=args.registry)

        # Generate dockerfile
        dockerfile = XnatViaCS.generate_dockerfile(
            pydra_task, json_config, image_name, args.maintainer,
            args.requirement, args.package, build_dir=build_dir,
            extra_labels=extra_labels)

        if args.build or args.install:
            cls.build(image_name, build_dir=build_dir)

        if args.install:
            cls.install(dockerfile, image_name, args.registry,
                        build_dir=build_dir)
        else:
            return dockerfile

    @classmethod
    def parse_interface(cls, args):
        return resolve_class(args.interface)()

    @classmethod
    def parse_frequency(cls, args):
        return Clinical[args.frequency]

    @classmethod
    def parse_image_name(cls, args):
        return (args.image_name + ':latest' if ':' not in args.image_name
                else args.image_name)

    @classmethod
    def build(cls, image_tag, build_dir):

        dc = docker.from_env()

        logger.info("Building image in %s", str(build_dir))

        dc.images.build(path=str(build_dir), tag=image_tag)        

    @classmethod
    def install(cls, image_tag, registry, build_dir):
        # Build and upload docker image

        dc = docker.from_env()

        image_path = f'{registry}/{image_tag}'
        
        logger.info("Uploading %s image to %s", image_tag, registry)

        dc.images.push(image_path)

    @classmethod
    def parse_input_args(cls, args, default_frequency):
        for inpt in args.input:
            name, required_datatype_name = inpt[:2]
            frequency = inpt[2] if len(inpt) > 2 else default_frequency
            required_datatype = resolve_datatype(required_datatype_name)
            yield XnatViaCS.InputArg(name, required_datatype, frequency)

    @classmethod
    def parse_output_args(cls, args):
        for output in args.output:
            name, datatype_name_name = output
            produced_datatype = resolve_datatype(datatype_name_name)
            yield XnatViaCS.OutputArg(name, produced_datatype)

