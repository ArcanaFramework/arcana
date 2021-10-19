from arcana2.tasks.bids import construct_bids, extract_bids, bids_app
from .run import RunCmd


class RunBidsAppCmd(RunCmd):

    cmd_name = 'run-bids'
    desc = ("Runs a BIDS app against a dataset stored in a repository.")

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
    def cmd_name(cls, args):
        if args.container:
            name = args.container[1].split('/')[-1]
        else:
            name = args.entrypoint
        return name

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
