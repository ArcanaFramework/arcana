from typing import Dict
from pydra import mark


@mark.task
def construct_bids(input_paths: Dict[str, str]):
    pass


@mark.task
def extract_bids(path: str, output_paths: Dict[str, str]):
    pass

@mark.task
def bids_app(app_name, bids_dir, analysis_level, ids, flags):
    app_args = [analysis_level]
    if ids:
        app_args.append('--participant_label ' + ' '.join(ids))
    app_args.append(flags)
    ShellCommandTask(
        name='app',
        executable=args.entrypoint,
        args=' '.join(args),
        container_info=args.container)
