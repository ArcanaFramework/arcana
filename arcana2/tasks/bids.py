from typing import Dict
from pydra import mark


@mark.task
@mark.annotate({'path': str})
def construct_bids(input_paths: Dict[str, str]):
    pass


@mark.task
def extract_bids(path: str, output_paths: Dict[str, str]):
    pass
