from pathlib import Path
from dataclasses import dataclass
import json
from collections import defaultdict
from argparse import ArgumentParser
import pydicom

@dataclass
class ByteData():

    length: int
    

def read_dicom(fpath: Path):
    """Reads a DICOM file and returns as dictionary stripped from large binary
    fields

    Parameters
    ----------
    path : Path
        File system path to dicom file

    Returns
    -------
    dict[str, Any]
        Dicom fields and their values. Binrary data fields and the length of
        the binary string they hold
    """
    dcm = pydicom.dcmread(str(fpath))
    js = dcm.to_json_dict()
    stripped = {k: (ByteData(v) if v['vr'].startswith('O') else v)
              for k, v in js.items()}
    return stripped


def collate_fields(dpath: Path):
    """Return 

    Parameters
    ----------
    dpath : Path
        Path to the directory holding the DICOM files

    Returns
    -------
    _type_
        _description_
    """
    collated = defaultdict(list)
    for fpath in dpath.iterdir():
        dcm = read_dicom(fpath)
        for k, v in dcm.items():
            collated[k].append(v)
    constant = {k: v[0] for k, v in collated.items()
                if len(set(json.dumps(a) for a in v)) == 1}
    varying = {k: v for k, v in collated if k not in constant}
    return constant, varying


FILE_TEMPLATE = """

constant = {constant}

varying = {}

"""


parser = ArgumentParser()
parser.add_argument(
    'dicom_dir', help="The directory containing the source dicoms")
parser.add_argument(
    'fixture_file',
    help="The file to save the extracted header information and byte data in")


if __name__ == '__main__':
    args = parser.parse_args()

    constant, varying = collate_fields(args.dicom_dir)

    with open(args.fixture_file, 'w') as f:
        f.write(FILE_TEMPLATE.format(
            constant=json.dumps(constant, indent='    '),
            varying=varying))
