from pathlib import Path
import tempfile
from dataclasses import dataclass
from copy import copy
import json
from collections import defaultdict
from argparse import ArgumentParser
import pydicom.dataset
from arcana.data.formats.medimage import Dicom


def generate_test_dicom(path: str, num_vols: int, constant_hdr: dict,
                        collated_data: dict, varying_hdr: dict):
    """Generates a dummy DICOM dataset for a test fixture

    Parameters
    ----------
    path : str
        Path (name) for the generated Dicom object
    num_vols : int
        Number of volumes in the set
    constant_hdr : dict[str, Any]
        constant header values
    collated_data : dict[str, int]
        data array lengths
    varying_hdr : dict[str, list], optional
        varying header values across a multi-volume set

    Returns
    -------
    Dicom
        Dicom dataset
    """

    dicom_dir = Path(tempfile.mkdtemp())

    for i in range(num_vols):

        vol_json = copy(constant_hdr)
        if varying_hdr is not None:
            vol_json.update({k: v[i] for k, v in varying_hdr.items()})
        # Reconstitute large binary fields with dummy data filled with \3 bytes
        vol_json.update({k: {'vr': v[i]['vr'], 'InlineBinary': "X" * v[i]['BinaryLength']}
                         for k, v in collated_data.items()})

        ds = pydicom.dataset.Dataset.from_json(vol_json)
        ds.is_implicit_VR = True
        ds.is_little_endian = True

        ds.save_as(dicom_dir / f"{i + 1}.dcm")

    dcm = Dicom(path)
    dcm.set_fs_paths([dicom_dir])
    return dcm


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
    header = {k: v for k, v in js.items()
              if not v['vr'].startswith('O')}
    # Replace data byte string with its length, so it can be recreated with
    # dummy data when it is loaded
    data = {k: {'vr': v['vr'], 'BinaryLength': len(v['InlineBinary'])} 
            for k, v in js.items()
            if v['vr'].startswith('O')}
    return header, data


def generate_code(dpath: Path, fixture_name: str):
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
    collated_hdr = defaultdict(list)
    collated_data = defaultdict(list)
    num_vols = 0
    for fpath in dpath.iterdir():
        if fpath.name.startswith('.'):
            continue
        header, data = read_dicom(fpath)
        for k, v in header.items():
            collated_hdr[k].append(v)
        for k, v in data.items():            
            collated_data[k].append(v)
        num_vols += 1
    constant_hdr = {k: v[0] for k, v in collated_hdr.items()
                    if all(v[0] == x for x in v)}
    varying_hdr = {k: v for k, v in collated_hdr.items()
                   if k not in constant_hdr}
    return FILE_TEMPLATE.format(
        num_vols=num_vols,
        fixture_name=fixture_name,
        constant_hdr=json.dumps({k: v for k, v in constant_hdr.items()
                                    if not isinstance(v, ByteData)},
                                indent='    '),
        varying_hdr=json.dumps({k: v for k, v in varying_hdr.items()
                                   if not isinstance(v, ByteData)}),
        collated_data=json.dumps({k: v for k, v in collated_data.items()}))


FILE_TEMPLATE = """
import pytest
from arcana.test.fixtures.medimage.dicom.base import generate_test_dicom


@pytest.fixture
def dummy_{fixture_name}_dicom():
    return generate_test_dicom('{fixture_name}', num_vols, constant_hdr, collated_data, varying_hdr)


num_vols = {num_vols}


constant_hdr = {constant_hdr}


varying_hdr = {varying_hdr}


collated_data = {collated_data}


if __name__ == '__main__':
    print(generate_test_dicom('{fixture_name}',num_vols, constant_hdr, collated_data, varying_hdr))
"""

if __name__ == '__main__':
    parser = ArgumentParser(description=(
        "Generates a module containing extracted metadata from a DICOM dataset"
        "in Python dictionaries so that a dummy DICOM dataset with similar "
        "header configuration can be generated in pytest fixtures"))
    parser.add_argument(
        'dicom_dir', help="The directory containing the source dicoms")
    parser.add_argument(
        'fixture_file',
        help="The file to save the extracted header information and byte data in")
    args = parser.parse_args()

    fpath = Path(args.fixture_file)

    with open(fpath, 'w') as f:
        f.write(generate_code(Path(args.dicom_dir), fpath.stem))
