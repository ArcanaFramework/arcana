import json
from arcana.data.formats.medimage import NiftiGzX


def test_dicom_to_nifti_conversion(dummy_t1w_dicom):

    nifti_gz_x = dummy_t1w_dicom.convert_to(NiftiGzX)

    with open(nifti_gz_x.side_car('json')) as f:
        js = json.load(f)

    assert js['EchoTime'] == 0.00207


def test_dicom_to_nifti_conversion_and_extract_volume(dummy_magfmap_dicom):

    nifti_gz_x = dummy_magfmap_dicom.convert_to(
        NiftiGzX, extract_volume=1)

    assert nifti_gz_x.header()['dim'][2] == 1


def test_dicom_to_nifti_conversion_with_jq_edit(dummy_t1w_dicom):

    nifti_gz_x = dummy_t1w_dicom.convert_to(
        NiftiGzX, side_car_jq='.EchoTime *= 1000')

    with open(nifti_gz_x.side_car('json')) as f:
        js = json.load(f)

    assert js['EchoTime'] == 2.07