import json
from arcana.data.formats.medimage import NiftiGzX, NiftiGzXFslgrad
from logging import getLogger


logger = getLogger('arcana')


def test_dicom_to_nifti_conversion(dummy_t1w_dicom):

    nifti_gz_x = dummy_t1w_dicom.convert_to(NiftiGzX)

    with open(nifti_gz_x.side_car('json')) as f:
        js = json.load(f)

    assert js['EchoTime'] == 0.00207


def test_dicom_to_nifti_conversion_and_echo(dummy_magfmap_dicom):

    nifti_gz_x_e1 = dummy_magfmap_dicom.convert_to(NiftiGzX, echo=1)
    nifti_gz_x_e2 = dummy_magfmap_dicom.convert_to(NiftiGzX, echo=2)
    assert nifti_gz_x_e1.get_header()['EchoNumber'] == 1
    assert nifti_gz_x_e2.get_header()['EchoNumber'] == 2


def test_dicom_to_nifti_conversion_with_jq_edit(dummy_t1w_dicom):

    nifti_gz_x = dummy_t1w_dicom.convert_to(NiftiGzX,
                                            side_car_jq='.EchoTime *= 1000')

    with open(nifti_gz_x.side_car('json')) as f:
        js = json.load(f)

    assert js['EchoTime'] == 2.07



def test_dicom_to_niftix_fslgrad_conversion(dummy_dwi_dicom):

    logger.debug('Performing FSL grad conversion')

    nifti_gz_x_fsgrad = dummy_dwi_dicom.convert_to(NiftiGzXFslgrad)

    with open(nifti_gz_x_fsgrad.side_car('bval')) as f:
        bvals = [float(b) for b in f.read().split()]

    with open(nifti_gz_x_fsgrad.side_car('bvec')) as f:
        bvec_lines = f.read().split('\n')

    bvecs = zip(*([float(v) for v in l.split()] for l in bvec_lines if l))
    bvec_mags = [(v[0] ** 2 + v[1] ** 2 + v[2] ** 2) for v in bvecs
                 if any(v)]

    assert all(b in (0.0, 3000.0) for b in bvals)
    assert len(bvec_mags) == 60
    assert all(abs(1 - m) < 1e5 for m in bvec_mags)
