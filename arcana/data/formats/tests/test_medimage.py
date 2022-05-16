from pathlib import Path
from arcana.data.formats.medimage import Dicom, NiftiGzX


def test_dicom_to_nifti_conversion(test_dicom_dataset_dir):

    dicom_dir = Path('/Users/tclose/.xnat4tests/xnat_root/archive/20220517081626mriqc/arc001/TESTSUBJ_01/SCANS/anat__l__T1w/DICOM')

    Dicom('anat/T1w').set_fs_paths([dicom_dir])

    # converter, output_lfs = NiftiGzX.find_converter(Dicom)(fs_path=test_dicom_dataset_dir)

    # result = converter()

    # print(converter)