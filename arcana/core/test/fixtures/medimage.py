import pytest
from arcana.data.formats.medimage import Dicom


@pytest.fixture(scope="session")
def dummy_t1w_dicom():
    import medimages4tests.dummy.dicom.mri.t1w.siemens.skyra.syngo_d13c as module

    dicom_dir = module.get_image()
    dcm = Dicom("t1w")
    dcm.set_fs_paths([dicom_dir])
    return dcm


@pytest.fixture(scope="session")
def dummy_magfmap_dicom():
    import medimages4tests.dummy.dicom.mri.fmap.siemens.skyra.syngo_d13c as module

    dicom_dir = module.get_image()
    dcm = Dicom("magfmap")
    dcm.set_fs_paths([dicom_dir])
    return dcm


@pytest.fixture(scope="session")
def dummy_dwi_dicom():
    import medimages4tests.dummy.dicom.mri.dwi.siemens.skyra.syngo_d13c as module

    dicom_dir = module.get_image()
    dcm = Dicom("dwi")
    dcm.set_fs_paths([dicom_dir])
    return dcm


@pytest.fixture(scope="session")
def dummy_mixedfmap_dicom():
    import medimages4tests.dummy.dicom.mri.fmap.ge.discovery_mr888.dv26_0_r05_2008a as module

    dicom_dir = module.get_image()
    dcm = Dicom("mixedfmap")
    dcm.set_fs_paths([dicom_dir])
    return dcm
