from .xnat import (
    nifti_sample_dir,
    xnat_dataset,
    mutable_xnat_dataset,
    saved_dataset_multi_store,
    xnat_root_dir,
    xnat_archive_dir,
    xnat_repository,
    xnat_respository_uri,
    docker_registry_for_xnat,
    docker_registry_for_xnat_uri,
    dummy_niftix,
)
from .dicom import dummy_t1w_dicom
from .dicom import dummy_magfmap_dicom
from .dicom import dummy_dwi_dicom
from .dicom import dummy_mixedfmap_dicom
