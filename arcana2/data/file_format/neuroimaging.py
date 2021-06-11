from copy import deepcopy, copy
from abc import ABCMeta, abstractmethod
import os
import os.path as op
import json
import pydicom
import numpy as np
from arcana.data.file_format import FileFormat, Converter
from banana.interfaces.mrtrix import MRConvert
from banana.requirement import (
    dcm2niix_req, mrtrix_req, matlab_req)
from banana.interfaces.converters import Dcm2niix, TwixReader
from banana.exceptions import BananaUsageError
import nibabel
# Import base file formats from Arcana for convenience
from arcana.data.file_format import (
    text_format, directory_format, zip_format, targz_format,
    png_format, jpg_format, gif_format, json_format,
    UnzipConverter, UnTarGzConverter, IdentityConverter)


class Dcm2niixConverter(Converter):

    interface = Dcm2niix(compression='y')
    input = 'input_dir'
    output = 'converted'
    requirements = [dcm2niix_req.v('1.0.20190720')]


class MrtrixConverter(Converter):

    input = 'in_file'
    output = 'out_file'
    requirements = [mrtrix_req.v(3)]

    @property
    def interface(self):
        return MRConvert(
            out_ext=self.output_format.extension,
            quiet=True)


class TwixConverter(Converter):

    input = 'in_file'
    output = 'out_file'
    output_aux_files = {'ref': 'ref_file', 'json': 'hdr_file'}
    requirements = [matlab_req.v('R2018a')]
    interface = TwixReader()





# =====================================================================
# Custom loader functions for different image types
# =====================================================================


class ImageFormat(FileFormat, metaclass=ABCMeta):

    INCLUDE_HDR_KEYS = None
    IGNORE_HDR_KEYS = None

    @abstractmethod
    def get_header(self, fileset):
        """
        Returns array data associated with the given path for the
        file format
        """

    @abstractmethod
    def get_array(self, fileset):
        """
        Returns header data associated with the given path for the
        file format
        """

    def contents_equal(self, fileset, other_fileset, rms_tol=None, **kwargs):
        """
        Test whether the (relevant) contents of two image filesets are equal
        given specific criteria

        Parameters
        ----------
        fileset : Fileset
            One of the two filesets to compare
        other_fileset : Fileset
            The other fileset to compare
        rms_tol : float
            The root-mean-square tolerance that is acceptable between the array
            data for the images to be considered equal
        """
        if other_fileset.format != self:
            return False
        if self.headers_diff(fileset, other_fileset, **kwargs):
            return False
        if rms_tol:
            rms_diff = self.rms_diff(fileset, other_fileset)
            return (rms_diff < rms_tol)
        else:
            return np.array_equiv(fileset.get_array(),
                                  other_fileset.get_array())

    def headers_diff(self, fileset, other_fileset, include_keys=None,
                     ignore_keys=None, **kwargs):
        """
        Check headers to see if all values
        """
        diff = []
        hdr = fileset.get_header()
        hdr_keys = set(hdr.keys())
        other_hdr = other_fileset.get_header()
        if include_keys is not None:
            if ignore_keys is not None:
                raise BananaUsageError(
                    "Doesn't make sense to provide both 'include_keys' ({}) "
                    "and ignore_keys ({}) to headers_equal method"
                    .format(include_keys, ignore_keys))
            include_keys &= hdr_keys
        elif ignore_keys is not None:
            include_keys = hdr_keys - set(ignore_keys)
        else:
            if self.INCLUDE_HDR_KEYS is not None:
                if self.IGNORE_HDR_KEYS is not None:
                    raise BananaUsageError(
                        "Doesn't make sense to have both 'INCLUDE_HDR_FIELDS'"
                        "and 'IGNORE_HDR_FIELDS' class attributes of class {}"
                        .format(type(self).__name__))
                include_keys = self.INCLUDE_HDR_KEYS  # noqa pylint: disable=no-member
            elif self.IGNORE_HDR_KEYS is not None:
                include_keys = hdr_keys - set(self.IGNORE_HDR_KEYS)
            else:
                include_keys = hdr_keys
        for key in include_keys:
            value = hdr[key]
            try:
                other_value = other_hdr[key]
            except KeyError:
                diff.append(key)
            else:
                if isinstance(value, np.ndarray):
                    if not isinstance(other_value, np.ndarray):
                        diff.append(key)
                    else:
                        try:
                            if not np.allclose(value, other_value,
                                               equal_nan=True):
                                diff.append(key)
                        except TypeError:
                            # Fallback to a straight comparison for some dtypes
                            if value != other_value:
                                diff.append(key)
                elif value != other_value:
                    diff.append(key)
        return diff

    def rms_diff(self, fileset, other_fileset):
        """
        Return the RMS difference between the image arrays
        """
        return np.sqrt(np.sum((fileset.get_array()
                               - other_fileset.get_array()) ** 2))


class NiftiFormat(ImageFormat):

    def get_header(self, fileset):
        return dict(nibabel.load(fileset.path).header)

    def get_array(self, fileset):
        return nibabel.load(fileset.path).get_data()

    def get_vox_sizes(self, fileset):
        # FIXME: This won't work for 4-D files
        return self.get_header(fileset)['pixdim'][1:4]

    def get_dims(self, fileset):
        # FIXME: This won't work for 4-D files
        return self.get_header(fileset)['dim'][1:4]


class NiftixFormat(NiftiFormat):

    def get_header(self, fileset):
        hdr = super().get_header(fileset)
        with open(fileset.aux_file('json')) as f:
            hdr.update(json.load(f))
        return hdr


class DicomFormat(ImageFormat):

    SERIES_NUMBER_TAG = ('0020', '0011')

    def dcm_files(self, fileset):
        return [f for f in os.listdir(fileset.path) if f.endswith('.dcm')]

    def get_array(self, fileset):
        image_stack = []
        for fname in self.dcm_files(fileset):
            image_stack.append(
                pydicom.dcmread(op.join(fileset.path, fname)).pixel_array)
        return np.asarray(image_stack)

    def get_header(self, fileset, index=0):
        dcm_files = [f for f in os.listdir(fileset.path) if f.endswith('.dcm')]
        # TODO: Probably should collate fields that vary across the set of
        #       files in the set into lists
        return pydicom.dcmread(op.join(fileset.path, dcm_files[index]))

    def get_vox_sizes(self, fileset):
        hdr = self.get_header(fileset)
        return np.array(hdr.PixelSpacing + [hdr.SliceThickness])

    def get_dims(self, fileset):
        hdr = self.get_header(fileset)
        return np.array((hdr.Rows, hdr.Columns, len(self.dcm_files(fileset))),
                        dtype=int)

    def extract_id(self, fileset):
        return int(fileset.dicom_values([self.SERIES_NUMBER_TAG])[0])

    def dicom_values(self, fileset, tags):
        """
        Returns a dictionary with the DICOM header fields corresponding
        to the given tag names

        Parameters
        ----------
        tags : List[Tuple[str, str]]
            List of DICOM tag values as 2-tuple of strings, e.g.
            [('0080', '0020')]
        repository_login : <repository-login-object>
            A login object for the repository to avoid having to relogin
            for every dicom_header call.

        Returns
        -------
        dct : Dict[Tuple[str, str], str|int|float]
        """
        try:
            if (fileset._path is None and fileset._dataset is not None
                    and hasattr(fileset.dataset.repository, 'dicom_header')):
                hdr = fileset.dataset.repository.dicom_header(self)
                dct = [hdr[t] for t in tags]
            else:
                # Get the DICOM object for the first file in the fileset
                dcm = fileset.get_header(0)
                dct = [dcm[t].value for t in tags]
        except KeyError as e:
            e.msg = ("{} does not have dicom tag {}".format(
                     self, str(e)))
            raise e
        return dct


class MrtrixImageFormat(ImageFormat):

    def _load_header_and_array(self, fileset):
        with open(fileset.path, 'rb') as f:
            contents = f.read()
        hdr_end = contents.find(b'\nEND\n')
        hdr_contents = contents[:hdr_end].decode('utf-8')
        hdr = dict(l.split(': ', maxsplit=1) for l in hdr_contents.split('\n'))
        for key, value in list(hdr.items()):
            if ',' in value:
                try:
                    hdr[key] = np.array(value.split(','), dtype=int)
                except ValueError:
                    try:
                        hdr[key] = np.array(value.split(','), dtype=float)
                    except ValueError:
                        pass
            else:
                try:
                    hdr[key] = int(value)
                except ValueError:
                    try:
                        hdr[key] = float(value)
                    except ValueError:
                        pass
        del hdr['mrtrix image']  # Delete "magic line" at start of header
        array_start = int(hdr['file'].split()[1])
        array = np.asarray(contents[array_start:])
        dim = [hdr['dim'][int(l[1])] for l in hdr['layout']]
        array = array.reshape(dim)
        return hdr, array

    def get_header(self, fileset):
        self._load_header_and_array(fileset)[0]

    def get_array(self, fileset):
        self._load_header_and_array(fileset)[1]

    def get_vox_sizes(self, fileset):
        return self.get_header(fileset)['vox']

    def get_dims(self, fileset):
        return self.get_header(fileset)['dim']


# =====================================================================
# All Data Formats
# =====================================================================


# NeuroImaging data formats
dicom_format = DicomFormat(name='dicom', extension=None,
                           directory=True, within_dir_exts=['.dcm'],
                           resource_names={'xnat': ['DICOM', 'secondary']})
nifti_gz_x_format = NiftixFormat(name='extended_nifti_gz', extension='.nii.gz',
                                 aux_files={'json': '.json'},
                                 resource_names={'xnat': ['NIFTI_GZ_X',
                                                          'NIFTIX_GZ']})
nifti_format = NiftiFormat(name='nifti', extension='.nii',
                           resource_names={'xnat': ['NIFTI', 'NiFTI']})

nifti_gz_format = NiftiFormat(name='nifti_gz', extension='.nii.gz',
                              resource_names={'xnat': ['NIFTI_GZ',
                                                       'NiFTI_GZ']})
analyze_format = NiftiFormat(name='analyze', extension='.img',
                             aux_files={'header': '.hdr'})
mrtrix_image_format = MrtrixImageFormat(name='mrtrix_image', extension='.mif',
                                        resource_names={'xnat': ['MIF',
                                                                 'MRTRIX']})

# Set converters between image formats

nifti_gz_x_format.set_converter(dicom_format, Dcm2niixConverter)

nifti_format.set_converter(dicom_format, Dcm2niixConverter)
nifti_format.set_converter(analyze_format, MrtrixConverter)
nifti_format.set_converter(nifti_gz_format, MrtrixConverter)
nifti_format.set_converter(mrtrix_image_format, MrtrixConverter)

nifti_gz_format.set_converter(dicom_format, Dcm2niixConverter)
nifti_gz_format.set_converter(nifti_format, MrtrixConverter)
nifti_gz_format.set_converter(analyze_format, MrtrixConverter)
nifti_gz_format.set_converter(mrtrix_image_format, MrtrixConverter)
nifti_gz_format.set_converter(nifti_gz_x_format, IdentityConverter)

analyze_format.set_converter(dicom_format, MrtrixConverter)
analyze_format.set_converter(nifti_format, MrtrixConverter)
analyze_format.set_converter(nifti_gz_format, MrtrixConverter)
analyze_format.set_converter(mrtrix_image_format, MrtrixConverter)

mrtrix_image_format.set_converter(dicom_format, MrtrixConverter)
mrtrix_image_format.set_converter(nifti_format, MrtrixConverter)
mrtrix_image_format.set_converter(nifti_gz_format, MrtrixConverter)
mrtrix_image_format.set_converter(analyze_format, MrtrixConverter)

STD_IMAGE_FORMATS = [dicom_format, nifti_format, nifti_gz_format,
                     nifti_gz_x_format, analyze_format, mrtrix_image_format]

multi_nifti_gz_format = FileFormat(name='multi_nifti_gz', extension=None,
                                   directory=True, within_dir_exts=['.nii.gz'])
multi_nifti_gz_format.set_converter(zip_format, UnzipConverter)
multi_nifti_gz_format.set_converter(targz_format, UnTarGzConverter)

# Tractography formats
mrtrix_track_format = FileFormat(name='mrtrix_track', extension='.tck')

# Tabular formats
rfile_format = FileFormat(name='rdata', extension='.RData')
tsv_format = FileFormat(name='tab_separated', extension='.tsv')
# matlab_format = FileFormat(name='matlab', extension='.mat')
csv_format = FileFormat(name='comma_separated', extension='.csv')
text_matrix_format = FileFormat(name='text_matrix', extension='.mat')

# Diffusion gradient-table data formats
fsl_bvecs_format = FileFormat(name='fsl_bvecs', extension='.bvec')
fsl_bvals_format = FileFormat(name='fsl_bvals', extension='.bval')
mrtrix_grad_format = FileFormat(name='mrtrix_grad', extension='.b')

# Tool-specific formats
eddy_par_format = FileFormat(name='eddy_par',
                             extension='.eddy_parameters')
ica_format = FileFormat(name='ica', extension='.ica', directory=True)
par_format = FileFormat(name='parameters', extension='.par')
motion_mats_format = FileFormat(
    name='motion_mats', directory=True, within_dir_exts=['.mat'],
    desc=("Format used for storing motion matrices produced during "
          "motion detection pipeline"))


# PET formats
list_mode_format = FileFormat(name='pet_list_mode', extension='.bf')

# K-space formats

twix_vb_format = FileFormat(
    name='twix_vb', extension='.dat',
    resource_names={'xnat': ['DAT', 'KSPACE']},
    desc=("The format that k-space data is saved in from Siemens scanners "
          "with system version vB to (at least) vE"))

custom_kspace_format = FileFormat(
    name='custom_kspace', extension='.ks',
    resource_names={'xnat': ['CUSTOM_KSPACE']},
    aux_files={'ref': '.ref', 'json': '.json'},
    desc=("""A custom format for saving k-space data in binary amd JSON files.

    Binary files
    ------------
    primary : 5-d matrix
        Data from "data" scan organised in the following dimension order:
        channel, freq-encode, phase-encode, partition-encode (slice), echoes
    reference : 5-d matrix
        Data from calibration scan organised in the same dimension order as
        primary scan

    JSON side-car
    -------------
    dims : 3-tuple(int)
        The dimensions of the image in freq, phase, partition (slice) order
    voxel_size : 3-tuple(float)
        Size of the voxels in same order as dims
    num_channels : int
        Number of channels in the k-space
    num_echos : int
        Number of echoes in the acquisition
    TE : tuple(float)
        The echo times
    B0_strength : float
        Stength of the B0 field
    B0_dir : 3-tuple(float)
        Direction of the B0 field
    larmor_freq : float
        The central larmor frequency of the scanner"""))

custom_kspace_format.set_converter(twix_vb_format, TwixConverter)

KSPACE_FORMATS = [twix_vb_format, custom_kspace_format]

# MRS format
rda_format = FileFormat(name='raw', extension='.rda')

# Record list of all data formats registered by module (not really
# used currently but could be useful in future)
std_formats = []

# Add all data formats in module to a list of "standard" biomedical formats
for file_format in copy(globals()).values():
    if isinstance(file_format, FileFormat):
        std_formats.append(file_format.name)


# Since the conversion from DICOM->NIfTI is unfortunately slightly
# different between MRConvert and Dcm2niix, these data formats can
# be used in pipeline input specs that need to use MRConvert instead
# of Dcm2niix (i.e. motion-detection pipeline)
mrconvert_nifti_format = deepcopy(nifti_format)
mrconvert_nifti_format.set_converter(dicom_format, MrtrixConverter)
mrconvert_nifti_gz_format = deepcopy(nifti_gz_format)
mrconvert_nifti_gz_format.set_converter(dicom_format, MrtrixConverter)