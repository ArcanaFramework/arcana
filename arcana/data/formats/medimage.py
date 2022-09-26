from abc import ABCMeta, abstractmethod
import os
import os.path as op
from pathlib import Path
import jq
import attrs
import json
import pydicom
import numpy as np
import nibabel
from pydra import Workflow, mark
from pydra.tasks.dcm2niix import Dcm2Niix
from pydra.tasks.mrtrix3.utils import MRConvert
from arcana.core.mark import converter
from arcana.exceptions import ArcanaUsageError
from arcana.core.data.format import WithSideCars
from arcana.data.formats.common import File, Directory


# =====================================================================
# Custom loader functions for different image types
# =====================================================================


class MedicalImage(File, metaclass=ABCMeta):

    INCLUDE_HDR_KEYS = None
    IGNORE_HDR_KEYS = None

    @abstractmethod
    def get_header(self):
        """
        Returns array data associated with the given path for the
        file format
        """

    @abstractmethod
    def get_array(self):
        """
        Returns header data associated with the given path for the
        file format
        """

    def contents_equal(self, other_image, rms_tol=None, **kwargs):
        """
        Test whether the (relevant) contents of two image self are equal
        given specific criteria

        Parameters
        ----------
        other_image : Fileset
            The other self to compare
        rms_tol : float
            The root-mean-square tolerance that is acceptable between the array
            data for the images to be considered equal
        """
        if type(other_image) != type(self):
            return False
        if self.headers_diff(self, other_image, **kwargs):
            return False
        if rms_tol:
            rms_diff = self.rms_diff(self, other_image)
            return rms_diff < rms_tol
        else:
            return np.array_equiv(self.get_array(), other_image.get_array())

    def headers_diff(self, other_image, include_keys=None, ignore_keys=None, **kwargs):
        """
        Check headers to see if all values
        """
        diff = []
        hdr = self.get_header()
        hdr_keys = set(hdr.keys())
        other_hdr = other_image.get_header()
        if include_keys is not None:
            if ignore_keys is not None:
                raise ArcanaUsageError(
                    "Doesn't make sense to provide both 'include_keys' ({}) "
                    "and ignore_keys ({}) to headers_equal method".format(
                        include_keys, ignore_keys
                    )
                )
            include_keys &= hdr_keys
        elif ignore_keys is not None:
            include_keys = hdr_keys - set(ignore_keys)
        else:
            if self.INCLUDE_HDR_KEYS is not None:
                if self.IGNORE_HDR_KEYS is not None:
                    raise ArcanaUsageError(
                        "Doesn't make sense to have both 'INCLUDE_HDR_FIELDS'"
                        "and 'IGNORE_HDR_FIELDS' class attributes of class {}".format(
                            type(self).__name__
                        )
                    )
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
                            if not np.allclose(value, other_value, equal_nan=True):
                                diff.append(key)
                        except TypeError:
                            # Fallback to a straight comparison for some formats
                            if value != other_value:
                                diff.append(key)
                elif value != other_value:
                    diff.append(key)
        return diff

    def rms_diff(self, other_image):
        """
        Return the RMS difference between the image arrays
        """
        return np.sqrt(np.sum((self.get_array() - other_image.get_array()) ** 2))


class DicomFile(
    File
):  # FIXME: Should extend from MedicalImage, but need to implement header and array

    ext = "dcm"


class SiemensDicomFile(DicomFile):

    ext = "IMA"


class Dicom(Directory, MedicalImage):

    content_types = (DicomFile,)
    alternate_names = ("secondary",)

    SERIES_NUMBER_TAG = ("0020", "0011")

    def dcm_files(self):
        return [f for f in os.listdir(self.path) if f.endswith(".dcm")]

    def get_array(self):
        image_stack = []
        for fname in self.dcm_files(self):
            image_stack.append(pydicom.dcmread(op.join(self.path, fname)).pixel_array)
        return np.asarray(image_stack)

    def get_header(self, index=0):
        dcm_files = [f for f in os.listdir(self.path) if f.endswith(".dcm")]
        # TODO: Probably should collate fields that vary across the set of
        #       files in the set into lists
        return pydicom.dcmread(op.join(self.path, dcm_files[index]))

    def get_vox_sizes(self):
        hdr = self.get_header()
        return np.array(hdr.PixelSpacing + [hdr.SliceThickness])

    def get_dims(self):
        hdr = self.get_header()
        return np.array(
            (hdr.Rows, hdr.DataColumns, len(self.dcm_files(self))), format=int
        )

    def extract_id(self):
        return int(self.dicom_values([self.SERIES_NUMBER_TAG])[0])

    def dicom_values(self, tags):
        """
        Returns a dictionary with the DICOM header fields corresponding
        to the given tag names

        Parameters
        ----------
        file_group : FileGroup
            The file group to extract the DICOM header for
        tags : List[Tuple[str, str]]
            List of DICOM tag values as 2-tuple of strings, e.g.
            [('0080', '0020')]

        Returns
        -------
        dct : Dict[Tuple[str, str], str|int|float]
        """

        def read_header():
            dcm = self.get_header(0)
            return [dcm[t].value for t in tags]

        try:
            if self.fs_path:
                # Get the DICOM object for the first file in the self
                dct = read_header()
            else:
                try:
                    # Try to access dicom header details remotely
                    hdr = self.row.dataset.store.dicom_header(self)
                except AttributeError:
                    self.get()  # Fallback to downloading data to read header
                    dct = read_header()
                else:
                    dct = [hdr[t] for t in tags]
        except KeyError as e:
            e.msg = "{} does not have dicom tag {}".format(self, str(e))
            raise e
        return dct


class SiemensDicom(Dicom):

    content_types = (SiemensDicomFile,)
    alternative_names = ("dicom",)


class NeuroImage(MedicalImage):
    """Imaging formats developed for neuroimaging scans"""

    @classmethod
    @converter(MedicalImage)
    def mrconvert(cls, fs_path):
        node = MRConvert(in_file=fs_path, out_file="out." + cls.ext)
        return node, node.lzout.out_file


class Nifti(NeuroImage):

    ext = "nii"
    alternative_names = ("NIFTI",)

    def get_header(self):
        return dict(nibabel.load(self.fs_path).header)

    def get_array(self):
        return nibabel.load(self.fs_path).get_data()

    def get_vox_sizes(self):
        # FIXME: This won't work for 4-D files
        return self.get_header()["pixdim"][1:4]

    def get_dims(self):
        # FIXME: This won't work for 4-D files
        return self.get_header()["dim"][1:4]

    @classmethod
    @converter(Dicom)
    def dcm2niix(
        cls,
        fs_path,
        extract_volume=None,
        file_postfix=attrs.NOTHING,
        side_car_jq=None,
        to_4d=False,
    ):
        as_workflow = extract_volume is not None or side_car_jq is not None or to_4d

        if extract_volume is not None and to_4d:
            raise ValueError(
                f"'extract_volume' ({extract_volume}) and 'to_4d' are mutually exclusive"
            )

        in_dir = fs_path
        compress = "n"
        if as_workflow:
            wf = Workflow(
                name="multistep_conv",
                input_spec=["in_dir", "compress"],
                in_dir=in_dir,
                compress=compress,
            )
            in_dir = wf.lzin.in_dir
            compress = wf.lzin.compress
        node = Dcm2Niix(
            in_dir=in_dir,
            out_dir=".",
            name="dcm2niix",
            compress=compress,
            file_postfix=file_postfix,
        )
        if as_workflow:
            wf.add(node)
            out_file = wf.dcm2niix.lzout.out_file
            out_json = wf.dcm2niix.lzout.out_json
            if extract_volume is not None or to_4d:
                if extract_volume:
                    coord = [3, extract_volume]
                    axes = [0, 1, 2]
                else:  # to_4d
                    coord = attrs.NOTHING
                    axes = [0, 1, 2, -1]
                wf.add(
                    MRConvert(
                        in_file=out_file,
                        coord=coord,
                        axes=axes,
                        name="mrconvert",
                    )
                )
                out_file = wf.mrconvert.lzout.out_file

            if side_car_jq is not None:
                wf.add(
                    edit_side_car(
                        in_file=out_json, jq_expr=side_car_jq, name="json_edit"
                    )
                )
                out_json = wf.json_edit.lzout.out
            wf.set_output(("out_file", out_file))
            wf.set_output(("out_json", out_json))
            wf.set_output(("out_bvec", wf.dcm2niix.lzout.out_bvec))
            wf.set_output(("out_bval", wf.dcm2niix.lzout.out_bval))
            out = wf, wf.lzout.out_file
        else:
            out = node, node.lzout.out_file
        return out


@mark.task
def edit_side_car(in_file: Path, jq_expr: str, out_file=None) -> Path:
    """ "Applies ad-hoc edit of JSON side car with JQ query language"""
    if out_file is None:
        out_file = in_file
    with open(in_file) as f:
        dct = json.load(f)
    dct = jq.compile(jq_expr).input(dct).first()
    with open(out_file, "w") as f:
        json.dump(dct, f)
    return in_file


class NiftiGz(Nifti):

    ext = "nii.gz"

    @classmethod
    @converter(Dicom)
    def dcm2niix(cls, fs_path, **kwargs):
        node, out_file = Nifti.dcm2niix(fs_path, **kwargs)
        node.inputs.compress = "y"
        return node, out_file


class NiftiX(WithSideCars, Nifti):

    side_car_exts = ("json",)

    def get_header(self):
        hdr = super().get_header()
        with open(self.side_car("json")) as f:
            hdr.update(json.load(f))
        return hdr

    @classmethod
    @converter(Dicom)
    def dcm2niix(cls, fs_path, **kwargs):
        node, out_file = Nifti.dcm2niix(fs_path, **kwargs)
        return node, (out_file, node.lzout.out_json)

    mrconvert = None  # Only dcm2niix produces the required JSON side car


class NiftiGzX(NiftiX, NiftiGz):
    @classmethod
    @converter(Dicom)
    def dcm2niix(cls, fs_path, **kwargs):
        node, out_files = NiftiX.dcm2niix(fs_path, **kwargs)
        node.inputs.compress = "y"
        return node, out_files


# NIfTI file format gzipped with BIDS side car
class NiftiFslgrad(WithSideCars, Nifti):

    side_car_exts = ("bvec", "bval")

    @classmethod
    @converter(Dicom)
    def dcm2niix(cls, fs_path, **kwargs):
        node, out_file = Nifti.dcm2niix(fs_path, **kwargs)
        return node, (out_file, node.lzout.out_bvec, node.lzout.out_bval)

    mrconvert = None  # Technically mrconvert can export fsl grads but dcm2niix will be sufficient 99% of the time


class NiftiGzFslgrad(NiftiFslgrad, NiftiGz):

    pass


class NiftiXFslgrad(NiftiX, NiftiFslgrad):

    side_car_exts = NiftiX.side_car_exts + NiftiFslgrad.side_car_exts

    @classmethod
    @converter(Dicom)
    def dcm2niix(cls, fs_path, **kwargs):
        node, out_file = NiftiX.dcm2niix(fs_path, **kwargs)
        return node, out_file + (node.lzout.out_bvec, node.lzout.out_bval)


class NiftiGzXFslgrad(NiftiXFslgrad, NiftiGz):
    @classmethod
    @converter(Dicom)
    def dcm2niix(cls, fs_path, **kwargs):
        node, out_files = NiftiXFslgrad.dcm2niix(fs_path, **kwargs)
        node.inputs.compress = "y"
        return node, out_files


class MrtrixImage(NeuroImage):

    ext = "mif"

    def _load_header_and_array(self):
        with open(self.path, "rb") as f:
            contents = f.read()
        hdr_end = contents.find(b"\nEND\n")
        hdr_contents = contents[:hdr_end].decode("utf-8")
        hdr = dict(ln.split(": ", maxsplit=1) for ln in hdr_contents.split("\n"))
        for key, value in list(hdr.items()):
            if "," in value:
                try:
                    hdr[key] = np.array(value.split(","), format=int)
                except ValueError:
                    try:
                        hdr[key] = np.array(value.split(","), format=float)
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
        del hdr["mrtrix image"]  # Delete "magic line" at start of header
        array_start = int(hdr["file"].split()[1])
        array = np.asarray(contents[array_start:])
        dim = [hdr["dim"][int(ln[1])] for ln in hdr["layout"]]
        array = array.reshape(dim)
        return hdr, array

    def get_header(self):
        return self._load_header_and_array(self)[0]

    def get_array(self):
        return self._load_header_and_array(self)[1]

    def get_vox_sizes(self):
        return self.get_header(self)["vox"]

    def get_dims(self):
        return self.get_header(self)["dim"]


# =====================================================================
# All Data Formats
# =====================================================================


class Analyze(WithSideCars, NeuroImage):

    ext = "img"
    side_car_exts = ("hdr",)

    def get_array(self):
        raise NotImplementedError

    def get_header(self):
        raise NotImplementedError


class MrtrixTrack(File):

    ext = "tck"


class Dwigrad(File):

    pass


class MtrixGrad(Dwigrad):

    ext = "b"


class Fslgrad(Dwigrad):

    ext = "bvec"
    side_cars = ("bval",)


# Raw formats


class ListMode(File):

    ext = "bf"


class Kspace(File):

    pass


class TwixVb(Kspace):
    """The format that k-space data is saved in from Siemens scanners
    with system version vB to (at least) vE"""

    ext = "dat"


class CustomKspace(Kspace):
    """A custom format for saving k-space data in binary amd JSON files.

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
    T E : tuple(float)
        The echo times
    B0_strength : float
        Strength of the B0 field
    B0_dir : 3-tuple(float)
        Direction of the B0 field
    larmor_freq : float
        The central larmor row_frequency of the scanner"""

    ext = "ks"
    side_cars = ("ref", "json")

    @classmethod
    @converter
    def from_twix(cls, fs_path):
        # input = 'in_file'
        # output = 'out_file'
        # output_side_cars = {'ref': 'ref_file', 'json': 'hdr_file'}
        # requirements = [matlab_req.v('R2018a')]
        # interface = TwixReader()
        raise NotImplementedError


class Rda(File):
    """MRS format"""

    ext = "rda"
