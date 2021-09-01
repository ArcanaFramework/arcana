import os
import os.path as op
import typing as ty
from itertools import chain
import hashlib
import shutil
from abc import ABCMeta, abstractmethod
import attr
from arcana2.exceptions import ArcanaError, ArcanaUsageError
from arcana2.utils import split_extension, parse_value
from arcana2.exceptions import (
    ArcanaError, ArcanaFileFormatError, ArcanaUsageError, ArcanaNameError,
    ArcanaDataNotDerivedYetError, ArcanaUriAlreadySetException)
# from .file_format import FileFormat
from .enum import DataQuality
from .file_format import FileFormat
from .provenance import Provenance

@attr.s
class DataItem(metaclass=ABCMeta):
    """
    A representation of a file_group within the dataset.

    Parameters
    ----------
    name_path : str
        The name_path to the relative location of the file group, i.e. excluding
        information about which node in the data tree it belongs to
    type : FileFormat or type
        The file format used to store the file_group.
    index : int | None
        The index in which the file-group appears in the node it belongs to
        (starting at 0). Typically corresponds to the acquisition order for
        scans within an imaging session. Can be used to distinguish between
        scans with the same series description (e.g. multiple BOLD or T1w
        scans) in the same imaging sessions.
    quality : str
        The quality label assigned to the file_group (e.g. as is saved on XNAT)
    data_node : DataNode
        The data node within a dataset that the file-group belongs to
    exists : bool
        Whether the file_group exists or is just a placeholder for a derivative
    provenance : Provenance | None
        The provenance for the pipeline that generated the file-group,
        if applicable        
    """

    path: str = attr.ib()
    dtype: type or FileFormat = attr.ib()
    uri: str = attr.ib(default=None)
    index: int = attr.ib(default=None)
    quality: DataQuality = attr.ib(default=None)
    data_node = attr.ib(default=None)
    exists: bool = attr.ib(default=True)
    provenance: Provenance = attr.ib(default=None)

    @abstractmethod
    def get(self):
        raise NotImplementedError

    @abstractmethod
    def put(self):
        raise NotImplementedError

    @property
    def recorded_checksums(self):
        if self.provenance is None:
            return None
        else:
            return self.provenance.outputs[self.name_path]

    @provenance.validator
    def check_provenance(self, _, provenance):
        "Checks that the data item path is present in the provenance "
        if provenance is not None:
            if self.path not in provenance.outputs:
                raise ArcanaNameError(
                    self.path,
                    f"{self.path} was not found in outputs "
                    f"{provenance.outputs.keys()} of provenance provenance "
                    f"{provenance}")

    def _check_exists(self):
        if not self.exists:
            raise ArcanaDataNotDerivedYetError(
                self.name_path,
                f"Cannot access {self} as it hasn't been derived yet")

    def _check_part_of_data_node(self):
        if self.data_node is None:
            raise ArcanaUsageError(
                f"Cannot 'get' {self} as it is not part of a dataset")

@attr.s
class FileGroup(DataItem):
    """
    A representation of a file_group within the dataset.

    Parameters
    ----------
    name_path : str
        The name_path to the relative location of the file group, i.e. excluding
        information about which node in the data tree it belongs to
    format : FileFormat
        The file format used to store the file_group.
    index : int | None
        The index in which the file-group appears in the node it belongs to
        (starting at 0). Typically corresponds to the acquisition order for
        scans within an imaging session. Can be used to distinguish between
        scans with the same series description (e.g. multiple BOLD or T1w
        scans) in the same imaging sessions.
    quality : str
        The quality label assigned to the file_group (e.g. as is saved on XNAT)
    data_node : DataNode
        The data node within a dataset that the file-group belongs to
    exists : bool
        Whether the file_group exists or is just a placeholder for a derivative
    provenance : Provenance | None
        The provenance for the pipeline that generated the file-group,
        if applicable
    file_path : str | None
        Path to the file-group on the local file system (i.e. cache for remote
        repositories)
    side_cars : dict[str, str] | None
        Additional files in the file_group. Keys should match corresponding
        side_cars dictionary in format.
    checksums : dict[str, str]
        A checksums of all files within the file_group in a dictionary sorted
        bys relative file name_paths
    """

    file_path: str = attr.ib(default=None, converter=op.abspath)
    side_cars: ty.Dict[str, str] = attr.ib()
    checksums: ty.Dict[str, str] = attr.ib(default=None)

    HASH_CHUNK_SIZE = 2 ** 20  # 1MB in calc. checksums to avoid mem. issues

    def get(self):
        self._check_exists()
        self._check_part_of_data_node()
        self.set_file_paths(*self.data_node.get_file_group_paths(self))

    def put(self):
        self._check_part_of_data_node()
        if self.file_path is None:
            raise ArcanaUsageError(
                f"Need to set file path of {self} before it is 'put' in "
                "dataset")
        self.data_node.put_file_group(self)          

    def set_file_paths(self, file_path, side_cars=None):
        """Sets the primary file path and any side-car files

        Parameters
        ----------
        file_path : str
            The path to the primary 
        side_cars : Dict[str, str] or None
            dictionary with name of side-car files as keys (as defined in the
            FileFormat class) and file paths as values
        """
        self.exists = True
        self.file_path = file_path
        if side_cars is None:
            side_cars = self.default_side_cars()
        attr.validate(self)

    @property
    def file_paths(self):
        "Iterates through all files in the group and returns their file paths")
        if self.dtype.directory:
            return chain(*((op.join(root, f) for f in files)
                           for root, _, files in os.walk(self.file_path)))
        else:
            return chain([self.file_path], self.side_cars.values())

    def aux_file(self, name):
        return self.side_cars[name]  

    def get_checksums(self):
        self._check_exists()
        if self.checksums is None:
            if self.dataset is not None:
                self.checksums = self.dataset.get_checksums(self)
            if self.checksums is None:
                self.calculate_checksums()

    def calculate_checksums(self):
        self._check_exists()
        checksums = {}
        for fpath in self.file_paths:
            fhash = hashlib.md5()
            with open(fpath, 'rb') as f:
                # Calculate hash in chunks so we don't run out of memory for
                # large files.
                for chunk in iter(lambda: f.read(self.HASH_CHUNK_SIZE), b''):
                    fhash.update(chunk)
            checksums[op.relpath(fpath, self.file_path)] = fhash.hexdigest()
        self.checksums = checksums

    def contents_equal(self, other, **kwargs):
        """
        Test the equality of the file_group contents with another file_group.
        If the file_group's format implements a 'contents_equal' method than
        that is used to determine the equality, otherwise a straight comparison
        of the checksums is used.

        Parameters
        ----------
        other : FileGroup
            The other file_group to compare to
        """
        self._check_exists()
        if hasattr(self.dtype, 'contents_equal'):
            equal = self.dtype.contents_equal(self, other, **kwargs)
        else:
            equal = (self.checksums == other.checksums)
        return equal

    def copy_to(self, path: str, symlink: bool=False):
        """Copies the file-group to the new path, with auxiliary files saved
        alongside the primary-file path.

        Parameters
        ----------
        path : str
            Path to save the file-group to excluding file extensions
        symlink : bool
            Use symbolic links instead of copying files to new location
        """
        if symlink:
            copy_dir = copy_file = os.symlink
        else:
            copy_file = shutil.copyfile
            copy_dir = shutil.copytree
        if self.format.directory:
            copy_dir(self.file_path, path)
        else:
            copy_file(self.file_path, path + self.format.ext)
            for aux_name, aux_path in self.side_cars.items():
                copy_file(aux_path, path + self.format.side_cars[aux_name])
        return self.format.file_group_cls.from_path(path)

    @file_path.validate
    def validate_file_path(self, _, file_path):
        if file_path is not None:
            if not op.exists(file_path):
                raise ArcanaUsageError(
                    "Attempting to set a path that doesn't exist "
                    f"({file_path})")
            if not self.exists:
                raise ArcanaUsageError(
                        "Attempting to set a path to a file group that hasn't "
                        f"been derived yet ({file_path})")

    @side_cars.default
    def default_side_cars(self):
        if self.file_path is None:
            return None
        return self.dtype.default_aux_file_paths(self.file_path)

    @side_cars.validate
    def validate_side_cars(self, _, side_cars):
        if side_cars is not None:
            if self.file_path is None:
                raise ArcanaUsageError(
                    "Auxiliary files can only be provided to a FileGroup "
                    f"of '{self.path}' ({side_cars}) if the local path is "
                    "as well")
            if set(self.dtype.side_cars.keys()) != set(side_cars.keys()):
                raise ArcanaUsageError(
                    "Keys of provided auxiliary files ('{}') don't match "
                    "format ('{}')".format(
                        "', '".join(side_cars.keys()),
                        "', '".join(self.dtype.side_cars.keys())))
            if missing_side_cars:= [f for f in side_cars.values()
                                    if not op.exists(f)]:
                raise ArcanaUsageError(
                    f"Attempting to set paths of auxiliary files for {self} "
                    "that don't exist ('{}')".format(
                        "', '".join(missing_side_cars)))
@attr.s
class Field(DataItem):
    """
    A representation of a value field in the dataset.

    Parameters
    ----------
    name_path : str
        The name_path to the relative location of the field, i.e. excluding
        information about which node in the data tree it belongs to
    dtype : type
        The datatype of the value. Can be one of (float, int, str)
    derived : bool
        Whether or not the value belongs in the derived session or not
    data_node : DataNode
        The data node that the field belongs to
    exists : bool
        Whether the field exists or is just a placeholder for a derivative
    provenance : Provenance | None
        The provenance for the pipeline that generated the field,
        if applicable
    """

    value = attr.ib(converter=parse_value)

    def get(self):
        self._check_exists()
        self._check_part_of_data_node()
        self.value = self.data_node.get_field_value(self)

    def put(self):
        self._check_part_of_data_node()
        if self.value is None:
            raise ArcanaUsageError(
                f"Need to set value of {self} before it is 'put' in "
                "dataset")
        self.data_node.put_field(self)

    def __int__(self):
        return int(self.value)

    def __float__(self):
        return float(self.value)

    def __str__(self):
        if self.dtype._name == 'Sequence':
            val = '[' + ','.join(self._to_str(v) for v in self.value) + ']'
        else:
            val = self._to_str(self.value)
        return val

    def _to_str(self, val):
        if self.dtype is str:
            val = '"{}"'.format(val)
        else:
            val = str(val)
        return val

    def get_checksums(self):
        """
        For duck-typing with file_groups in checksum management. Instead of a
        checksum, just the value of the field is used
        """
        return self.value
