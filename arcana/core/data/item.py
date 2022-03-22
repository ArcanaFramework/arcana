import os
import os.path as op
from pathlib import Path
import typing as ty
from itertools import chain
import hashlib
import shutil
from abc import ABCMeta, abstractmethod
import attr
from attr.converters import optional
from arcana.core.utils import parse_value
from arcana.exceptions import (
    ArcanaUsageError, ArcanaNameError, ArcanaUsageError,
    ArcanaDataNotDerivedYetError, ArcanaFileFormatError)
from ..enum import DataQuality
from .format import FileFormat
from .provenance import DataProvenance


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
    order : int | None
        The order in which the file-group appears in the node it belongs to
        (starting at 0). Typically corresponds to the acquisition order for
        scans within an imaging session. Can be used to distinguish between
        scans with the same series description (e.g. multiple BOLD or T1w
        scans) in the same imaging sessions.
    quality : str
        The quality label assigned to the file_group (e.g. as is saved on XNAT)
    data_node : DataNode
        The data node within a dataset that the file-group belongs to
    exists : bool
        Whether the file_group exists or is just a placeholder for a sink
    provenance : Provenance | None
        The provenance for the pipeline that generated the file-group,
        if applicable        
    """

    path: str = attr.ib()
    format: type or FileFormat = attr.ib()
    uri: str = attr.ib(default=None)
    order: int = attr.ib(default=None)
    quality: DataQuality = attr.ib(default=DataQuality.usable)
    exists: bool = attr.ib(default=True)
    provenance: DataProvenance = attr.ib(default=None)
    data_node = attr.ib(default=None)    

    @abstractmethod
    def get(self, assume_exists=False):
        """Pulls data from the store (if remote) and caches locally

        Parameters
        ----------
        assume_exists: bool
            If set, checks to see whether the item exists are skipped (used
            to pull data after a successful workflow run)
            """
        raise NotImplementedError

    @abstractmethod
    def put(self, value):
        """Updates the value of the item in the store to the provided value,
        pushing remotely if necessary.

        Parameters
        ----------
        value : ty.Any
            The value to update
        """
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
                self.path,
                f"Cannot access {self} as it hasn't been derived yet")

    def _check_part_of_data_node(self):
        if self.data_node is None:
            raise ArcanaUsageError(
                f"Cannot 'get' {self} as it is not part of a dataset")


def absolute_path(path):
    return Path(path).absolute()


def absolute_paths_dict(dct):
    return {n: absolute_path(p) for n, p in dict(dct).items()}

@attr.s
class FileGroup(DataItem, metaclass=ABCMeta):
    """
    A representation of a file_group within the dataset.

    Parameters
    ----------
    name_path : str
        The name_path to the relative location of the file group, i.e. excluding
        information about which node in the data tree it belongs to
    format : FileFormat
        The file format used to store the file_group.
    order : int | None
        The order in which the file-group appears in the node it belongs to
        (starting at 0). Typically corresponds to the acquisition order for
        scans within an imaging session. Can be used to distinguish between
        scans with the same series description (e.g. multiple BOLD or T1w
        scans) in the same imaging sessions.
    quality : str
        The quality label assigned to the file_group (e.g. as is saved on XNAT)
    data_node : DataNode
        The data node within a dataset that the file-group belongs to
    exists : bool
        Whether the file_group exists or is just a placeholder for a sink
    provenance : Provenance | None
        The provenance for the pipeline that generated the file-group,
        if applicable
    fs_path : str | None
        Path to the primary file or directory on the local file system
    side_cars : ty.Dict[str, str] | None
        Additional files in the file_group. Keys should match corresponding
        side_cars dictionary in format.
    checksums : ty.Dict[str, str]
        A checksums of all files within the file_group in a dictionary sorted
        bys relative file name_paths
    """

    fs_path: str = attr.ib(default=None, converter=optional(absolute_path))
    _checksums: ty.Dict[str, str] = attr.ib(default=None, repr=False,
                                            init=False)

    HASH_CHUNK_SIZE = 2 ** 20  # 1MB in calc. checksums to avoid mem. issues

    @fs_path.validator
    def validate_fs_path(self, _, fs_path):
        if fs_path is not None:
            if not fs_path.exists:
                raise ArcanaUsageError(
                    "Attempting to set a path that doesn't exist "
                    f"({fs_path})")
            if not self.exists:
                raise ArcanaUsageError(
                        "Attempting to set a path to a file group that hasn't "
                        f"been derived yet ({fs_path})")

    def get(self, assume_exists=False):
        if assume_exists:
            self.exists = True
        self._check_part_of_data_node()
        self.set_fs_paths()

    def put(self, fs_path, side_cars=None):
        self._check_part_of_data_node()
        if side_cars is None:
            side_cars = self.format.default_side_cars(fs_path)
        elif side_cars is not None:
            side_cars = absolute_paths_dict(side_cars)
        self.data_node.put_file_group(self, fs_path=fs_path,
                                      side_cars=side_cars)
        if not self.exists:
            self.set_fs_paths()

    @property
    def value(self):
        return str(self.fs_path)

    def set_fs_paths(self, fs_path=None, side_cars=None):
        """Sets the primary file path and any side-car files from the node

        Parameters
        ----------
        fs_path : str
            The path to the primary 
        side_cars : Dict[str, str] or None
            dictionary with name of side-car files as keys (as defined in the
            FileFormat class) and file paths as values
        """
        self.exists = True
        if fs_path is None:
            fs_path, side_cars = self.data_node.get_file_group(self)
        self.fs_path = absolute_path(fs_path)
        if side_cars is None:
            side_cars = self.default_side_cars()
        self.side_cars = absolute_paths_dict(side_cars)
        attr.validate(self)

    @property
    def fs_paths(self):
        if self.fs_path is None:
            raise ArcanaUsageError(
                f"Attempting to access file paths of {self} before they are set")
        return chain([self.fs_path], self.side_cars.values())

    def all_file_paths(self):
        "Iterates through all files in the group and returns their file paths"
        if self.fs_path is None:
            raise ArcanaUsageError(
                f"{self} has not be retrieved from the store. Use 'get' "
                "method first.")
        if self.format.directory:
            return chain(*((Path(root) / f for f in files)
                           for root, _, files in os.walk(self.fs_path)))
        else:
            return self.fs_paths

    def side_car(self, name):
        return self.side_cars[name]

    @property
    def checksums(self):
        if self._checksums is None:
            self.get_checksums()
        return self._checksums

    def get_checksums(self, force_calculate=False):
        self._check_exists()
        # Load checksums from store (e.g. via API)
        if self.data_node is not None and not force_calculate:
            self._checksums = self.data_node.dataset.store.get_checksums(self)
        # If the store cannot calculate the checksums do them manually
        else:
            self._checksums = self.calculate_checksums()

    def calculate_checksums(self):
        self._check_exists()
        checksums = {}
        for fpath in self.all_file_paths():
            fhash = hashlib.md5()
            with open(fpath, 'rb') as f:
                # Calculate hash in chunks so we don't run out of memory for
                # large files.
                for chunk in iter(lambda: f.read(self.HASH_CHUNK_SIZE), b''):
                    fhash.update(chunk)
            try:
                rel_path = str(fpath.relative_to(self.fs_path))
            except ValueError:
                rel_path = '.'.join(fpath.suffixes)
            checksums[rel_path] = fhash.hexdigest()
        return checksums

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
        if hasattr(self.format, 'contents_equal'):
            equal = self.format.contents_equal(self, other, **kwargs)
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
            copy_dir(self.fs_path, path)
        else:
            copy_file(self.fs_path, path + self.format.ext)
            for aux_name, aux_path in self.side_cars.items():
                copy_file(aux_path, path + self.format.side_cars[aux_name])
        return self.format.from_path(path)


@attr.s
class Field(DataItem):
    """
    A representation of a value field in the dataset.

    Parameters
    ----------
    name_path : str
        The name_path to the relative location of the field, i.e. excluding
        information about which node in the data tree it belongs to
    format : type
        The format of the value. Can be one of (float, int, str)
    derived : bool
        Whether or not the value belongs in the derived session or not
    data_node : DataNode
        The data node that the field belongs to
    exists : bool
        Whether the field exists or is just a placeholder for a sink
    provenance : Provenance | None
        The provenance for the pipeline that generated the field,
        if applicable
    """

    value: int or float or str = attr.ib(converter=parse_value, default=None)

    def get(self, assume_exists=False):
        if not assume_exists:
            self._check_exists()
        self._check_part_of_data_node()
        self.value = self.data_node.get_field(self)

    def put(self, value):
        self._check_part_of_data_node()
        self.data_node.put_field(self, self.format(value))
        self.exists = True

    def __int__(self):
        return int(self.value)

    def __float__(self):
        return float(self.value)

    def __str__(self):
        if self.format.__args__:  # Sequence type
            val = '[' + ','.join(self._to_str(v) for v in self.value) + ']'
        else:
            val = self._to_str(self.value)
        return val

    def _to_str(self, val):
        if self.format is str:
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

@attr.s
class File(FileGroup):

    @classmethod
    def from_paths(cls, paths: ty.List[Path], **kwargs):
        return [cls(p, **kwargs)
                for p in paths if str(p).endswith('.' + cls.ext)]
     
@attr.s
class FileWithSideCars(File):

    side_cars: ty.Dict[str, str] = attr.ib(converter=optional(absolute_paths_dict))

    @side_cars.default
    def default_side_cars(self):
        if self.fs_path is None:
            return {}
        return self.format.default_side_cars(self.fs_path)

    @side_cars.validator
    def validate_side_cars(self, _, side_cars):
        if side_cars is not None:
            if self.fs_path is None:
                raise ArcanaUsageError(
                    "Auxiliary files can only be provided to a FileGroup "
                    f"of '{self.path}' ({side_cars}) if the local path is "
                    "as well")
            if set(self.format.side_cars.keys()) != set(side_cars.keys()):
                raise ArcanaUsageError(
                    "Keys of provided auxiliary files ('{}') don't match "
                    "format ('{}')".format(
                        "', '".join(side_cars.keys()),
                        "', '".join(self.format.side_cars.keys())))
            missing_side_cars = [f for f in side_cars.values()
                                 if not op.exists(f)]
            if missing_side_cars:
                raise ArcanaUsageError(
                    f"Attempting to set paths of auxiliary files for {self} "
                    "that don't exist ('{}')".format(
                        "', '".join(missing_side_cars)))

    @classmethod
    def from_paths(cls, paths: ty.List[Path], **kwargs):
        return [cls(p, **kwargs)
                for p in paths if str(p).endswith('.' + cls.ext)]
    

class Directory(FileGroup):

    @classmethod
    def from_paths(cls, paths: ty.List[Path], **kwargs):
        return [cls(p, **kwargs) for p in paths if p.is_dir()]



# @classmethod
# def from_files(cls, candidates):
#     matches = cls.matches(candidates)
#     if len(matches) == 1:
#         return cls(cls.matches_ext(candidates))
#     elif not matches:
#         candidates_str = ', '.join(str(c) for c in candidates)
#         raise ArcanaFileFormatError(
#             f"No files match extension of {cls.__name__} ('{cls.ext}') out "
#             f"of potential candidates of {candidates_str}")
#     else:
#         matching_str = ', '.join(str(m) for m in matches)
#         msg = (f"Multiple files match extension of {cls.__name__} "
#                 f"('{cls.ext}'): {matching_str}")