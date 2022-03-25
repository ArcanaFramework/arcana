import os
import os.path as op
from pathlib import Path
import typing as ty
from itertools import chain
from copy import copy
import hashlib
import logging
import shutil
from abc import ABCMeta, abstractmethod
import attr
from attr.converters import optional
from arcana.core.utils import parse_value, func_task
from arcana.exceptions import (
    ArcanaUnresolvableFormatException, ArcanaUsageError, ArcanaNameError, ArcanaUsageError,
    ArcanaDataNotDerivedYetError, ArcanaFileFormatError, ArcanaNoConverterError)
from ..enum import DataQuality


logger = logging.getLogger('arcana')

@attr.s
class DataItem(metaclass=ABCMeta):
    """
    A representation of a file_group within the dataset.

    Parameters
    ----------
    name_path : str
        The name_path to the relative location of the file group, i.e. excluding
        information about which node in the data tree it belongs to
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
    uri: str = attr.ib(default=None)
    order: int = attr.ib(default=None)
    quality: DataQuality = attr.ib(default=DataQuality.usable)
    exists: bool = attr.ib(default=True)
    provenance: ty.Dict[str, ty.Any] = attr.ib(default=None)
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
        self.value = self.data_node.dataset.store.get_field(self)

    def put(self, value):
        self._check_part_of_data_node()
        self.data_node.dataset.store.put_field(self, self.format(value))
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
    # Alternative names for the file format, empty by default overridden in
    # sub-classes where necessary
    alternative_names = ()

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
        self.set_file_paths(
            *self.data_node.dataset.store.get_file_group_paths(self))
        self.validate_file_paths()

    def put(self, *fs_paths):
        self._check_part_of_data_node()
        dir_paths = list(p for p in fs_paths if p.is_dir())
        if len(dir_paths) > 1:
            dir_paths_str = "', '".join(str(p) for p in dir_paths)
            raise ArcanaFileFormatError(
                f"Cannot put more than one directory, {dir_paths_str}, as part "
                f"of the same file group {self}")
        cache_paths = self.data_node.dataset.store.put_file_group_paths(
            self, fs_paths)
        self.set_file_paths(*cache_paths)
        self.validate_file_paths()
        # Save provenance
        if self.provenance:
            self.data_node.dataset.store.put_provenance(self)

    @property
    def fs_paths(self):
        """All base paths (i.e. not nested within directories) in the file group"""
        if self.fs_path is None:
            raise ArcanaUsageError(
                f"Attempting to access file path of {self} before it is set")
        return [self.fs_path]        

    @classmethod
    def format_name(cls):
        return cls.__name__.lower()

    @classmethod
    def lf_file_names(cls):
        return ('fs_path',)

    @classmethod
    def matches_format_name(cls, name: str):
        """Checks to see whether the provided name is a valid name for the
        file format. Alternative names can be provided for format-specific
        subclasses, or this method can be overridden.

        Parameters
        ----------
        name : str
            Name to match

        Returns
        -------
        bool
            whether or not the name matches the format
        """
        return name in (cls.format_name(),) + cls.alternative_names

    @property
    def value(self):
        return str(self.fs_path)

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
        for fpath in self.all_file_paths:
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
        other._check_exists()
        return self.checksums == other.checksums

    @classmethod
    def resolve(cls, unresolved):
        """Resolve file group loaded from a repository to the specific format

        Parameters
        ----------
        unresolved : UnresolvedFileGroup
            A file group loaded from a repository that has not been resolved to
            a specific format yet

        Returns
        -------
        FileGroup
            The resolved file-group object

        Raises
        ------
        ArcanaUnresolvableFormatException
            If there doesn't exist a unique resolution from the unresolved file
            group to the given format, then an ArcanaFileFormatError should be
            raised
        """
        # Perform matching based on resource names in multi-format
        # file-group
        if unresolved.uris is not None:
            item = None
            for format_name, uri in unresolved.uris.items():
                if cls.matches_format_name(format_name):
                    item = cls(uri=uri, **unresolved.item_kwargs)
            if item is None:
                raise ArcanaFileFormatError(
                    f"Could not file a matching resource in {unresolved.path} for"
                    f" the given format ({format.name}), found "
                    "('{}')".format("', '".join(unresolved.uris)))
        else:
            item = cls(**unresolved.item_kwargs)
            item.set_file_paths(unresolved.file_paths)
        return item

    @abstractmethod
    def set_file_paths(self, paths):
        """Set the file paths of the file group

        Parameters
        ----------
        paths : list[Path]
            The candidate paths from which to set the paths of the 
            file group from. Note that not all paths need to be set if
            they are not relevant.

        Raises
        ------
        ArcanaFileFormatError
            is raised if the required the paths cannot be set from the provided
        """

    @classmethod
    def matches_ext(cls, ext, paths):
        matches = [str(p) for p in paths if str(p).endswith('.' + ext)]
        if not matches:
            paths_str = ', '.join(str(p) for p in paths)
            raise ArcanaFileFormatError(
                f"No matching files with '{ext}' extension found in "
                f"file group {paths_str}")
        elif len(matches) > 1:
            matches_str = ', '.join(matches)
            raise ArcanaFileFormatError(
                f"Multiple files with '{ext}' extension found in : {matches_str}")
        return absolute_path(matches[0])

    def validate_file_paths(self):
        attr.validate(self)
        self.exists = True

    @classmethod
    def get_converter(cls, from_format):
        """Gets the converter method from the given format

        Parameters
        ----------
        from_format : type
            The format type to convert from

        Returns
        -------
        function
            The bound method that adds nodes to a given workflow

        Raises
        ------
        ArcanaNoConverterError
            _description_
        """
        converter = None
        for attr_name in dir(cls):
            meth = getattr(cls, attr_name)
            try:
                converts_from = meth.__annotations__['arcana_converter']
            except (AttributeError, KeyError):
                pass
            else:
                if issubclass(from_format, converts_from):
                    if converter:
                        prev_converts_from = converter.__annotations__['arcana_converter']
                        if issubclass(converts_from, prev_converts_from):
                            converter = meth
                        elif not issubclass(prev_converts_from, converts_from):
                            raise ArcanaNoConverterError(
                                f"Ambiguous converters between {from_format} "
                                f"and {cls}: {converter} and {meth}. Please "
                                "define a specific converter directly between "
                                f"the subclasses (i.e. instead of {prev_converts_from} "
                                f"and {converts_from} respectively)")
        if not converter:
            raise ArcanaNoConverterError(
                f"No conversion defined between {from_format} and {cls}")
        return converter

    
    @classmethod
    def add_converter(cls, wf, from_format, file_group_lf, node_name):
        """Adds a converter node to a workflow

        Parameters
        ----------
        wf : Workflow
            the pydra workflow to add the node to
        from_format : type
            the file-group class to convert from
        file_group_lf : LazyField
            Lazy field pointing to a FileGroup that is to be converted
        node_name : str
            the name to give the added converter node
        """
        converter_spec = cls.get_converter(from_format)
        converter = converter_spec[0]  # Converter task
        output_lfs = converter_spec[1:]  # Output lazy-fields to pass to encapsulater
        
        wf.add(func_task(
            extract_paths,
            in_fields=[('from_format', type), ('file_group', FileGroup)],
            out_fields=[(i, str) for i in inputs],
            name=node_name + '_extract_paths',
            from_format=from_format,
            file_group=wf.lzin.to_convert))
        
        converter_task = stored_format.converter(produced_format)(
                    wf,
                    to_sink[output_name],
                    name=cname)
        # Add the actual converter node
        conv_kwargs = copy(task_kwargs)
        conv_kwargs.update(kwargs)
        # Map 
        conv_kwargs.update((inputs[i], getattr(wf.extract_paths.lzout, i))
                            for i in self.inputs)
        wf.add(self.task(name='converter', **conv_kwargs))

        wf.add(func_task(
            encapsulate_paths,
            in_fields=[('to_format', type)] + [(o, str) for o in self.outputs],
            out_fields=[('converted', FileGroup)],
            name='encapsulate_paths',
            to_format=self.to_format,
            **{k: getattr(wf.converter.lzout, v)
               for k, v in self.outputs.items()}))


def extract_paths(from_format, file_group):
    """Copies files into the CWD renaming so the basenames match
    except for extensions"""
    logger.debug("Extracting paths from %s (%s format) before conversion", file_group, from_format)
    if file_group.format != from_format:
        raise ValueError(f"Format of {file_group} doesn't match converter {from_format}")
    cpy = file_group.copy_to(Path(file_group.path).name, symlink=True)
    return cpy.fs_paths if len(cpy.fs_paths) > 1 else cpy.fs_path


def encapsulate_paths(to_format: type, orig_file_group: FileGroup, *fs_paths: ty.List[Path]):
    """Copies files into the CWD renaming so the basenames match
    except for extensions"""
    logger.debug("Encapsulating %s into %s format after conversion",
                 fs_paths, to_format)
    file_group = to_format(orig_file_group.path + '_' + to_format.format_name())
    file_group.set_file_paths(fs_paths)
    return file_group


@attr.s
class BaseFile(FileGroup):

    is_dir = False

    def set_file_paths(self, paths):
        self.fs_path = self.matches_ext(self.ext, paths)

    def all_file_paths(self):
        """The paths of all nested files within the file-group"""
        if self.fs_path is None:
            raise ArcanaUsageError(
                f"Attempting to access file paths of {self} before they are set")
        return self.fs_paths

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
            copy_file = os.symlink
        else:
            copy_file = shutil.copyfile
        copy_file(self.fs_path, path + self.format.ext)
        return self.from_paths(path)[0]

    @classmethod
    def copy_ext(cls, old_path, new_path):
        """Copy extension from the old path to the new path, ensuring that all
        of the extension is used (e.g. 'nii.gz' instead of 'gz')

        Parameters
        ----------
        old_path: Path or str
            The path from which to copy the extension from
        new_path: Path or str
            The path to append the extension to

        Returns
        -------
        Path
            The new path with the copied extension
        """
        if not cls.matches_ext(old_path):
            raise ArcanaFileFormatError(
                f"Extension of old path ('{str(old_path)}') does not match that "
                f"of file, '{cls.ext}'")
        return new_path.with_suffix(cls.ext)

     
@attr.s
class BaseFileWithSideCars(BaseFile):
    """Base class for file-groups with a primary file and several header or
    side car files
    """

    side_cars: ty.Dict[str, str] = attr.ib(converter=optional(absolute_paths_dict))

    @side_cars.default
    def default_side_cars(self):
        if self.fs_path is None:
            return {}
        return self.default_side_car_paths(self.fs_path)

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
    def lf_file_names(cls):
        return super().lf_file_names() + cls.side_car_exts           

    def set_file_paths(self, *paths):
        super().set_file_paths(paths)
        to_assign = copy(paths)
        to_assign.remove(self.fs_path)
        for sc_ext in self.side_car_exts:
            matched = self.side_cars[sc_ext] = self.matches_ext(sc_ext, paths)
            to_assign.remove(matched)

    @property
    def fs_paths(self):
        return chain(super().fs_paths, self.side_cars.values())

    def side_car(self, name):
        return self.side_cars[name]

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
            copy_file = os.symlink
        else:
            copy_file = shutil.copyfile
        dest_path = path + self.ext
        copy_file(self.fs_path, dest_path)
        dest_side_cars = self.default_side_car_paths(dest_path)
        for sc_ext, sc_path in self.side_cars.items():
            copy_file(sc_path, dest_side_cars[sc_ext])
        cpy = copy(self)
        cpy.set_file_paths(path, *dest_side_cars.values())
        return cpy
    
    @classmethod
    def default_side_car_paths(cls, primary_path):
        """
        Get the default paths for auxiliary files relative to the path of the
        primary file, i.e. the same name as the primary path with a different
        extension

        Parameters
        ----------
        primary_path : str
            Path to the primary file in the file_group

        Returns
        -------
        aux_paths : ty.Dict[str, str]
            A dictionary of auxiliary file names and default paths
        """
        return dict((n, str(primary_path)[:-len(cls.ext)] + ext)
                    for n, ext in cls.side_cars.items())

    @classmethod
    def copy_ext(cls, old_path, new_path):
        """Copy extension from the old path to the new path, ensuring that all
        of the extension is used (e.g. 'nii.gz' instead of 'gz'). If the old
        path extension doesn't match the primary path, the methods loops through
        all side-car extensions and selects the longest matching.

        Parameters
        ----------
        old_path: Path or str
            The path from which to copy the extension from
        new_path: Path or str
            The path to append the extension to

        Returns
        -------
        Path
            The new path with the copied extension
        """
        try:
            # Check to see if the path it matches the primary path extension
            return super().copy_ext(old_path, new_path)
        except ArcanaFileFormatError:
            pass
        matches = [e for e in cls.side_car_exts
                    if cls.matches_ext(e, old_path)]
        if not matches:
            sc_exts_str = "', '".join(cls.side_car_exts)
            raise ArcanaFileFormatError(
                f"Extension of old path ('{str(old_path)}') does not match any "
                f" in {cls}: '{cls.ext}', {sc_exts_str}")
        longest_match = max(matches, key=len)
        return Path(new_path).with_suffix(longest_match)


class BaseDirectory(FileGroup):

    is_dir = True
    content_types = ()  # By default, don't check contents for any types
    
    @classmethod
    def from_paths(cls, *paths: ty.List[Path], **kwargs):
        return [cls(p, **kwargs) for p in paths
                if p.is_dir() and cls.contents_match(p)]

    @classmethod
    def contents_match(cls, path: Path):
        from arcana.core.data.node import UnresolvedFileGroup
        contents = UnresolvedFileGroup.from_paths(path.iterdir())
        for content_type in cls.contents:
            resolved = False
            for unresolved in contents:
                try:
                    content_type.resolve(unresolved)
                except ArcanaFileFormatError:
                    pass
                else:
                    resolved = True
            if not resolved:
                raise ArcanaFileFormatError(
                    f"Did not find a match for required content type {content_type} "
                    f"of {cls} in {path} directory")
        return True

    def all_file_paths(self):
        "Iterates through all files in the group and returns their file paths"
        if self.fs_path is None:
            raise ArcanaUsageError(
                f"Attempting to access file paths of {self} before they are set")
        return chain(*((Path(root) / f for f in files)
                        for root, _, files in os.walk(self.fs_path)))

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
            copy_dir = os.symlink
        else:
            copy_dir = shutil.copytree
        copy_dir(self.fs_path, path)
        return self.from_paths(path)[0]