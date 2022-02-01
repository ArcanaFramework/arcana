import os.path as op
from copy import copy
from abc import abstractmethod, ABCMeta
from collections import defaultdict
from pathlib import Path
import numpy as np
import attr
from typing import Any, Dict
from pydra import Workflow
from pydra.engine.core import TaskBase
from ..utils import func_task
import logging
from arcana.exceptions import (
    ArcanaFileFormatError, ArcanaUsageError, ArcanaNoConverterError)
import arcana.core.data.item


logger = logging.getLogger('arcana')


class FileFormat(object):
    """
    Defines a format for a file group (e.g. DICOM, NIfTI, Matlab file)

    Parameters
    ----------
    name : str
        A name for the data format
    extension : str
        The extension of the format
    desc : str
        A description of what the format is and ideally a link to its
        documentation
    directory : bool
        Whether the format is a directory or a file
    within_dir_exts : List[str]
        A list of extensions that are found within the top level of
        the directory (for directory formats). Used to identify
        formats from paths.
    side_cars : ty.Dict[str, str]
        A dictionary of side cars (e.g. header or NIfTI json side cars) aside
        from the primary file, along with their expected extension.
        Automatically they will be assumed to be located adjancent to the
        primary file, with the same base name and this extension. However, in
        the initialisation of the file_group, alternate locations can be specified
    alternate_names : List[str]
        A list of alternate names that might be used to refer to the format
        when saved in a store
    file_group_cls : FileGroup
        The class that is used when the format of a file-group is resolved
    """

    def __init__(self, name, extension=None, desc='', directory=False,
                 within_dir_exts=None, side_cars=None, alternate_names=None,
                 file_group_cls=None):
        if not name.islower():
            raise ArcanaUsageError(
                "All data format names must be lower case ('{}')"
                .format(name))
        if extension is None and not directory:
            raise ArcanaUsageError(
                "Extension for '{}' format can only be None if it is a "
                "directory".format(name))
        self.name = name.lower()
        self.extension = extension
        self.desc = desc
        self.directory = directory
        if within_dir_exts is not None:
            if not directory:
                raise ArcanaUsageError(
                    "'within_dir_exts' keyword arg is only valid "
                    "for directory data formats, not '{}'".format(name))
            within_dir_exts = frozenset(within_dir_exts)
        self._within_dir_exts = within_dir_exts
        self._converters = {}
        if alternate_names is None:
            alternate_names = []
        self.file_group_cls = (file_group_cls if file_group_cls is not None
                               else arcana.core.data.item.FileGroup)
        self.alternate_names = [n.lower() for n in alternate_names]
        self.side_cars = side_cars if side_cars is not None else {}
        for sc_name, sc_ext in self.side_cars.items():
            if sc_ext == self.ext:
                raise ArcanaUsageError(
                    "Extension for side car '{}' cannot be the same as the "
                    "primary file ('{}')".format(sc_name, sc_ext))

    def __eq__(self, other):
        try:
            return (
                self.name == other.name
                and self.extension == other.extension
                and self.desc == other.desc
                and self.directory == other.directory
                and self._within_dir_exts ==
                other._within_dir_exts
                and self.alternate_names == other.alternate_names
                and self.side_cars == other.side_cars)
        except AttributeError:
            return False

    def __hash__(self):
        if not self.__dict__:
            return hash(type(self))
        return (
            hash(self.name)
            ^ hash(self.extension)
            ^ hash(self.desc)
            ^ hash(self.directory)
            ^ hash(self._within_dir_exts)
            ^ hash(tuple(self.alternate_names))
            ^ hash(tuple(sorted(self.side_cars.items()))))

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return ("FileFormat(name='{}', extension='{}', directory={}{})"
                .format(self.name, self.extension, self.directory,
                        (', within_dir_extension={}'.format(
                            self.within_dir_exts)
                         if self.directory else '')))

    def __str__(self):
        return self.name

    def __call__(self, *args, **kwargs):
        """Temporary workaround until FileFormat is retired in place of using
        separate subclasses for each type"""
        from .item import FileGroup
        return FileGroup(*args, datatype=self, **kwargs)

    @property
    def all_names(self):
        return [self.name] + self.alternate_names

    @property
    def extensions(self):
        return tuple([self.extension] + sorted(self.side_car_exts))

    @property
    def ext(self):
        return self.extension

    @property
    def ext_str(self):
        return self.extension if self.extension is not None else ''

    def default_side_cars(self, primary_path):
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
        return dict((n, str(primary_path)[:-len(self.ext)] + ext)
                    for n, ext in self.side_cars.items())

    @property
    def side_car_exts(self):
        return frozenset(self.side_cars.values())

    @property
    def within_dir_exts(self):
        return self._within_dir_exts

    @classmethod
    def aux_interface_name(self, file_group_name, aux_name):
        """Standardised method for naming aux files in Pydra Task/Workflow
        interfaces

        Parameters
        ----------
        file_group_name : str
            Name of the file group in the task/workflow interface
        aux_name : str
            The name of the aux file

        Returns
        -------
        str
            The combined name for the aux file within an interface
        """
        return f"{file_group_name}___{aux_name}"

    def converter(self, file_format):
        try:
            return self._converters[file_format]
        except KeyError:
            raise ArcanaNoConverterError(
                f"No converter set for conversion between {self} and "
                f"{file_format}")

    def input_spec_fields(self):
        return ['in_file'] + list(self.side_cars)

    def output_spec_fields(self):
        return ['out_file'] + list(self.side_cars)

    def set_converter(self, file_format, task, inputs=None,
                      outputs=None, **default_kwargs):
        """Creates a small workflow that takes a file group of format
        `file_format` and converts it into a file-group of format `self`.
        Wraps up an existing task interface (e.g. Dcm2Nixx or MRConvert)
        and provides mapping from the auxiliary files of a file-group to
        the input/output interface of the converter.

        Parameters
        ----------
        file_format : FileFormater
            The file format to convert from
        task : pydra.TaskBase
            The task that actually performs the conversion
        inputs : Dict[str, str]
            Maps the auxiliary file names in the format to convert from and
            the 'in_file' (for primary) onto the appropriate fields in the
            converter's input spec
        outputs : Dict[str, str]
            Maps the auxiliary file names and 'in_file' (for primary) onto
            the appropriate fields in the converter's output spec
        **default_kwargs
            Keyword arguments passed through to the task interface on
            initialisation
        """
        if inputs is None:
            inputs = {'primary': 'in_file'}
        if outputs is None:
            outputs = {'primary': 'out_file'}
        # Save the converter for when it is required
        self._converters[file_format] = FileGroupConverter(
            from_format=file_format,
            to_format=self,
            task=task,
            inputs=inputs,
            outputs=outputs,
            task_kwargs=default_kwargs)

    @property
    def convertable_from(self):
        """
        A list of formats that the current format can be converted from
        """
        return (f for f, _ in self._converters.values())

    def assort_files(self, candidates):
        """
        Assorts candidate files into primary and auxiliary (and ignored) files
        corresponding to the format by their file extensions. Can be overridden
        in specialised subclasses to assort files based on other
        characteristics

        Parameters
        ----------
        candidates : ty.List[str]
            The list of filenames to assort

        Returns
        -------
        primary_file : str
            Path to the selected primary file
        side_cars : dict[str, str]
            A dictionary mapping the auxiliary file name to the selected path
        """
        by_ext = defaultdict(list)
        candidates = list(candidates)  # protect against iterators
        for path in candidates:
            ext = ''.join(Path(path).suffixes).lower()
            if not ext:
                ext = None
            by_ext[ext].append(path)
        primary_file = by_ext[self.ext]
        if not primary_file:
            raise ArcanaFileFormatError(
                "No files match primary file extension of {} out of "
                "potential candidates of {}"
                .format(self, "', '".join(str(p) for p in candidates)))
        elif len(primary_file) > 1:
            raise ArcanaFileFormatError(
                "Found multiple potential files for primary file of {} ('{}')"
                .format(self, "', '".join(str(p) for p in primary_file)))
        else:
            primary_file = primary_file[0]
        side_cars = {}
        for sc_name, sc_ext in self.side_cars.items():
            side_car = by_ext[sc_ext]
            if not side_car:
                raise ArcanaFileFormatError(
                    "No files match auxiliary file extension '{}' of {} out of"
                    " potential candidates of {}"
                    .format(sc_ext, self,
                            "', '".join(str(p) for p in candidates)))
            elif len(side_car) > 1:
                raise ArcanaFileFormatError(
                    ("Multiple potential files for '{}' auxiliary file ext. "
                     + "({}) of {}".format("', '".join(side_car), self)))
            else:
                side_cars[sc_name] = side_car[0]
        return primary_file, side_cars

    # def matches(self, file_group):
    #     """
    #     Checks to see whether the format matches the given file_group

    #     Parameters
    #     ----------
    #     file_group : FileGroup
    #         The file_group to check
    #     """
    #     if file_group._format_name is not None:
    #         return (file_group._format_name in self.alternate_names(
    #             file_group.dataset.store.type))
    #     elif self.directory:
    #         if op.isdir(file_group.path):
    #             if self.within_dir_exts is None:
    #                 return True
    #             else:
    #                 # Get set of all extensions in the directory
    #                 return self.within_dir_exts == frozenset(
    #                     split_extension(f)[1] for f in os.listdir(file_group.path)
    #                     if not f.startswith('.'))
    #         else:
    #             return False
    #     else:
    #         if op.isfile(file_group.path):
    #             all_paths = [file_group.path]
    #             if file_group._potential_side_cars is not None:
    #                 all_paths += file_group._potential_side_cars
    #             try:
    #                 primary_path = self.assort_files(all_paths)[0]
    #             except ArcanaFileFormatError:
    #                 return False
    #             else:
    #                 return primary_path == file_group.path
    #         else:
    #             return False

    # def converter_from(self, format):
    #     if format == self:
    #         return mark.task(lambda x: x)
    #     else:
    #         raise ArcanaConverterNotAvailableError(
    #             f"Cannot convert between {self} and {format} formats")

    def aux(self, aux_name):
        """
        Returns a FileFormatAuxFile that points directly to the auxiliary file
        """
        return FileFormatAuxFile(self, aux_name)

    def from_path(self, file_path, **kwargs):
        """Creates a FileGroup object from a path on the file-system

        Parameters
        ----------
        file_path : str
            Path to primary file or directory of the file-group

        Returns
        -------
        FileGroup
            The file-group constructed from the local path

        Raises
        ------
        ArcanaUsageError
            If path does not exists
        ArcanaNameError
            [description]
        """
        name_path = Path(file_path).name
        if self.ext is not None:
            if name_path.endswith(self.ext):
                name_path = name_path[:-len(self.ext)]
            else:
                file_path += self.ext       
        return self.file_group_cls(name_path, fs_path=file_path, datatype=self,
                                   **kwargs)


class FileFormatAuxFile(object):
    """
    A thin wrapper around a FileFormat to point to a specific auxiliary file
    that sets the extension to be that of the auxiliary file but passes all
    other calls to the wrapped format.

    Parameters
    ----------
    file_format : FileFormat
        The file format to wrap
    aux_name : str
        The name of the auxiliary file to point to
    """

    def __init__(self, file_format, aux_name):
        self._file_format = file_format
        self._aux_name = aux_name

    @property
    def extension(self):
        self._file_format.side_car_exts(self._aux_name)

    @property
    def aux_name(self):
        return self._aux_name

    def __repr__(self):
        return ("FileFormatAuxFile(aux_name='{}', format={})"
                .format(self.aux_name, self._file_format))

    def __getattr__(self, attr):
        return getattr(self._file_format, attr)


class Image(FileFormat, metaclass=ABCMeta):

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
        if other_fileset.datatype != self:
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
                raise ArcanaUsageError(
                    "Doesn't make sense to provide both 'include_keys' ({}) "
                    "and ignore_keys ({}) to headers_equal method"
                    .format(include_keys, ignore_keys))
            include_keys &= hdr_keys
        elif ignore_keys is not None:
            include_keys = hdr_keys - set(ignore_keys)
        else:
            if self.INCLUDE_HDR_KEYS is not None:
                if self.IGNORE_HDR_KEYS is not None:
                    raise ArcanaUsageError(
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
                            # Fallback to a straight comparison for some datatypes
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


@attr.s
class FileGroupConverter:

    from_format: FileFormat = attr.ib()
    to_format: FileFormat = attr.ib()
    task: TaskBase = attr.ib()
    inputs: Dict[str, str] = attr.ib()
    outputs: Dict[str, str] = attr.ib()
    task_kwargs: Dict[str, Any] = attr.ib(factory=dict)

    @inputs.default
    def inputs_default(self):
        return {'primary': 'in_file'}

    @outputs.default
    def outputs_default(self):
        return {'primary': 'out_file'}

    @inputs.validator
    def inputs_validator(self, _, inputs):
        if 'primary' not in inputs:
            raise ArcanaUsageError(
                f"'primary' path must be present in converter inputs ({inputs.keys()})")

    @outputs.validator
    def outputs_validator(self, _, outputs):
        if 'primary' not in outputs:
            raise ArcanaUsageError(
                f"'primary' path must be present in converter outputs ({outputs.keys()})")
    
    def __call__(self, name, **kwargs):
        """
        Create a Pydra workflow to convert a file group from one format to
        another
        """
        from .item import FileGroup
        wf = Workflow(name=name,
                      input_spec=['to_convert'],
                      **kwargs)

        # Add task collect the input paths to a common directory (as we
        # assume the converter expects)
        wf.add(func_task(
            extract_paths,
            in_fields=[('from_format', type), ('file_group', FileGroup)],
            out_fields=[(i, str) for i in self.inputs],
            name='extract_paths',
            from_format=self.from_format,
            file_group=wf.lzin.to_convert))

        # Add the actual converter node
        conv_kwargs = copy(self.task_kwargs)
        conv_kwargs.update(kwargs)
        # Map 
        conv_kwargs.update((self.inputs[i], getattr(wf.extract_paths.lzout, i))
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

        # Set the outputs of the workflow
        wf.set_output(('converted', wf.encapsulate_paths.lzout.converted))

        return wf


def extract_paths(from_format, file_group):
    """Copies files into the CWD renaming so the basenames match
    except for extensions"""
    logger.debug("Extracting paths from %s (%s format) before conversion", file_group, from_format)
    if file_group.datatype != from_format:
        raise ValueError(f"Format of {file_group} doesn't match converter {from_format}")
    cpy = file_group.copy_to(Path(file_group.path).name, symlink=True)
    paths = (cpy.fs_path,) + tuple(cpy.side_cars.values())
    return paths if len(paths) > 1 else paths[0]


def encapsulate_paths(to_format, primary, **side_car_paths):
    """Copies files into the CWD renaming so the basenames match
    except for extensions"""
    logger.debug("Encapsulating %s and %s into %s format after conversion",
                 primary, side_car_paths, to_format)
    return to_format.from_path(primary, side_cars=side_car_paths)
