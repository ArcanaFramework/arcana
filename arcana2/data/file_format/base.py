import os
import os.path as op
import shutil
import typing as ty
from abc import abstractmethod, ABCMeta
from collections import defaultdict
import numpy as np
from arcana2.utils import split_extension
import logging
from pydra import mark, Workflow
from pydra.engine.task import FunctionTask
from pydra.engine.specs import BaseSpec, SpecInfo
from arcana2.exceptions import (
    ArcanaFileFormatError, ArcanaUsageError, ArcanaNoConverterError)
from ..item import FileGroup


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
    aux_files : dict[str, str]
        A dictionary of side cars (e.g. header or NIfTI json side cars) aside
        from the primary file, along with their expected extension.
        Automatically they will be assumed to be located adjancent to the
        primary file, with the same base name and this extension. However, in
        the initialisation of the file_group, alternate locations can be specified
    alternate_names : List[str]
        A list of alternate names that might be used to refer to the format
        when saved in a repository
    file_group_cls : FileGroup
        The class that is used when the format of a file-group is resolved
    """

    def __init__(self, name, extension=None, desc='', directory=False,
                 within_dir_exts=None, aux_files=None, alternate_names=None,
                 file_group_cls=FileGroup):
        if not name.islower():
            raise ArcanaUsageError(
                "All data format names must be lower case ('{}')"
                .format(name))
        if extension is None and not directory:
            raise ArcanaUsageError(
                "Extension for '{}' format can only be None if it is a "
                "directory".format(name))
        self.name = name
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
        self.file_group_cls = file_group_cls
        self.alternate_names = alternate_names
        self.aux_files = aux_files if aux_files is not None else {}
        for sc_name, sc_ext in self.aux_files.items():
            if sc_ext == self.ext:
                raise ArcanaUsageError(
                    "Extension for side car '{}' cannot be the same as the "
                    "primary file ('{}')".format(sc_name, sc_ext))

    def __eq__(self, other):
        try:
            return (
                self._name == other._name
                and self.extension == other.extension
                and self.desc == other.desc
                and self.directory == other.directory
                and self._within_dir_exts ==
                other._within_dir_exts
                and self.alternate_names == other.alternate_names
                and self.aux_files == other.aux_files)
        except AttributeError:
            return False

    def __hash__(self):
        return (
            hash(self._name)
            ^ hash(self.extension)
            ^ hash(self.desc)
            ^ hash(self.directory)
            ^ hash(self._within_dir_exts)
            ^ hash(tuple(self.alternate_names))
            ^ hash(tuple(sorted(self.aux_files.items()))))

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

    @property
    def extensions(self):
        return tuple([self.extension] + sorted(self.aux_file_exts))

    @property
    def ext(self):
        return self.extension

    @property
    def ext_str(self):
        return self.extension if self.extension is not None else ''

    def default_aux_file_paths(self, primary_path):
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
        aux_paths : dict[str, str]
            A dictionary of auxiliary file names and default paths
        """
        return dict((n, primary_path[:-len(self.ext)] + ext)
                    for n, ext in self.aux_files.items())

    @property
    def aux_file_exts(self):
        return frozenset(self.aux_files.values())

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
        return ['in_file'] + list(self.aux_files)

    def output_spec_fields(self):
        return ['out_file'] + list(self.aux_files)

    def set_converter(self, file_format, task, inputs=None, outputs=None,
                      **kwargs):
        """Creates a small workflow that maps the inputs of a task interface
        that actually performs the conversion the names used for the
        input and output auxiliary files by the file format objects.

        Parameters
        ----------
        file_format : FileFormater
            The file format to convert from
        task : pydra.engine.core.TaskBase
            The task that actually performs the conversion
        inputs : Dict[str, str]
            Maps the auxiliary file names in the format to convert from and
            the 'in_file' (for primary) onto the appropriate fields in the
            converter's input spec
        outputs : [type]
            Maps the auxiliary file names and 'in_file' (for primary) onto
            the appropriate fields in the converter's output spec
        """
        in_fields = ['in_file'] + list(file_format.aux_files)
        out_fields = ['out_file'] + list(self.aux_files)

        # Set defaults for inputs and outputs
        if inputs is None:
            inputs = {'in_file': 'in_file'}
        if outputs is None:
            outputs = {'out_file': 'out_file'}

        # Create a workflow to perform the conversion
        wf = Workflow(input_spec=in_fields)

        def collect_files(fformat: FileFormat, in_file: str,
                          aux_fields: ty.Sequence[str], **kwargs):
            """Copies files into the CWD renaming so the basenames match except
            for extensions"""
            shutil.copyfile(in_file, 'file' + fformat.extension)
            for name in aux_fields:
                shutil.copyfile(kwargs[name], 'file' + fformat.aux_files[name])
            return ['in_file'] + aux_fields

        # Add task collect the input paths to a common directory (as we assume
        # the converter expects)
        wf.add(
            FunctionTask(
                collect_files,
                input_spec=SpecInfo(
                    name='CollectFilesInputs', bases=(BaseSpec,), fields=(
                        [('fformat', FileFormat)]
                        + [(f, str) for f in in_fields])),
                output_spec=SpecInfo(
                    name='CollectFilesOutputs', bases=(BaseSpec,), fields=[
                        ('out_file', str)]))(
            name='collect_files',
            fformat=file_format,
            in_file=list(file_format.aux_files),
            **{f: getattr(wf.inputs.lzout, f) for f in in_fields}))

        # Add the actual converter node
        conv_kwargs = {inputs[f]:
                       getattr(wf.collect_files.lzout, f) for f in in_fields}
        conv_kwargs.update(kwargs)
        wf.add(task(name='converter', **conv_kwargs))

        # Set the outputs of the workflow
        wf.set_output(
            [(o, getattr(wf.converter.lzout, outputs[o]))
             for o in out_fields])

        # Save the converter for when it is required
        self._converters[file_format] = wf

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
        candidates : list[str]
            The list of filenames to assort

        Returns
        -------
        primary_file : str
            Path to the selected primary file
        aux_files : dict[str, str]
            A dictionary mapping the auxiliary file name to the selected path
        """
        by_ext = defaultdict(list)
        for path in candidates:
            by_ext[split_extension(path)[1].lower()].append(path)
        try:
            primary_file = by_ext[self.ext]
        except KeyError:
            raise ArcanaFileFormatError(
                "No files match primary file extension of {} out of "
                "potential candidates of {}"
                .format(self, "', '".join(candidates)))
        if not primary_file:
            raise ArcanaFileFormatError(
                "No potential files for primary file of {}".format(self))
        elif len(primary_file) > 1:
            raise ArcanaFileFormatError(
                "Multiple potential files for '{}' primary file of {}"
                .format("', '".join(primary_file), self))
        else:
            primary_file = primary_file[0]
        aux_files = {}
        for aux_name, aux_ext in self.aux_files.items():
            aux = by_ext[aux_ext]
            if not aux:
                raise ArcanaFileFormatError(
                    "No files match auxiliary file extension '{}' of {} out of"
                    " potential candidates of {}"
                    .format(aux_ext, self, "', '".join(candidates)))
            elif len(aux) > 1:
                raise ArcanaFileFormatError(
                    ("Multiple potential files for '{}' auxiliary file ext. "
                     + "({}) of {}".format("', '".join(aux), self)))
            else:
                aux_files[aux_name] = aux[0]
        return primary_file, aux_files

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
    #             file_group.dataset.repository.type))
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
    #             if file_group._potential_aux_files is not None:
    #                 all_paths += file_group._potential_aux_files
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
        self._file_format.aux_file_exts(self._aux_name)

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


class Converter(object):
    """
    Base class for all Arcana data format converters

    Parameters
    ----------
    input_format : FileFormat
        The input format to convert from
    output_format : FileFormat
        The output format to convert to
    """

    requirements = []

    def __init__(self, input_format, output_format, wall_time=None,
                 mem_gb=None):
        self._input_format = input_format
        self._output_format = output_format
        self._wall_time = wall_time
        self._mem_gb = mem_gb

    def __eq__(self, other):
        return (self.input_format == self.input_format
                and self._output_format == other.output_format)

    @property
    def input_format(self):
        return self._input_format

    @property
    def output_format(self):
        return self._output_format

    @property
    def interface(self):
        # To be overridden by subclasses
        return NotImplementedError

    @property
    def input(self):
        # To be overridden by subclasses
        return NotImplementedError

    @property
    def output(self):
        # To be overridden by subclasses
        return NotImplementedError

    @property
    def output_aux_files(self):
        return {}

    def output_aux(self, aux_name):
        try:
            return self.output_aux_files[aux_name]
        except KeyError:
            raise ArcanaNameError(
                aux_name,
                "No auxiliary file in output format {} named '{}".format(
                    self.output_format, aux_name))

    @property
    def mem_gb(self):
        return self._mem_gb

    @property
    def wall_time(self):
        return self._wall_time

    def __repr__(self):
        return "{}(input_format={}, output_format={})".format(
            type(self).__name__, self.input_format, self.output_format)
