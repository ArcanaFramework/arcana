from builtins import object
import os
import os.path as op
from collections import defaultdict
from arcana2.exceptions import (
    ArcanaUsageError, ArcanaNoConverterError, ArcanaFileFormatError,
    ArcanaNameError)
from nipype.interfaces.utility import IdentityInterface
from arcana2.utils.interfaces import (
    ZipDir, UnzipDir, TarGzDir, UnTarGzDir)
from arcana2.utils import split_extension
import logging


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
    resource_names : Dict[str, List[str]]
        A dictionary mapping the name of a dataset type to a list of
        alternate names to use to load the file format with (when the format is
        saved by format name, e.g. XNAT, instead of a file with an extension)
    """

    def __init__(self, name, extension=None, desc='',
                 directory=False, within_dir_exts=None,
                 aux_files=None, resource_names=None):
        if not name.islower():
            raise ArcanaUsageError(
                "All data format names must be lower case ('{}')"
                .format(name))
        if extension is None and not directory:
            raise ArcanaUsageError(
                "Extension for '{}' format can only be None if it is a "
                "directory".format(name))
        self._name = name
        self._extension = extension
        self._desc = desc
        self._directory = directory
        if within_dir_exts is not None:
            if not directory:
                raise ArcanaUsageError(
                    "'within_dir_exts' keyword arg is only valid "
                    "for directory data formats, not '{}'".format(name))
            within_dir_exts = frozenset(within_dir_exts)
        self._within_dir_exts = within_dir_exts
        self._converters = {}
        self._resource_names = (resource_names
                                if resource_names is not None else {})
        self._aux_files = aux_files if aux_files is not None else {}
        for sc_name, sc_ext in self.aux_files.items():
            if sc_ext == self.ext:
                raise ArcanaUsageError(
                    "Extension for side car '{}' cannot be the same as the "
                    "primary file ('{}')".format(sc_name, sc_ext))

    def __eq__(self, other):
        try:
            return (
                self._name == other._name
                and self._extension == other._extension
                and self._desc == other._desc
                and self._directory == other._directory
                and self._within_dir_exts ==
                other._within_dir_exts
                and self._resource_names == other._resource_names
                and self.aux_files == other.aux_files)
        except AttributeError:
            return False

    def __hash__(self):
        return (
            hash(self._name)
            ^ hash(self._extension)
            ^ hash(self._desc)
            ^ hash(self._directory)
            ^ hash(self._within_dir_exts)
            ^ hash(tuple((repo_type, tuple(self._resource_names[repo_type]))
                         for repo_type in sorted(self._resource_names)))
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
    def name(self):
        return self._name

    @property
    def extension(self):
        return self._extension

    @property
    def extensions(self):
        return tuple([self._extension] + sorted(self.aux_file_exts))

    @property
    def ext(self):
        return self.extension

    @property
    def ext_str(self):
        return self.extension if self.extension is not None else ''

    @property
    def desc(self):
        return self._desc

    @property
    def directory(self):
        return self._directory

    @property
    def aux_files(self):
        return self._aux_files

    def resource_names(self, repo_type):
        """
        Names of resources used to store the format on a given repository type.
        Defaults to the name of the name of the format
        """
        try:
            names = self._resource_names[repo_type]
        except KeyError:
            names = [self.name, self.name.upper()]
        return names

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
        return frozenset(self._aux_files.values())

    @property
    def within_dir_exts(self):
        return self._within_dir_exts

    def converter_from(self, file_format, **kwargs):
        if file_format == self:
            return IdentityConverter(file_format, self)
        try:
            matching_format, converter_cls = self._converters[file_format.name]
        except KeyError:
            raise ArcanaNoConverterError(
                "There is no converter to convert {} to {} (available: {})"
                .format(file_format, self,
                        ', '.join(
                            '{} <- {}'.format(k, v)
                            for k, v in self._converters.items())))
        if file_format != matching_format:
            raise ArcanaNoConverterError(
                "{} matches the name of a format that {} can be converted from"
                " but is not the identical".format(file_format,
                                                   matching_format))
        return converter_cls(file_format, self, **kwargs)

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

    def matches(self, file_group):
        """
        Checks to see whether the format matches the given file_group

        Parameters
        ----------
        file_group : FileGroup
            The file_group to check
        """
        if file_group._resource_name is not None:
            return (file_group._resource_name in self.resource_names(
                file_group.dataset.repository.type))
        elif self.directory:
            if op.isdir(file_group.path):
                if self.within_dir_exts is None:
                    return True
                else:
                    # Get set of all extensions in the directory
                    return self.within_dir_exts == frozenset(
                        split_extension(f)[1] for f in os.listdir(file_group.path)
                        if not f.startswith('.'))
            else:
                return False
        else:
            if op.isfile(file_group.path):
                all_paths = [file_group.path]
                if file_group._potential_aux_files is not None:
                    all_paths += file_group._potential_aux_files
                try:
                    primary_path = self.assort_files(all_paths)[0]
                except ArcanaFileFormatError:
                    return False
                else:
                    return primary_path == file_group.path
            else:
                return False

    def set_converter(self, file_format, converter):
        """
        Register a Converter and the FileFormat that it is able to convert from

        Parameters
        ----------
        converter : Converter
            The converter to register
        file_format : FileFormat
            The file format that can be converted into this format
        """
        self._converters[file_format.name] = (file_format, converter)

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


class IdentityConverter(Converter):

    requirements = []
    interface = IdentityInterface(['i'])
    input = 'i'
    output = 'i'


class UnzipConverter(Converter):

    interface = UnzipDir()
    mem_gb = 12
    input = 'zipped'
    output = 'unzipped'


class ZipConverter(Converter):

    interface = ZipDir()
    mem_gb = 12
    input = 'dirname'
    output = 'zipped'


class TarGzConverter(Converter):

    interface = TarGzDir()
    mem_gb = 12
    input = 'dirname'
    output = 'zipped'


class UnTarGzConverter(Converter):

    interface = UnTarGzDir()
    mem_gb = 12
    input = 'gzipped'
    output = 'gunzipped'


# General formats
directory_format = FileFormat(name='directory', extension=None, directory=True)
text_format = FileFormat(name='text', extension='.txt')
json_format = FileFormat(name='json', extension='.json')

# Compressed formats
zip_format = FileFormat(name='zip', extension='.zip')
targz_format = FileFormat(name='targz', extension='.tar.gz')

standard_formats = [text_format, json_format, directory_format, zip_format,
                    targz_format]

# General image formats
gif_format = FileFormat(name='gif', extension='.gif')
png_format = FileFormat(name='png', extension='.png')
jpg_format = FileFormat(name='jpg', extension='.jpg')

# Document formats
pdf_format = FileFormat(name='pdf', extension='.pdf')

# Set Converters
directory_format.set_converter(zip_format, UnzipConverter)
directory_format.set_converter(targz_format, UnTarGzConverter)
targz_format.set_converter(directory_format, TarGzConverter)
zip_format.set_converter(directory_format, ZipConverter)
