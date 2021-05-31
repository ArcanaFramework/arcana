from __future__ import absolute_import
from builtins import next
from builtins import range
import os
import math
import os.path as op
import re
import shutil
from nipype.interfaces.base import (
    TraitedSpec, traits, BaseInterface, File,
    Directory, CommandLineInputSpec, CommandLine, DynamicTraitedSpec,
    BaseInterfaceInputSpec, isdefined)
from arcana2.exceptions import ArcanaUsageError
from itertools import chain, groupby
from nipype.interfaces.utility.base import Merge, MergeInputSpec
from nipype.interfaces.utility import IdentityInterface
from nipype.interfaces.io import IOBase, add_traits
from nipype.utils.filemanip import filename_to_list
from nipype.interfaces.base import OutputMultiPath, InputMultiPath
import numpy as np
from arcana2.exceptions import ArcanaError, ArcanaDesignError
from .base import split_extension


bash_resources = op.abspath(op.join(op.dirname(__file__), 'resources', 'bash'))

zip_path = op.join(bash_resources, 'zip.sh')
targz_path = op.join(bash_resources, 'targz.sh')
cp_path = op.join(bash_resources, 'copy_file.sh')
cp_dir_path = op.join(bash_resources, 'copy_dir.sh')
mkdir_path = op.join(bash_resources, 'make_dir.sh')


special_char_re = re.compile(r'[^\w]')


# class MergeInputSpec(DynamicTraitedSpec, BaseInterfaceInputSpec):
#     axis = traits.Enum(
#         'vstack', 'hstack', usedefault=True,
#         desc=('direction in which to merge, hstack requires same number '
#               'of elements in each input'))
#     no_flatten = traits.Bool(
#         False, usedefault=True,
#         desc='append to outlist instead of extending in vstack mode')


# class MergeOutputSpec(TraitedSpec):
#     out = traits.List(desc='Merged output')


# class Merge(IOBase):
#     """Basic interface class to merge inputs into a single list

#     Examples
#     --------

#     >>> from nipype.interfaces.utility import Merge
#     >>> mi = Merge(3)
#     >>> mi.inputs.in1 = 1
#     >>> mi.inputs.in2 = [2, 5]
#     >>> mi.inputs.in3 = 3
#     >>> out = mi.run()
#     >>> out.outputs.out
#     [1, 2, 5, 3]

#     """
#     input_spec = MergeInputSpec
#     output_spec = MergeOutputSpec

#     def __init__(self, numinputs=0, **inputs):
#         super(Merge, self).__init__(**inputs)
#         self._numinputs = numinputs
#         if numinputs > 0:
#             input_names = ['in%d' % (i + 1) for i in range(numinputs)]
#         elif numinputs == 0:
#             input_names = ['in_lists']
#         else:
#             input_names = []
#         add_traits(self.inputs, input_names)

#     def _list_outputs(self):
#         outputs = self._outputs().get()
#         out = []

#         if self._numinputs == 0:
#             values = getattr(self.inputs, 'in_lists')
#             if not isdefined(values):
#                 return outputs
#         else:
#             getval = lambda idx: getattr(self.inputs, 'in%d' % (idx + 1))
#             values = [getval(idx) for idx in range(self._numinputs)
#                       if isdefined(getval(idx))]

#         if self.inputs.axis == 'vstack':
#             for value in values:
#                 if isinstance(value, list) and not self.inputs.no_flatten:
#                     out.extend(value)
#                 else:
#                     out.append(value)
#         else:
#             lists = [filename_to_list(val) for val in values]
#             out = [[val[i] for val in lists] for i in range(len(lists[0]))]
#         if out:
#             outputs['out'] = out
#         return outputs


class MergeTupleOutputSpec(TraitedSpec):
    # Not possible to specify variable length tuples given Nipype traits
    # extensions
    out = traits.Any(desc='Merged output')  # @UndefinedVariable


class MergeTuple(Merge):
    """Basic interface class to merge inputs into a single tuple

    Examples
    --------

    >>> from nipype.interfaces.utility import Merge
    >>> mi = MergeTuple(3)
    >>> mi.inputs.in1 = 1
    >>> mi.inputs.in2 = [2, 5]
    >>> mi.inputs.in3 = 3
    >>> out = mi.run()
    >>> out.outputs.out
    (1, 2, 5, 3)

    """
    input_spec = MergeInputSpec
    output_spec = MergeTupleOutputSpec

    def _list_outputs(self):
        outputs = super(MergeTuple, self)._list_outputs()
        outputs['out'] = tuple(outputs['out'])
        return outputs


class Chain(IdentityInterface):

    def _list_outputs(self):
        outputs = super(Chain, self)._list_outputs()
        chained_outputs = {}
        for k, v in outputs.items():
            chained_outputs[k] = list(chain(*v))
        return chained_outputs


def dicom_fname_sort_key(fname):
    in_parts = special_char_re.split(op.basename(fname))
    out_parts = []
    for part in in_parts:
        try:
            part = int(part)
        except ValueError:
            pass
        out_parts.append(part)
    return tuple(out_parts)


class JoinPathInputSpec(TraitedSpec):
    dirname = Directory(mandatory=True, desc='directory name')
    filename = traits.Str(mandatory=True, desc='file name')


class JoinPathOutputSpec(TraitedSpec):
    path = traits.Str(mandatory=True, desc="The joined path")


class JoinPath(BaseInterface):
    """Joins a filename to a directory name"""

    input_spec = JoinPathInputSpec
    output_spec = JoinPathOutputSpec

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['path'] = op.join(self.inputs.dirname,
                                  self.inputs.filename)
        return outputs

    def _run_interface(self, runtime):
        return runtime


class CopyFileInputSpec(CommandLineInputSpec):
    src = File(mandatory=True, desc='source file', argstr='%s',
               position=0)
    base_dir = Directory(mandatory=True, desc='root directory', argstr='%s',
                         position=1)
    dst = File(genfile=True, argstr='%s', position=2,
               desc=("The destination file"))


class CopyFileOutputSpec(TraitedSpec):
    copied = File(exists=True, desc="The copied file")
    basedir = Directory(exists=True, desc='base directory')


class CopyFile(CommandLine):
    """Creates a copy of a given file"""

    _cmd = cp_path
    input_spec = CopyFileInputSpec
    output_spec = CopyFileOutputSpec

    def _list_outputs(self):
        outputs = self._outputs().get()

        outputs['copied'] = op.join(self.inputs.base_dir, self.inputs.dst)
        outputs['basedir'] = op.join(self.inputs.base_dir)
        return outputs

    def _gen_filename(self, name):
        if name == 'copied':
            fname = op.basename(self.inputs.dst)
        else:
            assert False
        return fname


class CopyDirInputSpec(CommandLineInputSpec):
    src = File(mandatory=True, desc='source file', argstr='%s',
               position=0)
    base_dir = Directory(mandatory=True, desc='root directory', argstr='%s',
                         position=1)
    dst = File(genfile=True, argstr='%s', position=2,
               desc=("The destination file"))
    method = traits.Int(mandatory=True, desc='method', argstr='%s', position=3)


class CopyDirOutputSpec(TraitedSpec):
    copied = Directory(exists=True, desc="The copied file")
    basedir = Directory(exists=True, desc='base directory')


class CopyDir(CommandLine):
    """Creates a copy of a given file"""

    _cmd = cp_dir_path
    input_spec = CopyDirInputSpec
    output_spec = CopyDirOutputSpec

    def _list_outputs(self):
        outputs = self._outputs().get()
        if self.inputs.method == 1:
            outputs['copied'] = op.join(self.inputs.base_dir)
            outputs['basedir'] = op.join(self.inputs.base_dir)
        elif self.inputs.method == 2:
            outputs['copied'] = op.join(self.inputs.base_dir,
                                        self._gen_filename('copied'))
            outputs['basedir'] = op.join(self.inputs.base_dir)
        return outputs

    def _gen_filename(self, name):
        if name == 'copied':
            fname = op.basename(self.inputs.dst)
        else:
            assert False
        return fname


class MakeDirInputSpec(CommandLineInputSpec):
    base_dir = Directory(mandatory=True, desc='root directory', argstr='%s',
                         position=0)
    name_dir = Directory(genfile=True, argstr='%s', position=1,
                         desc=("name of the new directory"))


class MakeDirOutputSpec(TraitedSpec):
    new_dir = Directory(exists=True, desc="The created directory")


class MakeDir(CommandLine):
    """Creates a new directory"""

    _cmd = mkdir_path
    input_spec = MakeDirInputSpec
    output_spec = MakeDirOutputSpec

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['new_dir'] = op.join(self.inputs.base_dir)
#                                           self._gen_filename('new_dir'))
        return outputs

    def _gen_filename(self, name):
        if name == 'new_dir':
            fname = op.basename(self.inputs.name_dir)
        else:
            assert False
        return fname


class ZipDirInputSpec(CommandLineInputSpec):
    dirname = Directory(mandatory=True, desc='directory name', argstr='%s',
                        position=1)
    zipped = File(genfile=True, argstr='%s', position=0,
                  desc=("The zipped zip file"))
    ext_prefix = traits.Str(
        mandatory=False, default='', usedefault=True,
        desc=("Extra extension to prepend before .zip is appended to "
              "file name"))


class ZipDirOutputSpec(TraitedSpec):
    zipped = File(exists=True, desc="The zipped directory")


class ZipDir(CommandLine):
    """Creates a zip repository from a given folder"""

    _cmd = zip_path
    input_spec = ZipDirInputSpec
    output_spec = ZipDirOutputSpec
    zip_ext = '.zip'

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['zipped'] = op.abspath(
            self._gen_filename('zipped'))
        return outputs

    def _gen_filename(self, name):
        if name == 'zipped':
            if isdefined(self.inputs.zipped):
                fname = self.inputs.zipped
            else:
                fname = (op.basename(self.inputs.dirname) +
                         self.inputs.ext_prefix + self.zip_ext)
        else:
            assert False
        return fname


class UnzipDirInputSpec(CommandLineInputSpec):
    zipped = Directory(mandatory=True, desc='zipped file name', argstr='%s',
                       position=0)


class UnzipDirOutputSpec(TraitedSpec):
    unzipped = Directory(exists=True, desc="The unzipped directory")


class UnzipDir(CommandLine):
    """Unzips a folder that was zipped by ZipDir"""

    _cmd = 'unzip -qo'
    input_spec = UnzipDirInputSpec
    output_spec = UnzipDirOutputSpec

    def _run_interface(self, *args, **kwargs):
        self.listdir_before = set(os.listdir(os.getcwd()))
        return super(UnzipDir, self)._run_interface(*args, **kwargs)

    def _list_outputs(self):
        outputs = self._outputs().get()
        new_files = set(os.listdir(os.getcwd())) - self.listdir_before
        if len(new_files) > 1:
            raise ArcanaUsageError(
                "Zip repositorys can only contain a single directory, found "
                "'{}'".format("', '".join(new_files)))
        try:
            unzipped = next(iter(new_files))
        except StopIteration:
            raise ArcanaUsageError(
                "No files or directories found in unzipped directory")
        outputs['unzipped'] = op.join(os.getcwd(), unzipped)
        return outputs


class CopyToDirInputSpec(TraitedSpec):
    in_files = traits.List(
        traits.Either(File(exists=True), Directory(exists=True)),
        mandatory=True, desc='input dicom files')
    out_dir = Directory(desc='the output dicom file')
    use_symlinks = traits.Bool(
        default=True,
        desc=("Whether it is okay to symlink the inputs into the directory "
              "instead of copying them"), usedefault=True)
    file_names = traits.List(
        traits.Str, desc=("The filenames to use to save the files with within "
                          "the directory"))


class CopyToDirOutputSpec(TraitedSpec):
    out_dir = Directory(exists=True, desc='the output dicom directory')
    file_names = traits.List(
        traits.Str, desc="the files/directories copied to the new directory")


class CopyToDir(BaseInterface):
    """
    Copies a list of files of directories into a directory. By default the
    input files/directories will only be symlinked into the output directory
    for performance reasons.
    """

    input_spec = CopyToDirInputSpec
    output_spec = CopyToDirOutputSpec

    def _run_interface(self, runtime):
        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        dirname = self.out_dir
        os.makedirs(dirname)
        num_files = len(self.inputs.in_files)
        if isdefined(self.inputs.file_names):
            if len(self.inputs.file_names) != num_files:
                raise ArcanaError(
                    "Number of provided filenames ({}) does not match number "
                    "of provided files ({})".format(
                        len(self.inputs.file_names), num_files))
            out_files = (op.basename(f) for f in self.inputs.file_names)
        else:
            # Create filenames that will sort ascendingly with the order the
            # file is inputed to the interface
            ndigits = int(math.ceil(math.log10(num_files)))
            out_files = []
            for i, fname in enumerate(self.inputs.in_files):
                ext = split_extension(fname)[1]
                if ext is None:
                    ext_str = ''
                else:
                    ext_str = ext
                out_files.append(str(i).zfill(ndigits) + ext_str)
        file_names = []
        for in_file, out_file in zip(self.inputs.in_files, out_files):
            out_path = op.join(self.out_dir, out_file)
            if self.inputs.use_symlinks:
                os.symlink(in_file, out_path)
            else:
                if op.isdir(in_file):
                    shutil.copytree(in_file, out_path)
                else:
                    shutil.copy(in_file, out_path)
            file_names.append(op.basename(out_path))
        outputs['out_dir'] = dirname
        outputs['file_names'] = file_names
        return outputs

    @property
    def out_dir(self):
        if isdefined(self.inputs.out_dir):
            dpath = self.inputs.out_dir
        else:
            dpath = op.abspath('store_dir')
        return dpath


class ListDirInputSpec(TraitedSpec):
    directory = File(mandatory=True, desc='directory to read')
    filter = traits.Callable(
        desc=("A callable (e.g. function) used to filter the filenames"))
    sort_key = traits.Callable(
        desc=("A callable (e.g. function) that generates a key from the "
              "listed filenames with which to sort them with"))
    group_key = traits.Callable(
        desc=("A callable (e.g. function) that generates a key with which to"
              "group the filenames"))


class ListDirOutputSpec(TraitedSpec):
    files = traits.List(File(exists=True),
                        desc='The files present in the directory')
    groups = traits.Dict(traits.Str, traits.List(File(exists=True)),
                         desc="The files grouped by the 'group_key' function")


class ListDir(BaseInterface):
    """
    Lists all files (not sub-directories) in a directory
    """

    input_spec = ListDirInputSpec
    output_spec = ListDirOutputSpec

    def _run_interface(self, runtime):
        return runtime

    def _list_outputs(self):
        dname = self.inputs.directory
        outputs = self._outputs().get()
        key = self.inputs.sort_key if isdefined(self.inputs.sort_key) else None
        files = []
        for fname in os.listdir(dname):
            path = op.join(dname, fname)
            if op.isfile(path) and (not isdefined(self.inputs.filter) or
                                    self.inputs.filter(fname)):
                files.append(fname)
        files = [op.join(dname, f) for f in sorted(files, key=key)]
        if isdefined(self.inputs.group_key):
            outputs['groups'] = dict(
                (k, list(values))
                for k, values in groupby(files, key=self.inputs.group_key))
        outputs['files'] = files
        return outputs


class TarGzDirInputSpec(CommandLineInputSpec):
    dirname = Directory(mandatory=True, desc='directory name', argstr='%s',
                        position=1)
    zipped = File(genfile=True, argstr='%s', position=0,
                  desc=("The tar_gz file"))


class TarGzDirOutputSpec(TraitedSpec):
    zipped = File(exists=True, desc="The tar_gz directory")


class TarGzDir(CommandLine):
    """Creates a tar_gzip repository from a given folder"""

    _cmd = targz_path
    input_spec = TarGzDirInputSpec
    output_spec = TarGzDirOutputSpec
    targz_ext = '.tar.gz'

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs['zipped'] = op.join(os.getcwd(), self._gen_filename('zipped'))
        return outputs

    def _gen_filename(self, name):
        if name == 'zipped':
            fname = op.basename(self.inputs.dirname) + self.targz_ext
        else:
            assert False
        return fname


class UnTarGzDirInputSpec(CommandLineInputSpec):

    gzipped = Directory(mandatory=True, argstr='%s', position=0,
                        desc=("The tar_gz file"))


class UnTarGzDirOutputSpec(TraitedSpec):
    gunzipped = Directory(exists=True, desc="The gunzipped directory")


class UnTarGzDir(CommandLine):
    """Unzip a folder created using TarGz"""

    _cmd = 'tar -zxvf '
    input_spec = UnTarGzDirInputSpec
    output_spec = UnTarGzDirOutputSpec

    def _run_interface(self, *args, **kwargs):
        self.listdir_before = set(os.listdir(os.getcwd()))
        return super(UnTarGzDir, self)._run_interface(*args, **kwargs)

    def _list_outputs(self):
        outputs = self._outputs().get()
        new_files = set(os.listdir(os.getcwd())) - self.listdir_before
        if len(new_files) > 1:
            raise ArcanaUsageError(
                "Zip repositorys can only contain a single directory, found "
                "'{}'".format("', '".join(new_files)))
        try:
            unzipped = next(iter(new_files))
        except StopIteration:
            raise ArcanaUsageError(
                "No files or directories found in unzipped directory")
        outputs['gunzipped'] = op.join(os.getcwd(), unzipped)
        return outputs


class SelectOneInputSpec(BaseInterfaceInputSpec):
    inlist = InputMultiPath(
        traits.Any, mandatory=True, desc='list of values to choose from')
    index = traits.Int(mandatory=True,
                       desc='0-based indice of element to extract')


class SelectOneOutputSpec(TraitedSpec):
    out = OutputMultiPath(traits.Any, desc='selected value')


class SelectOne(IOBase):
    """Basic interface class to select an element from a list"""

    input_spec = SelectOneInputSpec
    output_spec = SelectOneOutputSpec

    def _list_outputs(self):
        outputs = self._outputs().get()
        out = np.array(self.inputs.inlist)[self.inputs.index]
        outputs['out'] = out
        return outputs


class SelectSessionInputSpec(BaseInterfaceInputSpec):
    inlist = InputMultiPath(
        traits.Any, mandatory=True, desc='List of items to select from')
    subject_ids = traits.List(traits.Str, mandatory=True,
                              desc=('List of subject IDs corresponding to the '
                                    'provided items'))
    timepoint_ids = traits.List(traits.Str, mandatory=True,
                            desc=('List of timepoint IDs corresponding to the '
                                  'provided items'))
    subject_id = traits.Str(mandatory=True, desc='Subject ID')
    timepoint_id = traits.Str(mandatory=True, desc='Visit ID')


class SelectSessionOutputSpec(TraitedSpec):
    out = traits.Any(desc='selected value')


class SelectSession(IOBase):
    """Basic interface class to select session from a list"""

    input_spec = SelectSessionInputSpec
    output_spec = SelectSessionOutputSpec

    def _list_outputs(self):
        outputs = self._outputs().get()
        if len(self.inputs.subject_ids) != len(self.inputs.inlist):
            raise ArcanaDesignError(
                "Length of subject IDs ({}) doesn't match that of input items "
                "({})".format(len(self.inputs.subject_ids),
                              len(self.inputs.inlist)))
        if len(self.inputs.timepoint_ids) != len(self.inputs.inlist):
            raise ArcanaDesignError(
                "Length of timepoint IDs ({}) doesn't match that of input items "
                "({})".format(len(self.inputs.timepoint_ids),
                              len(self.inputs.inlist)))
        session_ids = list(zip(self.inputs.subject_ids, self.inputs.timepoint_ids))
        index = session_ids.index((self.inputs.subject_id,
                                   self.inputs.timepoint_id))
        outputs['out'] = self.inputs.inlist[index]
        return outputs
