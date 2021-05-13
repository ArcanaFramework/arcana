import os
import os.path as op
import errno
from itertools import chain
import stat
import shutil
import logging
import json
from fasteners import InterProcessLock
from arcana2.data import FileGroup, Field
from arcana2.data.provenance import Record
from arcana2.exceptions import (
    ArcanaError, ArcanaUsageError,
    ArcanaRepositoryError,
    ArcanaMissingDataException,
    ArcanaInsufficientRepoDepthError)
from arcana2.utils import get_class_info, HOSTNAME, split_extension
from .base import Repository


logger = logging.getLogger('arcana')


class FileSystem(Repository):
    """
    A 'Repository' class for data stored simply in file-system
    directories. Can be a single directory if it contains only one subject
    and visit, otherwise if sub-directories are present (that aren't
    recognised as single file_groups) then they are assumed to be
    separate subjects. For multi-visit datasets an additional
    layer of sub-directories for each visit is required within each
    subject sub-directory.
    """

    type = 'directory'
    SUMMARY_NAME = '__ALL__'
    FIELDS_FNAME = 'fields.json'
    PROV_DIR = '__prov__'
    LOCK_SUFFIX = '.lock'
    DEFAULT_SUBJECT_ID = 'SUBJECT'
    DEFAULT_VISIT_ID = 'VISIT'
    MAX_DEPTH = 2
    POSSIBLE_LEVELS = ('group', 'subject', 'visit')

    def __repr__(self):
        return "{}()".format(type(self).__name__)

    def __eq__(self, other):
        try:
            return self.type == other.type and self.root_dir == other.root_dir
        except AttributeError:
            return False

    @property
    def prov(self):
        return {
            'type': get_class_info(type(self)),
            'host': HOSTNAME}

    def __hash__(self):
        return hash(self.type)

    def standardise_name(self, name):
        return op.abspath(name)

    def get_file_group(self, file_group):
        """
        Set the path of the file_group from the repository
        """
        # Don't need to cache file_group as it is already local as long
        # as the path is set
        if file_group._path is None:
            primary_path = self.file_group_path(file_group)
            aux_files = file_group.format.default_aux_file_paths(primary_path)
            if not op.exists(primary_path):
                raise ArcanaMissingDataException(
                    "{} does not exist in {}"
                    .format(file_group, self))
            for aux_name, aux_path in aux_files.items():
                if not op.exists(aux_path):
                    raise ArcanaMissingDataException(
                        "{} is missing '{}' side car in {}"
                        .format(file_group, aux_name, self))
        else:
            primary_path = file_group.path
            aux_files = file_group.aux_files
        return primary_path, aux_files

    def get_field(self, field):
        """
        Update the value of the field from the repository
        """
        # Load fields JSON, locking to prevent read/write conflicts
        # Would be better if only checked if locked to allow
        # concurrent reads but not possible with multi-process
        # locks (in my understanding at least).
        fpath = self.fields_json_path(field)
        try:
            with InterProcessLock(fpath + self.LOCK_SUFFIX,
                                  logger=logger), open(fpath, 'r') as f:
                dct = json.load(f)
            val = dct[field.name]
            if field.array:
                val = [field.dtype(v) for v in val]
            else:
                val = field.dtype(val)
        except (KeyError, IOError) as e:
            try:
                # Check to see if the IOError wasn't just because of a
                # missing file
                if e.errno != errno.ENOENT:
                    raise
            except AttributeError:
                pass
            raise ArcanaMissingDataException(
                "{} does not exist in the local repository {}"
                .format(field.name, self))
        return val

    def put_file_group(self, file_group):
        """
        Inserts or updates a file_group in the repository
        """
        target_path = self.file_group_path(file_group)
        if op.isfile(file_group.path):
            shutil.copyfile(file_group.path, target_path)
            # Copy side car files into repository
            for aux_name, aux_path in file_group.format.default_aux_file_paths(
                    target_path).items():
                shutil.copyfile(file_group.format.aux_files[aux_name], aux_path)
        elif op.isdir(file_group.path):
            if op.exists(target_path):
                shutil.rmtree(target_path)
            shutil.copytree(file_group.path, target_path)
        else:
            assert False

    def put_field(self, field):
        """
        Inserts or updates a field in the repository
        """
        fpath = self.fields_json_path(field)
        # Open fields JSON, locking to prevent other processes
        # reading or writing
        with InterProcessLock(fpath + self.LOCK_SUFFIX, logger=logger):
            try:
                with open(fpath, 'r') as f:
                    dct = json.load(f)
            except IOError as e:
                if e.errno == errno.ENOENT:
                    dct = {}
                else:
                    raise
            if field.array:
                dct[field.name] = list(field.value)
            else:
                dct[field.name] = field.value
            with open(fpath, 'w') as f:
                json.dump(dct, f, indent=2)

    def put_record(self, record, dataset):
        fpath = self.prov_json_path(record, dataset)
        if not op.exists(op.dirname(fpath)):
            os.mkdir(op.dirname(fpath))
        record.save(fpath)

    # root_dir=None, all_from_analysis=None,
    def find_data(self, dataset, subject_ids=None, visit_ids=None, **kwargs):
        """
        Find all data within a repository, registering file_groups, fields and
        provenance with the found_file_group, found_field and found_provenance
        methods, respectively

        Parameters
        ----------
        subject_ids : list(str)
            List of subject IDs with which to filter the tree with. If
            None all are returned
        visit_ids : list(str)
            List of visit IDs with which to filter the tree with. If
            None all are returned
        root_dir : str
            The root dir to use instead of the 'name' (path) of the dataset.
            Only for use in sub-classes (e.g. BIDS)
        all_from_analysis : str
            Global 'from_analysis' to be applied to every found item.
            Only for use in sub-classes (e.g. BIDS)

        Returns
        -------
        file_groups : list[FileGroup]
            All the file_groups found in the repository
        fields : list[Field]
            All the fields found in the repository
        records : list[Record]
            The provenance records found in the repository
        """
        all_file_groups = []
        all_fields = []
        all_records = []
        # if root_dir is None:
        root_dir = dataset.name
        for session_path, dirs, files in os.walk(root_dir):
            relpath = op.relpath(session_path, root_dir)
            path_parts = relpath.split(op.sep) if relpath != '.' else []
            ids = self._extract_ids_from_path(dataset.depth, path_parts, dirs,
                                              files)
            if ids is None:
                continue
            subj_id, visit_id, from_analysis = ids
            # if all_from_analysis is not None:
            #     if from_analysis is not None:
            #         raise ArcanaRepositoryError(
            #             "Found from_analysis sub-directory '{}' when global "
            #             "from analysis '{}' was passed".format(
            #                 from_analysis, all_from_analysis))
            #     else:
            #         from_analysis = all_from_analysis
            # Check for summaries and filtered IDs
            if subj_id == self.SUMMARY_NAME:
                subj_id = None
            elif subject_ids is not None and subj_id not in subject_ids:
                continue
            if visit_id == self.SUMMARY_NAME:
                visit_id = None
            elif visit_ids is not None and visit_id not in visit_ids:
                continue
            # Map IDs into ID space of analysis
            subj_id = dataset.map_subject_id(subj_id)
            visit_id = dataset.map_visit_id(visit_id)
            # Determine frequency of session|summary
            if (subj_id, visit_id) == (None, None):
                frequency = 'per_dataset'
            elif subj_id is None:
                frequency = 'per_visit'
            elif visit_id is None:
                frequency = 'per_subject'
            else:
                frequency = 'per_session'
            filtered_files = self._filter_files(files, session_path)
            for fname in filtered_files:
                basename = split_extension(fname)[0]
                all_file_groups.append(
                    FileGroup.from_path(
                        op.join(session_path, fname),
                        frequency=frequency,
                        subject_id=subj_id, visit_id=visit_id,
                        dataset=dataset,
                        from_analysis=from_analysis,
                        potential_aux_files=[
                            f for f in filtered_files
                            if (split_extension(f)[0] == basename
                                and f != fname)],
                        **kwargs))
            for fname in self._filter_dirs(dirs, session_path):
                all_file_groups.append(
                    FileGroup.from_path(
                        op.join(session_path, fname),
                        frequency=frequency,
                        subject_id=subj_id, visit_id=visit_id,
                        dataset=dataset,
                        from_analysis=from_analysis,
                        **kwargs))
            if self.FIELDS_FNAME in files:
                with open(op.join(session_path,
                                  self.FIELDS_FNAME), 'r') as f:
                    dct = json.load(f)
                all_fields.extend(
                    Field(name=k, value=v, frequency=frequency,
                          subject_id=subj_id, visit_id=visit_id,
                          dataset=dataset, from_analysis=from_analysis,
                          **kwargs)
                    for k, v in list(dct.items()))
            if self.PROV_DIR in dirs:
                if from_analysis is None:
                    raise ArcanaRepositoryError(
                        "Found provenance directory in session directory (i.e."
                        " not in analysis-specific sub-directory)")
                base_prov_dir = op.join(session_path, self.PROV_DIR)
                for fname in os.listdir(base_prov_dir):
                    all_records.append(Record.load(
                        split_extension(fname)[0],
                        frequency, subj_id, visit_id, from_analysis,
                        op.join(base_prov_dir, fname)))
        return all_file_groups, all_fields, all_records

    def _extract_ids_from_path(self, depth, path_parts, dirs, files):
        path_depth = len(path_parts)
        if path_depth == depth:
            # Load input data
            from_analysis = None
        elif (path_depth == (depth + 1)
              and self.PROV_DIR in dirs):
            # Load analysis output
            from_analysis = path_parts.pop()
        elif (path_depth < depth
              and any(not f.startswith('.') for f in files)):
            # Check to see if there are files in upper level
            # directories, which shouldn't be there (ignoring
            # "hidden" files that start with '.')
            raise ArcanaRepositoryError(
                "Files ('{}') not permitted at {} level in local "
                "repository".format("', '".join(files),
                                    ('subject'
                                     if path_depth else 'dataset')))
        else:
            # Not a directory that contains data files or directories
            return None
        if len(path_parts) == 2:
            subj_id, visit_id = path_parts
        elif len(path_parts) == 1:
            subj_id = path_parts[0]
            visit_id = self.DEFAULT_VISIT_ID
        else:
            subj_id = self.DEFAULT_SUBJECT_ID
            visit_id = self.DEFAULT_VISIT_ID
        return subj_id, visit_id, from_analysis

    def file_group_path(self, item, dataset=None, fname=None):
        if fname is None:
            fname = item.fname
        if dataset is None:
            dataset = item.dataset
        root_dir = dataset.name
        depth = dataset.depth
        subject_id = dataset.inv_map_subject_id(item.subject_id)
        visit_id = dataset.inv_map_visit_id(item.visit_id)
        if item.frequency == 'per_dataset':
            subj_dir = self.SUMMARY_NAME
            visit_dir = self.SUMMARY_NAME
        elif item.frequency.startswith('per_subject'):
            if depth < 2:
                raise ArcanaInsufficientRepoDepthError(
                    "Basic repo needs to have depth of 2 (i.e. sub-directories"
                    " for subjects and visits) to hold 'per_subject' data")
            subj_dir = str(subject_id)
            visit_dir = self.SUMMARY_NAME
        elif item.frequency.startswith('per_visit'):
            if depth < 1:
                raise ArcanaInsufficientRepoDepthError(
                    "Basic repo needs to have depth of at least 1 (i.e. "
                    "sub-directories for subjects) to hold 'per_visit' data")
            subj_dir = self.SUMMARY_NAME
            visit_dir = str(visit_id)
        elif item.frequency.startswith('per_session'):
            subj_dir = str(subject_id)
            visit_dir = str(visit_id)
        else:
            assert False, "Unrecognised frequency '{}'".format(
                item.frequency)
        if depth == 2:
            acq_dir = op.join(root_dir, subj_dir, visit_dir)
        elif depth == 1:
            acq_dir = op.join(root_dir, subj_dir)
        elif depth == 0:
            acq_dir = root_dir
        else:
            assert False
        if item.from_analysis is None:
            sess_dir = acq_dir
        else:
            # Append analysis-name to path (i.e. make a sub-directory to
            # hold derived products)
            sess_dir = op.join(acq_dir, item.from_analysis)
        # Make session dir if required
        if item.derived and not op.exists(sess_dir):
            os.makedirs(sess_dir, stat.S_IRWXU | stat.S_IRWXG)
        return op.join(sess_dir, fname)

    def fields_json_path(self, field, dataset=None):
        return self.file_group_path(field, fname=self.FIELDS_FNAME,
                                 dataset=dataset)

    def prov_json_path(self, record, dataset):
        return self.file_group_path(record,
                                 dataset=dataset,
                                 fname=op.join(self.PROV_DIR,
                                               record.pipeline_name + '.json'))

    @classmethod
    def guess_depth(cls, root_dir):
        """
        Try to guess the depth of a directory repository (i.e. whether it has
        sub-folders for multiple subjects or visits, depending on where files
        and/or derived label files are found in the hierarchy of
        sub-directories under the root dir.

        Parameters
        ----------
        root_dir : str
            Path to the root directory of the repository
        """
        deepest = -1
        for path, dirs, files in os.walk(root_dir):
            depth = cls.path_depth(root_dir, path)
            filtered_files = cls._filter_files(files, path)
            if filtered_files:
                logger.info("Guessing depth of directory repository at '{}' is"
                            " {} due to unfiltered files ('{}') in '{}'"
                            .format(root_dir, depth,
                                    "', '".join(filtered_files), path))
                return depth
            if cls.PROV_DIR in dirs:
                depth_to_return = max(depth - 1, 0)
                logger.info("Guessing depth of directory repository at '{}' is"
                            "{} due to \"Derived label file\" in '{}'"
                            .format(root_dir, depth_to_return, path))
                return depth_to_return
            if depth >= cls.MAX_DEPTH:
                logger.info("Guessing depth of directory repository at '{}' is"
                            " {} as '{}' is already at maximum depth"
                            .format(root_dir, cls.MAX_DEPTH, path))
                return cls.MAX_DEPTH
            try:
                for fpath in chain(filtered_files,
                                   cls._filter_dirs(dirs, path)):
                    FileGroup.from_path(fpath)
            except ArcanaError:
                pass
            else:
                if depth > deepest:
                    deepest = depth
        if deepest == -1:
            raise ArcanaRepositoryError(
                "Could not guess depth of '{}' repository as did not find "
                "a valid session directory within sub-directories."
                .format(root_dir))
        return deepest

    @classmethod
    def _filter_files(cls, files, base_dir):
        # Matcher out hidden files (i.e. starting with '.')
        return [op.join(base_dir, f) for f in files
                if not (f.startswith('.')
                        or f.startswith(cls.FIELDS_FNAME))]

    @classmethod
    def _filter_dirs(cls, dirs, base_dir):
        # Matcher out hidden directories (i.e. starting with '.')
        # and derived analysis directories from file_group names
        filtered = [
            op.join(base_dir, d) for d in dirs
            if not (d.startswith('.') or d == cls.PROV_DIR or (
                cls.PROV_DIR in os.listdir(op.join(base_dir, d))))]
        return filtered

    @classmethod
    def path_depth(cls, root_dir, dpath):
        relpath = op.relpath(dpath, root_dir)
        if '..' in relpath:
            raise ArcanaUsageError(
                "Path '{}' is not a sub-directory of '{}'".format(
                    dpath, root_dir))
        elif relpath == '.':
            depth = 0
        else:
            depth = relpath.count(op.sep) + 1
        return depth
