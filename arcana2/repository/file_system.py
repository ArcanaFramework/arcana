import os
import os.path as op
import errno
from collections import defaultdict
import shutil
import logging
import json
import attr
from fasteners import InterProcessLock
from arcana2.core.data.provenance import DataProvenance
from arcana2.exceptions import ArcanaMissingDataException, ArcanaUsageError
from arcana2.core.utils import get_class_info, HOSTNAME, split_extension
from arcana2.core.data.set import Dataset
from arcana2.core.data.enum import Clinical, DataStructure
from arcana2.core.repository import Repository


logger = logging.getLogger('arcana')


@attr.s
class FileSystem(Repository):
    """
    A Repository class for data stored hierarchically within sub-directories
    of a file-system directory. The depth and which layer in the data tree
    the sub-directories correspond to is defined by the `hierarchy` argument.

    Parameters
    ----------
    base_dir : str
        Path to the base directory of the "repository", i.e. datasets are
        arranged by name as sub-directories of the base dir.

    """

    type = 'file_system'
    NODE_DIR = '__node__'
    PROV_SUFFIX = '.__prov__.json'
    FIELDS_FNAME = '__fields__.json'
    LOCK_SUFFIX = '.lock'
    PROV_KEY = 'provenance'
    VALUE_KEY = 'value'

    def get_file_group_paths(self, file_group):
        """
        Set the path of the file_group from the repository
        """
        # Don't need to cache file_group as it is already local as long
        # as the path is set
        primary_path = self.file_group_path(file_group)
        side_cars = file_group.format.default_aux_file_paths(primary_path)
        if not op.exists(primary_path):
            raise ArcanaMissingDataException(
                "{} does not exist in {}"
                .format(file_group, self))
        for aux_name, aux_path in side_cars.items():
            if not op.exists(aux_path):
                raise ArcanaMissingDataException(
                    "{} is missing '{}' side car in {}"
                    .format(file_group, aux_name, self))
        return primary_path, side_cars

    def get_field_value(self, field):
        """
        Update the value of the field from the repository
        """
        val = self._get_field_val(field)
        if isinstance(val, dict):
            val = val[self.VALUE_KEY]
        if field.array:
            val = [field.data_format(v) for v in val]
        else:
            val = field.data_format(val)
        return val

    def put_file_group(self, file_group):
        """
        Inserts or updates a file_group in the repository
        """
        target_path = self.file_group_path(file_group)
        source_path = file_group.file_path
        # Create target directory if it doesn't exist already
        dname = op.dirname(target_path)
        if not op.exists(dname):
            shutil.makedirs(dname)
        if op.isfile(source_path):
            shutil.copyfile(source_path, target_path)
            # Copy side car files into repository
            for aux_name, aux_path in file_group.format.default_aux_file_paths(
                    target_path).items():
                shutil.copyfile(
                    file_group.format.side_cars[aux_name], aux_path)
        elif op.isdir(source_path):
            if op.exists(target_path):
                shutil.rmtree(target_path)
            shutil.copytree(source_path, target_path)
        else:
            assert False
        if file_group.provenance is not None:
            file_group.provenance.save(self.prov_json_path(file_group))

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
                value = list(field.value)
            else:
                value = field.value
            if field.provenance is not None:
                value = {self.VALUE_KEY: value,
                         self.PROV_KEY: field.provenance.dct}
            with open(fpath, 'w') as f:
                json.dump(dct, f, indent=2)

    def construct_tree(self, dataset: Dataset):
        """
        Find all data within a repository, registering file_groups, fields and
        provenance with the found_file_group, found_field and found_provenance
        methods, respectively

        Parameters
        ----------
        dataset : Dataset
            The dataset to construct the tree structure for
        """
        if not os.path.exists(dataset.name):
            raise ArcanaUsageError(
                f"Could not find a directory at '{dataset.name}' to be the "
                "root node of the dataset")        

        def load_prov(dpath, bname):
            prov_path = op.join(dpath, bname + self.PROV_SUFFIX)
            if op.exists(prov_path):
                prov = DataProvenance.load(prov_path)
            else:
                prov = None
            return prov

        def construct_node(dpath, tree_path=None, dname=None):
            if tree_path is None:
                tree_path = []
            "Recursive function to traverse data tree"
            if dname is not None:
                dpath = op.join(dpath, dname)
                tree_path += [dname]
            # First ID can be omitted
            node = dataset.new_node(tree_path)
            # Check if node is a leaf (i.e. lowest level in directory
            # structure)
            is_leaf_node = (node.frequency == dataset.hierarchy[-1])
            filtered, has_fields = self._list_node_dir_contents(
                dpath, is_leaf=is_leaf_node)
            # Group files and sub-dirs that match except for extensions
            matching = defaultdict(set)
            for fname in filtered:
                basename = fname.split('.')[0]
                matching[basename].add(fname)
            # Add file groups
            for bname, fnames in matching.items():
                node.add_file_group(
                    path=bname,
                    file_paths=[op.join(dpath, f) for f in fnames],
                    provenance=load_prov(dpath, bname))
            # Add fields
            if has_fields:
                with open(op.join(dpath, self.FIELDS_FNAME), 'r') as f:
                    dct = json.load(f)
                for name, value in dct.items():
                    if isinstance(value, dict):
                        prov = value[self.PROV_KEY]
                        value = value[self.VALUE_KEY]
                    else:
                        prov = None
                    node.add_field(name_path=name, value=value,
                                   provenance=prov)
            # Add sub-directory nodes
            if not is_leaf_node:
                for sub_dir in os.listdir(dpath):
                    if (not sub_dir.startswith('.')
                            and sub_dir != self.NODE_DIR):
                        construct_node(dpath, tree_path, dname=sub_dir)

        construct_node(dataset.name)

    @property
    def provenance(self):
        return {
            'type': get_class_info(type(self)),
            'host': HOSTNAME}

    @classmethod
    def _list_node_dir_contents(cls, path, is_leaf):
        # Selector out hidden files (i.e. starting with '.')
        if not is_leaf:
            path += cls.NODE_DIR
        filtered = []
        has_fields = False
        if op.exists(path):
            contents = os.listdir(path)
            for item in contents:
                if (item.startswith('.') or item == cls.FIELDS_FNAME
                        or item.endswith(cls.PROV_SUFFIX)):
                    continue
                filtered.append(item)
            has_fields = cls.FIELDS_FNAME in contents
        return filtered, has_fields

    def node_path(self, data_node):
        return op.join(
            data_node.dataset.name,
            *(data_node.ids[f] for f in data_node.frequency.hierarchy()))

    def file_group_path(self, file_group):
        return op.join(self.node_path(file_group.data_node)
                        *(file_group.name_path.split('/')))

    def fields_json_path(self, field):
        return op.join(self.node_path(field.data_node), self.FIELDS_FNAME)

    def prov_json_path(self, file_group):
        return self.file_group_path(file_group) + self.PROV_SUFFX

    def get_provenance(self, item):
        if item.is_file_group:
            prov = self._get_file_group_provenance(item)
        else:
            prov = self._get_field_provenance(item)
        return prov

    def _get_file_group_provenance(self, file_group):
        if file_group.file_path is not None:
            prov = DataProvenance.load(self.prov_json_path(file_group))
        else:
            prov = None
        return prov

    def _get_field_provenance(self, field):
        """
        Loads the fields provenance from the JSON dictionary
        """
        val_dct = self._get_field_val(field)
        if isinstance(val_dct, dict):
            prov = val_dct.get(self.PROV_KEY)
        else:
            prov = None
        return prov

    def _get_field_val(self, field):
        """
        Load fields JSON, locking to prevent read/write conflicts
        Would be better if only checked if locked to allow
        concurrent reads but not possible with multi-process
        locks (in my understanding at least).
        """
        fpath = self.fields_json_path(field)
        try:
            with InterProcessLock(fpath + self.LOCK_SUFFIX,
                                  logger=logger), open(fpath, 'r') as f:
                dct = json.load(f)
            val_dct = dct[field.name]
            return val_dct
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
                                 


def single_dataset(path: str, tree_structure: DataStructure=Clinical,
                   **kwargs) -> Dataset:
    """
    Creates a Dataset from a file system path to a directory

    Parameters
    ----------
    path : str
        Path to directory containing the dataset
    tree_structure : type
        The enum class that defines the directory tree structure of the
        repositories
    """

    return FileSystem(op.join(path, '..'), **kwargs).dataset(
        op.basename(path), tree_structure)
