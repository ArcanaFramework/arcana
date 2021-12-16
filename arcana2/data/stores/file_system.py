import os
import os.path as op
from pathlib import Path
import re
from copy import copy
import errno
from collections import defaultdict
import shutil
import logging
import json
import attr
from fasteners import InterProcessLock
from arcana2.core.data.provenance import DataProvenance
from arcana2.exceptions import ArcanaFileFormatError, ArcanaMissingDataException, ArcanaUsageError
from arcana2.core.utils import get_class_info, HOSTNAME, split_extension
from arcana2.core.data.set import Dataset
from arcana2.data.spaces.clinical import Clinical, DataSpace
from arcana2.core.data.store import DataRepository


logger = logging.getLogger('arcana')


@attr.s
class FileSystem(DataRepository):
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
    PROV_SUFFIX = '.prov'
    FIELDS_FNAME = '__fields__.json'
    LOCK_SUFFIX = '.lock'
    PROV_KEY = '__provenance__'
    VALUE_KEY = '__value__'
    
    def dataset(self, name, *args, **kwargs):
        name = Path(name)
        if not name.exists():
            raise ArcanaUsageError(
                f"Path to dataset root '{str(name)}'' does not exist")
        return super().dataset(name, *args, **kwargs)        

    @property
    def provenance(self):
        return {
            'type': get_class_info(type(self)),
            'host': HOSTNAME}

    def get_file_group(self, file_group, **kwargs):
        """
        Set the path of the file_group from the repository
        """
        # Don't need to cache file_group as it is already local as long
        # as the path is set
        primary_path = self.file_group_path(file_group)
        side_cars = file_group.datatype.default_side_cars(primary_path)
        location_str = (f"{file_group.data_node} node of "
                        f"{file_group.data_node.dataset} on {self}")
        if not op.exists(primary_path):
            raise ArcanaMissingDataException(
                f"{file_group} ({primary_path}) does not exist in {location_str}")
        for aux_name, aux_path in side_cars.items():
            if not op.exists(aux_path):
                raise ArcanaMissingDataException(
                    f"{file_group} is missing '{aux_name}' side car "
                    f"({aux_path}) in {location_str}")
        return primary_path, side_cars

    def get_field(self, field):
        """
        Update the value of the field from the repository
        """
        self.cast_value(self.get_field_val(field))

    def cast_value(self, val, field):
        if isinstance(val, dict):
            val = val[self.VALUE_KEY]
        if field.array:
            val = [field.datatype(v) for v in val]
        else:
            val = field.datatype(val)
        return val

    def put_file_group(self, file_group, fs_path, side_cars):
        """
        Inserts or updates a file_group in the repository
        """
        fs_path = Path(fs_path)
        target_path = self.file_group_path(file_group)
        if fs_path == target_path:
            logger.info(
                f"Attempted to set file path of {file_group} to its path in "
                f"the repository {target_path}")
            return
        # Create target directory if it doesn't exist already
        dname = target_path.parent
        if not dname.exists():
            os.makedirs(dname)
        if fs_path.is_file():
            shutil.copyfile(fs_path, target_path)
            sc_target_paths = file_group.datatype.default_side_cars(target_path)
            # Copy side car files into repository
            if side_cars is not None:
                side_cars = copy(side_cars)
            for sc_name in file_group.datatype.side_cars:
                try:
                    sc_path = side_cars.pop(sc_name)
                except KeyError:
                    raise ArcanaFileFormatError(
                        f"Missing side car '{sc_name}' when attempting to "
                        f"put file_group")
                shutil.copyfile(str(sc_path), str(sc_target_paths[sc_name]))
            if side_cars:
                raise ArcanaFileFormatError(
                    f"Unrecognised side cars ({side_cars}) when attempting to "
                    f"write {file_group.datatype.name} files")
        elif fs_path.is_dir():
            if target_path.exists():
                shutil.rmtree(target_path)
            shutil.copytree(fs_path, target_path)
        else:
            raise ValueError(
                f"Source path '{fs_path}' to be set for {file_group} does not exist")
        if file_group.provenance is not None:
            file_group.provenance.save(self.prov_json_path(file_group))

    def put_field(self, field, value):
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
                value = list(value)
            dct[field.path] = {
                self.VALUE_KEY: value,
                self.PROV_KEY: field.provenance.dct}
            with open(fpath, 'w') as f:
                json.dump(dct, f, indent=2)

    def find_nodes(self, dataset: Dataset):
        """
        Find all nodes within the dataset stored in the repository and
        construct the data tree within the dataset

        Parameters
        ----------
        dataset : Dataset
            The dataset to construct the tree dimensions for
        """
        if not os.path.exists(dataset.id):
            raise ArcanaUsageError(
                f"Could not find a directory at '{dataset.id}' to be the "
                "root node of the dataset")

        for dpath, _, _ in os.walk(dataset.id):
            tree_path = Path(dpath).relative_to(dataset.id).parts
            if (len(tree_path) == len(dataset.hierarchy)
                    and not re.match(r'__.*__$', tree_path[-1])):
                dataset.add_leaf_node(tree_path)

    def find_items(self, data_node):
        # First ID can be omitted
        self.find_items_in_dir(
            self.root_dir(data_node) / self.node_path(data_node),
            data_node)

    def find_items_in_dir(self, dpath, data_node):
        if not op.exists(dpath):
            return
        # Filter contents of directory to omit fields JSON and provenance
        filtered = []
        for subpath in dpath.iterdir():
            if not (subpath.name.startswith('.')
                    or subpath.name == self.FIELDS_FNAME
                    or subpath.name.endswith(self.PROV_SUFFIX)):
                filtered.append(subpath.name)
        # Group files and sub-dirs that match except for extensions
        matching = defaultdict(set)
        for fname in filtered:
            basename = fname.split('.')[0]
            matching[basename].add(fname)
        # Add file groups
        for bname, fnames in matching.items():
            data_node.add_file_group(
                path=bname,
                file_paths=[op.join(dpath, f) for f in fnames],
                provenance=DataProvenance.load(
                    op.join(dpath, bname + self.PROV_SUFFIX),
                    ignore_missing=True))
        # Add fields
        try:
            with open(op.join(dpath, self.FIELDS_FNAME), 'r') as f:
                dct = json.load(f)
        except FileNotFoundError:
            pass
        else:
            for name, value in dct.items():
                if isinstance(value, dict):
                    prov = value[self.PROV_KEY]
                    value = value[self.VALUE_KEY]
                else:
                    prov = None
                data_node.add_field(name_path=name, value=value,
                                    provenance=prov)

    def node_path(self, node):
        path = Path()
        accounted_freq = node.dataset.space(0)
        for layer in node.dataset.hierarchy:
            if not (layer.is_parent(node.frequency)
                    or layer == node.frequency):
                break
            path /= node.ids[layer]
            accounted_freq |= layer
        # If not "leaf node" then 
        if node.frequency != max(node.dataset.space):
            unaccounted_freq = node.frequency - (node.frequency
                                                 & accounted_freq)
            unaccounted_id = node.ids[unaccounted_freq]
            if unaccounted_id is None:
                path /= f'__{unaccounted_freq}__'
            elif isinstance(unaccounted_id, str):
                path /= f'__{unaccounted_freq}_{unaccounted_id}__'
            else:
                path /= (f'__{unaccounted_freq}_'
                         + '_'.join(unaccounted_id) + '__')
        return path

    def root_dir(self, data_node):
        return Path(data_node.dataset.id)

    @classmethod
    def absolute_node_path(cls, data_node):
        repo = cls()
        return repo.root_dir(data_node) / repo.node_path(data_node)

    def file_group_path(self, file_group):
        fs_path = self.root_dir(file_group.data_node) / self.node_path(
            file_group.data_node).joinpath(*file_group.path.split('/'))
        if file_group.datatype.extension:
            fs_path = fs_path.with_suffix(file_group.datatype.extension)
        return fs_path

    def fields_json_path(self, field):
        return (self.root_dir(field.data_node)
                / self.node_path(field.data_node)
                / self.FIELDS_FNAME)

    def prov_json_path(self, file_group):
        return self.file_group_path(file_group) + self.PROV_SUFFX

    def get_provenance(self, item):
        if item.is_file_group:
            prov = self._get_file_group_provenance(item)
        else:
            prov = self._get_field_provenance(item)
        return prov

    def _get_file_group_provenance(self, file_group):
        if file_group.fs_path is not None:
            prov = DataProvenance.load(self.prov_json_path(file_group))
        else:
            prov = None
        return prov

    def _get_field_provenance(self, field):
        """
        Loads the fields provenance from the JSON dictionary
        """
        val_dct = self.get_field_val(field)
        if isinstance(val_dct, dict):
            prov = val_dct.get(self.PROV_KEY)
        else:
            prov = None
        return prov

    def get_field_val(self, field):
        """
        Load fields JSON, locking to prevent read/write conflicts
        Would be better if only checked if locked to allow
        concurrent reads but not possible with multi-process
        locks (in my understanding at least).
        """
        json_path = self.fields_json_path(field)
        try:
            with InterProcessLock(json_path + self.LOCK_SUFFIX,
                                  logger=logger), open(json_path, 'r') as f:
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
                                 


def single_dataset(path: str, tree_dimensions: DataSpace=Clinical,
                   **kwargs) -> Dataset:
    """
    Creates a Dataset from a file system path to a directory

    Parameters
    ----------
    path : str
        Path to directory containing the dataset
    tree_dimensions : type
        The enum class that defines the directory tree dimensions of the
        repositories
    """

    return FileSystem(op.join(path, '..'), **kwargs).dataset(
        op.basename(path), tree_dimensions)
