from genericpath import exists
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
from arcana.core.data.provenance import DataProvenance
from arcana.exceptions import ArcanaFileFormatError, ArcanaMissingDataException, ArcanaUsageError
from arcana.core.utils import get_class_info, HOSTNAME, split_extension
from arcana.core.data.set import Dataset
from arcana.data.spaces.medicalimaging import Clinical, DataSpace
from arcana.core.data.store import DataStore
from arcana.core.data.format import FileGroup, Field


logger = logging.getLogger('arcana')


@attr.s
class FileSystem(DataStore):
    """
    A Repository class for data stored hierarchically within sub-directories
    of a file-system directory. The depth and which layer in the data tree
    the sub-directories correspond to is defined by the `hierarchy` argument.

    Parameters
    ----------
    base_dir : str
        Path to the base directory of the "store", i.e. datasets are
        arranged by name as sub-directories of the base dir.

    """

    alias = 'file'
    PROV_SUFFIX = '.prov'
    FIELDS_FNAME = '__fields__.json'
    LOCK_SUFFIX = '.lock'
    PROV_KEY = '__provenance__'
    VALUE_KEY = '__value__'
    METADATA_DIR = '.arcana'
    
    def new_dataset(self, id, *args, **kwargs):
        if not Path(id).exists():
            raise ArcanaUsageError(
                f"Path to dataset root '{id}'' does not exist")
        return super().new_dataset(id, *args, **kwargs)

    def save_dataset_definition(self, dataset_id, definition, name):
        definition_path = self.definition_save_path(dataset_id, name)
        definition_path.parent.mkdir(exist_ok=True)
        with open(definition_path, 'w') as f:
            json.dump(definition, f)

    def load_dataset_definition(self, dataset_id, name):
        fpath = self.definition_save_path(dataset_id, name)
        if fpath.exists():
            with open(fpath) as f:
                definition = json.load(f)
        else:
            definition = None
        return definition

    def definition_save_path(self, dataset_id, name):
        return Path(dataset_id) / self.METADATA_DIR / (name + '.json')

    @property
    def provenance(self):
        return {
            'type': get_class_info(type(self)),
            'host': HOSTNAME}

    def get_file_group_paths(self, file_group: FileGroup):
        """
        Set the path of the file_group from the store
        """
        # Don't need to cache file_group as it is already local as long
        # as the path is set
        stem_path = self.file_group_stem_path(file_group)
        matches = [p for p in stem_path.parent.iterdir()
                   if str(p).startswith(str(stem_path))]
        if not matches:
            raise ArcanaMissingDataException(
                f"No files/sub-dirs matching '{file_group.path}' path found in "
                f"{str(self.absolute_node_path(file_group))} directory")
        return matches

    def get_field_value(self, field):
        """
        Update the value of the field from the store
        """
        self.cast_value(self.get_field_val(field))

    def cast_value(self, val, field):
        if isinstance(val, dict):
            val = val[self.VALUE_KEY]
        if field.array:
            val = [field.format(v) for v in val]
        else:
            val = field.format(val)
        return val

    def put_file_group_paths(self, file_group: FileGroup, fs_paths: list[Path]):
        """
        Inserts or updates a file_group in the store
        """
        stem_path = self.file_group_stem_path(file_group)
        # Create target directory if it doesn't exist already
        stem_path.parent.mkdir(exist_ok=True, parents=True)
        cached_paths = []
        for fs_path in fs_paths:
            if fs_path.is_dir():
                target_path = stem_path
                if target_path.exists():
                    shutil.rmtree(target_path)
                shutil.copytree(fs_path, target_path)
            else:
                target_path = file_group.copy_ext(fs_path, stem_path)
                shutil.copyfile(str(fs_path), str(target_path))
            cached_paths.append(target_path)
        return cached_paths

    def file_group_stem_path(self, file_group):
        """The path to the stem of the paths (i.e. the path without
        file extension) where the files are saved in the file-system.
        NB: this method is overridden in BidsFormat store.

        Parameters
        ----------
        file_group: FileGroup
            the file group stored or to be stored
        """
        node_path = self.absolute_node_path(file_group.data_node)
        return node_path.joinpath(*file_group.path.split('/'))

    def put_field_value(self, field, value):
        """
        Inserts or updates a field in the store
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

    def put_provenance(self, item, provenance):
        with open(self.prov_json_path(item), 'w') as f:
            json.dump(provenance, f)

    def get_provenance(self, item):
        with open(self.prov_json_path(item)) as f:
            provenance = json.load(f)
        return provenance

    def find_nodes(self, dataset: Dataset):
        """
        Find all nodes within the dataset stored in the store and
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

    def root_dir(self, data_node) -> Path:
        return Path(data_node.dataset.id)

    @classmethod
    def absolute_node_path(cls, data_node) -> Path:
        return cls().root_dir(data_node) / cls().node_path(data_node)

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
                "{} does not exist in the local store {}"
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
        stores
    """

    return FileSystem(op.join(path, '..'), **kwargs).dataset(
        op.basename(path), tree_dimensions)
