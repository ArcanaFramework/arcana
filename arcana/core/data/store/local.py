from __future__ import annotations
from pathlib import Path
import re
import typing as ty
import errno
import logging
import json
import attrs
import yaml
from fasteners import InterProcessLock
from fileformats.core.base import DataType, FileSet, Field
from arcana.core.exceptions import (
    ArcanaMissingDataException,
    ArcanaUsageError,
    DatatypeUnsupportedByStoreError,
)
from arcana.core.utils.misc import get_home_dir
from ..space import Samples
from ..row import DataRow
from ..entry import DataEntry
from .base import DataStore

if ty.TYPE_CHECKING:
    from ..testing import TestDatasetBlueprint

logger = logging.getLogger("arcana")


# Matches directory names used for summary rows with dunder beginning and
# end (e.g. '__visit_01__') and hidden directories (i.e. starting with '.' or
# '~')
special_dir_re = re.compile(r"(__.*__$|\..*|~.*)")


@attrs.define
class Local(DataStore):
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

    LOCK_SUFFIX = ".lock"
    METADATA_DIR = ".arcana"
    SITE_LICENSES_DIR = "site-licenses"

    name: str

    def get_field(self, entry: DataEntry, datatype: type) -> Field:
        raise NotImplementedError

    def get_fileset(self, entry: DataEntry, datatype: type) -> FileSet:
        raise NotImplementedError

    def put_fileset(self, fileset: FileSet, entry: DataEntry) -> FileSet:
        """
        Inserts or updates a fileset in the store
        """
        raise NotImplementedError

    def put_field(self, field: Field, entry: DataEntry):
        """
        Inserts or updates a field in the store
        """
        raise NotImplementedError

    def make_fileset_entry(self, path: str, datatype: type, row: DataRow) -> str:
        raise NotImplementedError

    def make_field_entry(self, path: str, datatype: type, row: DataRow) -> str:
        raise NotImplementedError

    def create_test_dataset_data(
        self, blueprint: TestDatasetBlueprint, dataset_id: str, source_data: Path = None
    ):
        """Creates the actual data in the store, from the provided blueprint, which
        can be used to run test routines against

        Parameters
        ----------
        blueprint
            the test dataset blueprint
        dataset_path : Path
            the pat
        """
        raise NotImplementedError

    def save_dataset_definition(self, dataset_id, definition, name):
        definition_path = self.definition_save_path(dataset_id, name)
        definition_path.parent.mkdir(exist_ok=True)
        with open(definition_path, "w") as f:
            yaml.dump(definition, f)

    def load_dataset_definition(self, dataset_id, name):
        fpath = self.definition_save_path(dataset_id, name)
        if fpath.exists():
            with open(fpath) as f:
                definition = yaml.load(f, Loader=yaml.Loader)
        else:
            definition = None
        return definition

    def get(self, entry: DataEntry, datatype: type) -> DataType:
        if entry.datatype.is_fileset:
            item = self.get_fileset(entry, datatype)
        elif entry.datatype.is_field:
            item = self.get_field(entry, datatype)
        else:
            raise RuntimeError(
                f"Don't know how to retrieve {entry.datatype} data from {type(self)} stores"
            )
        return item

    def put(self, item: DataType, entry: DataEntry):
        if entry.datatype.is_fileset:
            cpy = self.put_fileset(item, entry)
        elif entry.datatype.is_field:
            cpy = self.put_field(item, entry)
        else:
            raise RuntimeError(
                f"Don't know how to store {entry.datatype} data in {type(self)} stores"
            )
        return cpy

    def post(
        self, item: DataType, path: str, datatype: type, row: DataRow
    ) -> DataEntry:
        if datatype.is_fileset:
            entry = self.post_fileset(item, path, datatype, row)
        elif datatype.is_field:
            entry = self.put_field(item, path, datatype, row)
        else:
            raise RuntimeError(
                f"Don't know how to store {datatype} data in {type(self)} stores"
            )
        return entry

    def get_provenance(self, entry: DataEntry) -> dict[str, ty.Any]:
        if entry.datatype.is_fileset:
            with open(self.get_fileset_prov_path(entry)) as f:
                provenance = json.load(f)
        elif entry.datatype.is_field:
            with open(self.get_fields_prov_path(entry)) as f:
                fields_provenance = json.load(f)
            provenance = fields_provenance[entry.path]
        else:
            raise DatatypeUnsupportedByStoreError(entry.datatype, self)
        return provenance

    def put_provenance(self, provenance: dict[str, ty.Any], entry: DataEntry):
        if entry.datatype.is_fileset:
            with open(self.get_fileset_prov_path(entry), "w") as f:
                json.dump(provenance, f)
        elif entry.datatype.is_field:
            self.update_json(self.get_fields_prov_path(entry), entry.path, provenance)
        else:
            raise DatatypeUnsupportedByStoreError(entry.datatype, self)

    def post_fileset(
        self, fileset: FileSet, path: str, datatype: type, row: DataRow
    ) -> DataEntry:
        uri = self.make_fileset_entry(path, datatype, row)
        entry = row.add_entry(path=path, datatype=datatype, uri=uri)
        # Need to wait until the entry is created so we can use the get_fileset_path
        # to determine the URI
        self.put(fileset, entry)
        return entry

    def post_field(
        self, field: Field, path: str, datatype: type, row: DataRow
    ) -> DataEntry:
        uri = self.make_field_entry(path, datatype, row)
        entry = row.add_entry(path=id, datatype=datatype, uri=uri)
        self.put(field, entry)
        return entry

    def root_dir(self, row) -> Path:
        return Path(row.dataset.id)

    def site_licenses_dataset(self):
        """Provide a place to store hold site-wide licenses"""
        dataset_root = get_home_dir() / self.SITE_LICENSES_DIR
        if not dataset_root.exists():
            dataset_root.mkdir(parents=True)
        try:
            dataset = self.load_dataset(dataset_root)
        except KeyError:
            dataset = self.new_dataset(dataset_root, space=Samples)
        return dataset

    def update_json(self, fpath: Path, key, value):
        """Updates a JSON file in a multi-process safe way"""
        # Open fields JSON, locking to prevent other processes
        # reading or writing
        with InterProcessLock(fpath + self.LOCK_SUFFIX, logger=logger):
            try:
                with open(fpath) as f:
                    dct = json.load(f)
            except IOError as e:
                if e.errno == errno.ENOENT:
                    dct = {}
                else:
                    raise
            dct[key] = value
            with open(fpath, "w") as f:
                json.dump(dct, f, indent=4)

    def read_from_json(self, fpath, key):
        """
        Load fields JSON, locking to prevent read/write conflicts
        Would be better if only checked if locked to allow
        concurrent reads but not possible with multi-process
        locks (in my understanding at least).
        """
        try:
            with InterProcessLock(fpath + self.LOCK_SUFFIX, logger=logger), open(
                fpath, "r"
            ) as f:
                dct = json.load(f)
            return dct[key]
        except (KeyError, IOError) as e:
            try:
                # Check to see if the IOError wasn't just because of a
                # missing file
                if e.errno != errno.ENOENT:
                    raise
            except AttributeError:
                pass
            raise ArcanaMissingDataException(
                "{} does not exist in the local store {}".format(key, self)
            )

    def definition_save_path(self, dataset_id, name):
        return Path(dataset_id) / self.METADATA_DIR / (name + ".yaml")

    def new_dataset(self, id, *args, **kwargs):
        if not Path(id).exists():
            raise ArcanaUsageError(f"Path to dataset root '{id}'' does not exist")
        return super().new_dataset(id, *args, **kwargs)
