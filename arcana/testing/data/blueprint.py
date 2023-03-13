from __future__ import annotations
import typing as ty
import itertools
from abc import ABCMeta, abstractmethod
from pathlib import Path
import tempfile
import shutil
import zipfile
import attrs
from fileformats.core import FileSet, Field
from arcana.core.data.row import DataRow
from arcana.core.utils.misc import path2varname, set_cwd
from arcana.core.exceptions import ArcanaError
from arcana.core.data.store import DataStore


@attrs.define(kw_only=True)
class EntryBlueprint(metaclass=ABCMeta):

    path: str
    datatype: type = attrs.field()
    row_frequency: str = None
    ids: list[str] = None  # the list of row IDs to create the blueprint in
    alternative_datatypes: list[type] = attrs.field(factory=list)

    @datatype.validator
    def datatype_validator(self, _, datatype):
        if datatype is None:
            raise ValueError("datatype cannot be None")

    @abstractmethod
    def make_item(self, **kwargs):
        pass

    def make_entry(self, row: DataRow, **kwargs):
        if self.ids and row.id not in self.ids:
            return
        item = self.make_item(**kwargs)
        entry = row.dataset.store.create_entry(
            path=self.path,
            datatype=self.datatype,
            row=row,
        )
        row.dataset.store.put(item, entry)


@attrs.define(kw_only=True)
class FileSetEntryBlueprint(EntryBlueprint):

    filenames: list[str]

    def make_item(
        self,
        source_data: Path = None,
        source_fallback: bool = False,
        escape_source_name: bool = True,
    ) -> FileSet:
        """For use in test routines, this classmethod creates a simple text file,
        zip file or nested directory at the given path

        Parameters
        ----------
        source_data : Path, optional
            path to a directory containing source data to use instead of the dummy
            data
        source_fallback : bool
            whether to fall back to the generated file if fname isn't in the source
            data dir
        escape_source_name : bool
            whether to escape the source name or simple use the file name of the source

        Returns
        -------
        FileSet
            the created fileset
        """
        tmp_dir = Path(tempfile.mkdtemp())
        out_paths = []
        for fname in self.filenames:
            out_path = None
            if source_data is not None:
                src_path = source_data.joinpath(*fname.split("/"))
                if src_path.exists():
                    if escape_source_name:
                        parts = fname.split(".")
                        out_fname = path2varname(parts[0]) + "." + ".".join(parts[1:])
                    else:
                        out_fname = Path(fname).name
                    out_path = tmp_dir / out_fname
                    if src_path.is_dir():
                        shutil.copytree(src_path, out_path)
                    else:
                        shutil.copyfile(src_path, out_path, follow_symlinks=True)
                elif not source_fallback:
                    raise ArcanaError(
                        f"Couldn't find {fname} in source data directory {source_data}"
                    )
            if out_path is None:
                out_path = tmp_dir / fname
                next_part = fname
                if next_part.endswith(".zip"):
                    next_part = next_part.strip(".zip")
                next_path = Path(next_part)
                # Make double dir
                if next_part.startswith("doubledir"):
                    (tmp_dir / next_path).mkdir(exist_ok=True)
                    next_part = "dir"
                    next_path /= next_part
                if next_part.startswith("dir"):
                    (tmp_dir / next_path).mkdir(exist_ok=True)
                    next_part = "test.txt"
                    next_path /= next_part
                if not next_path.suffix:
                    next_path = next_path.with_suffix(".txt")
                if next_path.suffix == ".json":
                    contents = '{"a": 1.0}'
                else:
                    contents = fname
                with open(tmp_dir / next_path, "w") as f:
                    f.write(contents)
                if fname.endswith(".zip"):
                    with zipfile.ZipFile(out_path, mode="w") as zfile, set_cwd(tmp_dir):
                        zfile.write(next_path)
                    (tmp_dir / next_path).unlink()
            out_paths.append(out_path)
        return self.datatype(out_paths)


@attrs.define(kw_only=True)
class FieldEntryBlueprint(EntryBlueprint):

    value: ty.Any
    expected_value: ty.Any = None

    def make_item(self, **kwargs) -> Field:
        return self.datatype(self.value)

    def __attrs_post_init__(self):
        if self.expected_value is None:
            self.expected_value = self.value


@attrs.define(slots=False, kw_only=True)
class TestDatasetBlueprint:

    space: type
    hierarchy: list[str]
    dim_lengths: list[int]  # size of layers a-d respectively
    entries: list[EntryBlueprint] = attrs.field(factory=list)
    derivatives: list[EntryBlueprint] = attrs.field(factory=list)
    id_composition: dict[str, str] = attrs.field(factory=dict)

    def make_dataset(
        self,
        store: DataStore,
        dataset_id: str,
        name: str = None,
        source_data: Path = None,
        **kwargs,
    ):
        """For use in tests, this method creates a test dataset from the provided
        blueprint

        Parameters
        ----------
        store: DataStore
            the store to make the dataset within
        dataset_id : str
            the ID of the project/directory within the store to create the dataset
        name : str, optional
            the name to give the dataset. If provided the dataset is also saved in the
            datastore
        source_data : Path, optional
            path to a directory containing source data to use instead of the dummy
            data
        **kwargs
            passed through to new_dataset
        """
        with store.connection:
            dataset = store.new_dataset(
                id=dataset_id,
                leaves=self.all_ids,
                name=name,
                hierarchy=self.hierarchy,
                id_composition=self.id_composition,
                space=self.space,
                **kwargs,
            )
            for row in dataset.rows(frequency=max(self.space)):
                self.make_entries(row, source_data=source_data)
        dataset.__annotations__["blueprint"] = self
        return dataset

    def access_dataset(self, store: DataStore, dataset_id: str):
        dataset = store.define_dataset(
            dataset_id,
            hierarchy=self.hierarchy,
            id_composition=self.id_composition,
        )
        dataset.__annotations__["blueprint"] = self
        return dataset

    def make_entries(
        self,
        row: DataRow,
        **kwargs,
    ):
        """Creates the actual data in the store, from the provided blueprint, which
        can be used to run test routines against

        Parameters
        ----------
        store
            the store to create the test dataset in
        dataset_id : str
            the ID of the dataset to create
        name : str
            the name of the dataset
        **kwargs
            passed directly through to the EntryBlueprint.create_item method
        """
        for entry_bp in self.entries:
            entry_bp.make_entry(row, **kwargs)

    @property
    def all_ids(self):
        """Iterate all leaves of the data tree specified by the test blueprint and yield
        ID tuples corresponding to the IDs of each leaf node"""
        for id_tple in itertools.product(*(list(range(d)) for d in self.dim_lengths)):
            base_ids = dict(zip(self.space.axes(), id_tple))
            ids = {}
            for layer in self.hierarchy:
                ids[layer] = "".join(
                    f"{b}{base_ids[b]}" for b in self.space[layer].span()
                )
            yield tuple(ids[h] for h in self.hierarchy)
