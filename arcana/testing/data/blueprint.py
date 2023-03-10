from __future__ import annotations
import typing as ty
import itertools
from pathlib import Path
import tempfile
import shutil
import zipfile
import attrs
from fileformats.core import FileSet
from arcana.core.data.space import DataSpace
from arcana.core.data.row import DataRow
from arcana.core.utils.misc import path2varname, set_cwd
from arcana.core.exceptions import ArcanaError
from arcana.core.data.store import DataStore


@attrs.define(kw_only=True)
class ExpDatatypeBlueprint:

    datatype: type
    filenames: list[str]


@attrs.define(kw_only=True)
class DerivBlueprint:

    name: str
    row_frequency: DataSpace
    datatype: type
    filenames: ty.List[str]


@attrs.define(kw_only=True)
class FileSetBlueprint:

    path: str
    datatype: type
    row_frequency: DataSpace
    filenames: ty.List[str]


@attrs.define(kw_only=True)
class FieldBlueprint:

    path: str
    row_frequency: DataSpace
    datatype: type
    value: ty.Any


@attrs.define(slots=False, kw_only=True)
class TestDatasetBlueprint:

    hierarchy: ty.List[DataSpace]
    dim_lengths: ty.List[int]  # size of layers a-d respectively
    filesets: ty.List[FileSetBlueprint]  # files present at bottom layer
    id_inference: ty.List[ty.Tuple[DataSpace, str]] = attrs.field(
        factory=list
    )  # id_inference dict
    expected_datatypes: ty.Dict[str, ExpDatatypeBlueprint] = attrs.field(
        factory=dict
    )  # expected formats
    derivatives: ty.List[DerivBlueprint] = attrs.field(
        factory=list
    )  # files to insert as derivatives
    fields: ty.List[FieldBlueprint] = attrs.field(factory=list)

    @property
    def space(self):
        return type(self.hierarchy[0])

    @property
    def all_ids(self):
        """Iterate all leaves of the data tree specified by the test blueprint and yield
        ID tuples corresponding to the IDs of each leaf node"""
        for id_tple in itertools.product(*(list(range(d)) for d in self.dim_lengths)):
            base_ids = dict(zip(self.space.axes(), id_tple))
            ids = {}
            for layer in self.hierarchy:
                ids[layer] = "".join(f"{b}{base_ids[b]}" for b in layer.span())
            yield tuple(ids[h] for h in self.hierarchy)

    def create_dataset(
        self,
        store: DataStore,
        dataset_id: str,
        source_data: Path = None,
        name: str = None,
        **kwargs,
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
        with store.connection:
            dataset = store.new_dataset(
                id=dataset_id,
                leaves=self.all_ids,
                name=name,
                hierarchy=self.hierarchy,
                space=self.space,
            )
            for row in dataset.rows(frequency=max(self.space)):
                self.create_entries(row, store, source_data=source_data, **kwargs)
        return dataset

    def create_entries(
        self, row: DataRow, store: DataStore, source_data: Path, **kwargs
    ):
        for blueprint in self.filesets:
            fileset = self.create_fileset(blueprint, source_data=source_data, **kwargs)
            entry = store.create_entry(
                path=fileset.stem,
                datatype=type(fileset),
                row=row,
            )
            store.put(fileset, entry)

    def create_fileset(
        self,
        blueprint: FileSetBlueprint,
        source_data: Path = None,
        source_fallback: bool = False,
        escape_source_name: bool = True,
    ) -> FileSet:
        """For use in test routines, this classmethod creates a simple text file,
        zip file or nested directory at the given path

        Parameters
        ----------
        blueprint : FileSetBlueprint
            blueprint of the file-set to create, a file or directory will be created
            depending on the name given
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
        for fname in blueprint.filenames:
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
                    return out_path
                elif not source_fallback:
                    raise ArcanaError(
                        f"Couldn't find {fname} in source data directory {source_data}"
                    )
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
        return blueprint.datatype(out_path)

    def make_dataset(
        self,
        store: DataStore,
        dataset_id: str,
        source_data: Path = None,
        **kwargs,
    ):
        """For use in tests, this method creates a test dataset from the provided
        blueprint"""
        dataset = self.create_dataset(
            store, dataset_id, source_data=source_data, **kwargs
        )
        dataset.__annotations__["blueprint"] = self
        return dataset

    def access_dataset(self, store: DataStore, dataset_id: str):
        dataset = store.define_dataset(
            dataset_id,
            hierarchy=self.hierarchy,
            id_inference=self.id_inference,
        )
        dataset.__annotations__["blueprint"] = self
        return dataset
