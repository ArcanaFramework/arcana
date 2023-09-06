from __future__ import annotations
import typing as ty
import itertools
from abc import ABCMeta, abstractmethod
from pathlib import Path
import tempfile
import shutil
import logging
import glob
import decimal
from copy import deepcopy
import zipfile
import attrs
from fileformats.core import FileSet, Field
from fileformats.generic import Directory
from fileformats.text import TextFile
from fileformats.application import Zip, Json
from fileformats.field import Text as TextField, Decimal, Boolean, Integer, Array
from fileformats.testing import (
    MyFormatGz,
    MyFormatGzX,
    MyFormatX,
    MyFormat,
    ImageWithHeader,
    YourFormat,
    Xyz,
)
from arcana.core.data.row import DataRow
from arcana.core.data.space import DataSpace
from arcana.core.utils.misc import path2varname, set_cwd
from arcana.core.exceptions import ArcanaError
from arcana.core.data.store import DataStore
from .space import TestDataSpace

logger = logging.getLogger("arcana")


@attrs.define(kw_only=True)
class EntryBlueprint(metaclass=ABCMeta):

    path: str
    datatype: type = attrs.field()
    row_frequency: ty.Optional[str] = None
    ids: ty.Optional[
        ty.List[str]
    ] = None  # the list of row IDs to create the blueprint in
    alternative_datatypes: ty.List[type] = attrs.field(factory=list)

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
        logger.debug("Creating entry at %s in %s", self.path, row)
        entry = row.dataset.store.create_entry(
            path=self.path,
            datatype=self.datatype,
            row=row,
        )
        logger.debug("Putting %s at %s", item, entry)
        row.dataset.store.put(item, entry)


@attrs.define(kw_only=True)
class FileSetEntryBlueprint(EntryBlueprint):

    filenames: ty.List[str]

    def make_item(
        self,
        source_data: ty.Optional[Path] = None,
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
                fspaths = [Path(f) for f in glob.glob(str(src_path))]
                if fspaths:
                    for fspath in fspaths:
                        if escape_source_name:
                            parts = fname.split(".")
                            out_fname = (
                                path2varname(parts[0]) + "." + ".".join(parts[1:])
                            )
                        else:
                            out_fname = Path(fname).name
                        out_path = tmp_dir / out_fname
                        if fspath.is_dir():
                            shutil.copytree(fspath, out_path)
                        else:
                            shutil.copyfile(fspath, out_path, follow_symlinks=True)
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
        item = self.datatype(out_paths)
        logger.debug("Created %s item", item)
        return item


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

    space: ty.Type[DataSpace]
    hierarchy: ty.List[str]
    dim_lengths: ty.List[int]  # size of layers a-d respectively
    entries: ty.List[EntryBlueprint] = attrs.field(factory=list)
    derivatives: ty.List[EntryBlueprint] = attrs.field(factory=list)
    id_patterns: ty.Dict[str, str] = attrs.field(factory=dict)
    include: ty.Dict[str, ty.Union[str, ty.List[str]]] = attrs.field(factory=dict)
    exclude: ty.Dict[str, ty.Union[str, ty.List[str]]] = attrs.field(factory=dict)

    DEFAULT_NUM_ACCESS_ATTEMPTS = 300
    DEFAULT_ACCESS_ATTEMPT_INTERVAL = 1.0  # secs

    def make_dataset(
        self,
        store: DataStore,
        dataset_id: str,
        name: ty.Optional[str] = None,
        source_data: ty.Optional[Path] = None,
        metadata: ty.Optional[ty.Dict[str, ty.Any]] = None,
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
            passed through to create_dataset
        """
        if metadata is None:
            metadata = {}
        orig_type = metadata.get("type", "derivative")
        metadata["type"] = "in-construction"
        with store.connection:
            logger.debug(
                "Creating test dataset in %s at %s from %s", store, dataset_id, self
            )
            if self.id_patterns:
                kwargs = {"id_patterns": self.id_patterns}
            dataset = store.create_dataset(
                id=dataset_id,
                leaves=self.all_ids,
                name=name,
                hierarchy=self.hierarchy,
                space=self.space,
                metadata=metadata,
                include=self.include,
                exclude=self.exclude,
                **kwargs,
            )
        with store.connection:
            logger.debug(
                "Adding entries to test dataset for: %s",
                dataset.rows(frequency=max(self.space)),
            )
            for row in dataset.rows(frequency=max(self.space)):
                self.make_entries(row, source_data=source_data)
            dataset.metadata.type = orig_type
            dataset.save()
        dataset.__annotations__["blueprint"] = self
        logger.debug("Successfully created test dataset at %s in %s", dataset_id, store)
        return dataset

    def translate_to(self, data_store: DataStore) -> "TestDatasetBlueprint":
        """Translates the blueprint so that it matches the default space and hierarchy
        of the data store (if applicable)

        Parameters
        ----------
        data_store : DataStore
            the data store to get the defaults for

        Returns
        -------
        blueprint : TestDatasetBlueprint
        """
        # Create copy of the blueprint
        blueprint = deepcopy(self)
        try:
            blueprint.space = data_store.DEFAULT_SPACE
        except AttributeError:
            space = TestDataSpace
        else:
            try:
                blueprint.hierarchy = data_store.DEFAULT_HIERARCHY
            except AttributeError:
                if space.ndim > self.space.ndim:
                    raise RuntimeError(
                        f"cannot translate hierarchy as from {self.space} to {space} "
                        "as it has more dimensions"
                    )
                # Translate frequencies into new space
                blueprint.hierarchy = [
                    space.union(*f.span()[-space.ndim :]) for f in self.hierarchy
                ]
                # Drop frequencies that mapped onto same value
                blueprint.hierarchy = [
                    h
                    for i, h in enumerate(blueprint.hierarchy)
                    if i == 0 or h != blueprint.hierarchy[i - 1]
                ]
                blueprint.dim_lengths = self.dim_lengths[-len(blueprint.hierarchy) :]
        return blueprint

    # def access_dataset(
    #     self,
    #     store: DataStore,
    #     dataset_id: str,
    #     name: ty.Optional[str] = None,
    #     max_num_attempts: int = DEFAULT_NUM_ACCESS_ATTEMPTS,
    #     attempt_interval: float = DEFAULT_ACCESS_ATTEMPT_INTERVAL,
    # ):
    #     """For data stores with significant latency, this method can be used to reuse
    #     test datasets between tests

    #     Parameters
    #     ----------
    #     store : DataStore
    #         the data store to access the dataset from
    #     dataset_id : str
    #         the ID of the dataset to access
    #     name : str, optional
    #         the name of the dataset
    #     max_num_attempts: int, optional
    #         the maximum number of attempts to try to access
    #     attempt_interval: float, optional
    #         the time (in secs) between each attempt

    #     Returns
    #     -------
    #     Dataset
    #         the accessed dataset
    #     """
    #     num_attempts = 0
    #     while num_attempts < max_num_attempts:
    #         try:
    #             dataset = store.load_dataset(dataset_id, name=name)
    #         except KeyError:
    #             pass
    #         else:
    #             if dataset.metadata.type != "in-construction":
    #                 break
    #         logger.info(
    #             "Waiting for test dataset '%s' to finish being constructed",
    #             dataset_id,
    #         )
    #         time.sleep(attempt_interval)
    #         num_attempts += 1
    #     if num_attempts >= max_num_attempts:
    #         wait_time = max_num_attempts * attempt_interval
    #         raise RuntimeError(
    #             f"Could not access {dataset_id} dataset in {store} after waiting "
    #             f"{wait_time}, something may have gone wrong in the construction process"
    #         )
    #     dataset.__annotations__["blueprint"] = self
    #     return dataset

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
        logger.debug("making entries for %s: %s", row, self.entries)
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


TEST_DATASET_BLUEPRINTS = {
    "full": TestDatasetBlueprint(  # dataset name
        space=TestDataSpace,
        hierarchy=["a", "b", "c", "d"],
        dim_lengths=[2, 3, 4, 5],
        entries=[
            FileSetEntryBlueprint(
                path="file1", datatype=TextFile, filenames=["file1.txt"]
            ),
            FileSetEntryBlueprint(
                path="file2", datatype=MyFormatGz, filenames=["file2.my.gz"]
            ),
            FileSetEntryBlueprint(path="dir1", datatype=Directory, filenames=["dir1"]),
            FieldEntryBlueprint(
                path="textfield",
                row_frequency="abcd",
                datatype=TextField,
                value="sample-text",
            ),  # Derivatives to insert
            FieldEntryBlueprint(
                path="booleanfield",
                row_frequency="c",
                datatype=Boolean,
                value="no",
                expected_value=False,
            ),  # Derivatives to insert
        ],
        derivatives=[
            FileSetEntryBlueprint(
                path="deriv1",
                row_frequency="abcd",
                datatype=TextFile,
                filenames=["file1.txt"],
            ),  # Derivatives to insert
            FileSetEntryBlueprint(
                path="deriv2",
                row_frequency="c",
                datatype=Directory,
                filenames=["dir"],
            ),
            FileSetEntryBlueprint(
                path="deriv3",
                row_frequency="bd",
                datatype=TextFile,
                filenames=["file1.txt"],
            ),
            FieldEntryBlueprint(
                path="integerfield",
                row_frequency="c",
                datatype=Integer,
                value=99,
            ),
            FieldEntryBlueprint(
                path="decimalfield",
                row_frequency="bd",
                datatype=Decimal,
                value="33.3333",
                expected_value=decimal.Decimal("33.3333"),
            ),
            FieldEntryBlueprint(
                path="arrayfield",
                row_frequency="bd",
                datatype=Array[Integer],
                value=[1, 2, 3, 4, 5],
            ),
        ],
    ),
    "one_layer": TestDatasetBlueprint(
        space=TestDataSpace,
        hierarchy=["abcd"],
        dim_lengths=[1, 1, 1, 5],
        entries=[
            FileSetEntryBlueprint(
                path="file1",
                datatype=MyFormatGzX,
                filenames=["file1.my.gz", "file1.json"],
                alternative_datatypes=[MyFormatGz, Json],
            ),
            FileSetEntryBlueprint(
                path="file2",
                datatype=MyFormatX,
                filenames=["file2.my", "file2.json"],
                alternative_datatypes=[MyFormat, Json],
            ),
        ],
        derivatives=[
            FileSetEntryBlueprint(
                path="deriv1",
                row_frequency="abcd",
                datatype=Json,
                filenames=["file1.json"],
            ),
            FileSetEntryBlueprint(
                path="deriv2",
                row_frequency="bc",
                datatype=Xyz,
                filenames=["file1.x", "file1.y", "file1.z"],
            ),
            FileSetEntryBlueprint(
                path="deriv3",
                row_frequency="__",
                datatype=YourFormat,
                filenames=["file1.yr"],
            ),
        ],
    ),
    "skip_single": TestDatasetBlueprint(
        space=TestDataSpace,
        hierarchy=["a", "bc", "d"],
        dim_lengths=[2, 1, 2, 3],
        entries=[
            FileSetEntryBlueprint(
                path="doubledir1", datatype=Directory, filenames=["doubledir1"]
            ),
            FileSetEntryBlueprint(
                path="doubledir2", datatype=Directory, filenames=["doubledir2"]
            ),
        ],
        derivatives=[
            FileSetEntryBlueprint(
                path="deriv1",
                row_frequency="ad",
                datatype=Json,
                filenames=["file1.json"],
            )
        ],
    ),
    "skip_with_inference": TestDatasetBlueprint(
        space=TestDataSpace,
        hierarchy=["bc", "ad"],
        dim_lengths=[2, 3, 2, 4],
        id_patterns={
            "a": r"ad::a(\d+)d\d+",
            "b": r"bc::b(\d+)c\d+",
            "c": r"bc::b\d+c(\d+)",
            "d": r"ad::a\d+d(\d+)",
        },
        entries=[
            FileSetEntryBlueprint(
                path="file1",
                datatype=ImageWithHeader,
                filenames=["file1.hdr", "file1.img"],
            ),
            FileSetEntryBlueprint(
                path="file2", datatype=YourFormat, filenames=["file2.yr"]
            ),
        ],
    ),
    "redundant": TestDatasetBlueprint(
        space=TestDataSpace,
        hierarchy=[
            "abc",
            "abcd",
        ],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        dim_lengths=[3, 4, 5, 6],
        id_patterns={
            "a": r"abc::a(\d+)b\d+c\d+",
            "b": r"abc::a\d+b(\d+)c\d+",
            "c": r"abc::a\d+b\d+c(\d+)",
            "d": r"abcd::a\d+b\d+c\d+d(\d+)",
        },
        entries=[
            FileSetEntryBlueprint(
                path="doubledir", datatype=Directory, filenames=["doubledir"]
            ),
            FileSetEntryBlueprint(
                path="file1", datatype=Xyz, filenames=["file1.x", "file1.y", "file1.z"]
            ),
        ],
        derivatives=[
            FileSetEntryBlueprint(
                path="deriv1",
                row_frequency="d",
                datatype=Json,
                filenames=["file1.json"],
            )
        ],
    ),
    "concatenate_test": TestDatasetBlueprint(
        space=TestDataSpace,
        hierarchy=[
            "abcd"
        ],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        dim_lengths=[1, 1, 1, 2],
        entries=[
            FileSetEntryBlueprint(
                path="file1", datatype=TextFile, filenames=["file1.txt"]
            ),
            FileSetEntryBlueprint(
                path="file2", datatype=TextFile, filenames=["file2.txt"]
            ),
        ],
    ),
    "concatenate_zip_test": TestDatasetBlueprint(
        space=TestDataSpace,
        hierarchy=[
            "abcd"
        ],  # e.g. XNAT where session ID is unique in project but final layer is organised by timepoint
        dim_lengths=[1, 1, 1, 1],
        entries=[
            FileSetEntryBlueprint(path="file1", datatype=Zip, filenames=["file1.zip"]),
            FileSetEntryBlueprint(path="file2", datatype=Zip, filenames=["file2.zip"]),
        ],
    ),
}


GOOD_DATASETS = ["full", "one_layer", "skip_single", "skip_with_inference", "redundant"]


EXTENSION_DATASET_BLUEPRINTS = {
    "complete": TestDatasetBlueprint(  # dataset name
        space=TestDataSpace,
        hierarchy=["a", "b", "c", "d"],
        dim_lengths=[2, 2, 2, 2],
        entries=[
            FileSetEntryBlueprint(
                path="file1", datatype=TextFile, filenames=["file.txt"]
            ),
            FileSetEntryBlueprint(
                path="file2", datatype=MyFormatGz, filenames=["file.my.gz"]
            ),
            FileSetEntryBlueprint(
                path="file3",
                datatype=MyFormatGzX,
                filenames=["file.my.gz", "file.json"],
            ),
            FileSetEntryBlueprint(path="dir1", datatype=Directory, filenames=["dir1"]),
            FieldEntryBlueprint(
                path="textfield",
                row_frequency="abcd",
                datatype=TextField,
                value="sample-text",
            ),  # Derivatives to insert
            FieldEntryBlueprint(
                path="booleanfield",
                row_frequency="c",
                datatype=Boolean,
                value="no",
                expected_value=False,
            ),  # Derivatives to insert
        ],
        derivatives=[
            FileSetEntryBlueprint(
                path="deriv1",
                row_frequency="abcd",
                datatype=TextFile,
                filenames=["file1.txt"],
            ),  # Derivatives to insert
            FileSetEntryBlueprint(
                path="deriv2",
                row_frequency="c",
                datatype=Directory,
                filenames=["dir"],
            ),
            FileSetEntryBlueprint(
                path="deriv3",
                row_frequency="bd",
                datatype=TextFile,
                filenames=["file1.txt"],
            ),
            FieldEntryBlueprint(
                path="integerfield",
                row_frequency="c",
                datatype=Integer,
                value=99,
            ),
            FieldEntryBlueprint(
                path="decimalfield",
                row_frequency="bd",
                datatype=Decimal,
                value="33.3333",
                expected_value=decimal.Decimal("33.3333"),
            ),
            FieldEntryBlueprint(
                path="arrayfield",
                row_frequency="bd",
                datatype=Array[Integer],
                value=[1, 2, 3, 4, 5],
            ),
        ],
    ),
}

SIMPLE_DATASET = TestDatasetBlueprint(  # dataset name
    space=TestDataSpace,
    hierarchy=["abcd"],
    dim_lengths=[2, 2, 2, 2],
    entries=[
        FileSetEntryBlueprint(path="file1", datatype=TextFile, filenames=["file.txt"]),
        FieldEntryBlueprint(path="field1", datatype=TextField, value="a field"),
    ],
)
