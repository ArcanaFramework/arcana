from __future__ import annotations
import logging
import typing as ty
from pathlib import Path
import shutil
from itertools import chain
import attrs
import attrs.filters
from attrs.converters import default_if_none
from fileformats.generic import File
from arcana.core.exceptions import (
    ArcanaDataMatchError,
    ArcanaLicenseNotFoundError,
    ArcanaNameError,
    ArcanaUsageError,
    ArcanaWrongDataSpaceError,
)
from ..space import DataSpace
from ..column import DataColumn, DataSink, DataSource
from .. import store as datastore
from ..tree import DataTree
from .metadata import DatasetMetadata, metadata_converter


if ty.TYPE_CHECKING:
    from ...deploy.image.components import License
    from ...data.entry import DataEntry

logger = logging.getLogger("arcana")


@attrs.define
class Dataset:
    """
    A representation of a "dataset", the complete collection of data
    (file-sets and fields) to be used in an analysis.

    Parameters
    ----------
    id : str
        The dataset id/path that uniquely identifies the dataset within the
        store it is stored (e.g. FS directory path or project ID)
    store : Repository
        The store the dataset is stored into. Can be the local file
        system by providing a MockRemoteStore repo.
    hierarchy : Sequence[str]
        The data frequencies that are explicitly present in the data tree.
        For example, if a MockRemoteStore dataset (i.e. directory) has
        two layer hierarchy of sub-directories, the first layer of
        sub-directories labelled by unique subject ID, and the second directory
        layer labelled by study time-point then the hierarchy would be

            ['subject', 'timepoint']

        Alternatively, in some stores (e.g. XNAT) the second layer in the
        hierarchy may be named with session ID that is unique across the project,
        in which case the layer dimensions would instead be

            ['subject', 'session']

        In such cases, if there are multiple timepoints, the timepoint ID of the
        session will need to be extracted using the `id_inference` argument.

        Alternatively, the hierarchy could be organised such that the tree
        first splits on longitudinal time-points, then a second directory layer
        labelled by member ID, with the final layer containing sessions of
        matched members labelled by their groups (e.g. test & control):

            ['timepoint', 'member', 'group']

        Note that the combination of layers in the hierarchy must span the
        space defined in the DataSpace enum, i.e. the "bitwise or" of the
        layer values of the hierarchy must be 1 across all bits
        (e.g. 'session': 0b111).
    space: DataSpace
        The space of the dataset. See https://arcana.readthedocs.io/en/latest/data_model.html#spaces)
        for a description
    id_inference : list[tuple[DataSpace, str]]
        Not all IDs will appear explicitly within the hierarchy of the data
        tree, and some will need to be inferred by extracting components of
        more specific labels.

        For example, given a set of subject IDs that combination of the ID of
        the group that they belong to and the member ID within that group
        (i.e. matched test & control would have same member ID)

            CONTROL01, CONTROL02, CONTROL03, ... and TEST01, TEST02, TEST03

        the group ID can be extracted by providing the a list of tuples
        containing ID to source the inferred IDs from coupled with a regular
        expression with named groups

            id_inference=[('subject',
                           r'(?P<group>[A-Z]+)(?P<member>[0-9]+)')}
    include : list[tuple[DataSpace, str or list[str]]]
        The IDs to be included in the dataset per row_frequency. E.g. can be
        used to limit the subject IDs in a project to the sub-set that passed
        QC. If a row_frequency is omitted or its value is None, then all available
        will be used
    exclude : list[tuple[DataSpace, str or list[str]]]
        The IDs to be excluded in the dataset per row_frequency. E.g. can be
        used to exclude specific subjects that failed QC. If a row_frequency is
        omitted or its value is None, then all available will be used
    name : str
        The name of the dataset as saved in the store under
    columns : list[tuple[str, DataSource or DataSink]
        The sources and sinks to be initially added to the dataset (columns are
        explicitly added when workflows are applied to the dataset).
    workflows : Dict[str, pydra.Workflow]
        Workflows that have been applied to the dataset to generate sink
    access_args: ty.Dict[str, Any]
        Repository specific args used to control the way the dataset is accessed
    """

    DEFAULT_NAME = "common"
    LICENSES_PATH = (
        "LICENSES"  # The resource that project-specifc licenses are expected
    )

    id: str = attrs.field(converter=str, metadata={"asdict": False})
    store: datastore.DataStore = attrs.field()
    hierarchy: ty.List[DataSpace] = attrs.field()
    space: DataSpace = attrs.field(default=None)
    metadata: DatasetMetadata = attrs.field(
        factory=DatasetMetadata,
        converter=metadata_converter,
        repr=False,
    )
    id_inference: ty.List[ty.Tuple[DataSpace, str]] = attrs.field(
        factory=dict, converter=default_if_none(factory=dict)
    )
    include: ty.List[ty.Tuple[DataSpace, str or list[str]]] = attrs.field(
        factory=dict, converter=default_if_none(factory=dict), repr=False
    )
    exclude: ty.List[ty.Tuple[DataSpace, str or list[str]]] = attrs.field(
        factory=dict, converter=default_if_none(factory=dict), repr=False
    )
    name: str = attrs.field(default=DEFAULT_NAME)
    columns: ty.Optional[ty.Dict[str, DataColumn]] = attrs.field(
        factory=dict, converter=default_if_none(factory=dict), repr=False
    )
    pipelines: ty.Dict[str, ty.Any] = attrs.field(
        factory=dict, converter=default_if_none(factory=dict), repr=False
    )
    tree: DataTree = attrs.field(factory=DataTree, init=False, repr=False, eq=False)

    def __attrs_post_init__(self):
        self.tree.dataset = self
        # Ensure that hierarchy items are in the DataSpace enums not strings
        # or set the space from the provided enums
        if self.space is not None:
            self.hierarchy = [self.space[str(h)] for h in self.hierarchy]
        else:
            first_layer = self.hierarchy[0]
            if not isinstance(first_layer, DataSpace):
                raise ArcanaUsageError(
                    "Data space needs to be specified explicitly as an "
                    "argument or implicitly via hierarchy types"
                )
            self.space = type()

        # Ensure the keys of the include, exclude and id_inference attrs are
        # in the space of the dataset
        self.include = [(self.space[str(k)], v) for k, v in self.include]
        self.exclude = [(self.space[str(k)], v) for k, v in self.exclude]
        self.id_inference = [(self.space[str(k)], v) for k, v in self.id_inference]
        # Set reference to pipeline in columns and pipelines
        for column in self.columns.values():
            column.dataset = self
        for pipeline in self.pipelines.values():
            pipeline.dataset = self

    def save(self, name=None):
        self.store.save_dataset(self, name=name)

    @classmethod
    def load(
        cls, id: str, store: datastore.DataStore = None, name: str = None, **kwargs
    ):
        """Loads a dataset from an store/ID/name string, as used in the CLI

        Parameters
        ----------
        id: str
            either the ID of a dataset if `store` keyword arg is provided or a
            "dataset ID string" in the format <store-nickname>//<dataset-id>[@<dataset-name>]
        store: DataStore, optional
            the store to load the dataset. If not provided the provided ID
            is interpreted as an ID string
        name: str, optional
            the name of the dataset within the project/directory
            (e.g. 'test', 'training'). Used to specify a subset of data rows
            to work with, within a greater project
        **kwargs
            keyword arguments parsed to the data store load

        Returns
        -------
        Dataset
            the loaded dataset"""
        if store is None:
            store_name, id, parsed_name = cls.parse_id_str(id)
            store = datastore.DataStore.load(store_name, **kwargs)
        if name is None:
            name = parsed_name
        return store.load_dataset(id, name=name)

    @columns.validator
    def columns_validator(self, _, columns):
        wrong_freq = [
            m for m in columns.values() if not isinstance(m.row_frequency, self.space)
        ]
        if wrong_freq:
            raise ArcanaUsageError(
                f"Data hierarchy of {wrong_freq} column specs do(es) not match"
                f" that of dataset {self.space}"
            )

    @exclude.validator
    def exclude_validator(self, _, exclude):
        include_freqs = set(f for f, i in self.include if i is not None)
        exclude_freqs = set(f for f, i in exclude if i is not None)
        both = include_freqs & exclude_freqs
        if both:
            raise ArcanaUsageError(
                "Cannot provide both 'include' and 'exclude' arguments "
                "for frequencies ('{}') to Dataset".format("', '".join(both))
            )

    @hierarchy.validator
    def hierarchy_validator(self, _, hierarchy):
        if not hierarchy:
            raise ArcanaUsageError(f"hierarchy provided to {self} cannot be empty")

        space = type(hierarchy[0]) if self.space is None else self.space
        not_valid = [f for f in hierarchy if f not in self.space.__members__]
        if space is str:
            raise ArcanaUsageError(
                "Either the data space of the set needs to be provided "
                "separately, or the hierarchy must be provided as members of "
                "a single data space, not strings"
            )
        try:
            hierarchy = [space[str(h)] for h in hierarchy]
        except KeyError:
            raise ArcanaWrongDataSpaceError(
                "{} are not part of the {} data space".format(
                    ", ".join(not_valid), self.space
                )
            )
        # Check that all data frequencies are "covered" by the hierarchy and
        # each subsequent
        covered = self.space(0)
        for i, layer in enumerate(hierarchy):
            diff = (layer ^ covered) & layer
            if not diff:
                raise ArcanaUsageError(
                    f"{layer} does not add any additional basis layers to "
                    f"previous layers {hierarchy[i:]}"
                )
            covered |= layer
        if covered != max(self.space):
            raise ArcanaUsageError(
                "The data hierarchy ['"
                + "', '".join(str(h) for h in hierarchy)
                + "'] does not cover the following basis frequencies ['"
                + "', '".join(str(m) for m in (covered ^ max(self.space)).span())
                + f"'] the '{self.space.__module__}.{self.space.__name__}' data space"
            )

    @property
    def root_freq(self):
        return self.space(0)

    @property
    def root_dir(self):
        return Path(self.id)

    @property
    def leaf_freq(self):
        return max(self.space)

    @property
    def prov(self):
        return {
            "id": self.id,
            "store": self.store.prov,
            "ids": {str(freq): tuple(ids) for freq, ids in self.rows.items()},
        }

    @property
    def root(self):
        """Lazily loads the data tree from the store on demand and return root

        Returns
        -------
        DataRow
            The root row of the data tree
        """
        # Build the tree cache and return the tree root. Note that if there is a
        # "with <this-dataset>.tree" statement further up the call stack then the
        # cache won't be broken down until the highest cache statement exits
        with self.tree:
            return self.tree.root

    @property
    def locator(self):
        if self.store.name is None:
            raise Exception(
                f"Must save store {self.store} first before accessing locator for "
                f"{self}"
            )
        locator = f"{self.store.name}//{self.id}"
        if self.name is not self.DEFAULT_NAME:
            locator += f"@{self.name}"
        return locator

    def add_source(
        self, name, datatype, path=None, row_frequency=None, overwrite=False, **kwargs
    ):
        """Specify a data source in the dataset, which can then be referenced
        when connecting workflow inputs.

        Parameters
        ----------
        name : str
            The name used to reference the dataset "column" for the
            source
        datatype : type
            The file-format (for file-sets) or datatype (for fields)
            that the source will be stored in within the dataset
        path : str, default `name`
            The location of the source within the dataset
        row_frequency : DataSpace, default self.leaf_freq
            The row_frequency of the source within the dataset
        overwrite : bool
            Whether to overwrite existing columns
        **kwargs : ty.Dict[str, Any]
            Additional kwargs to pass to DataSource.__init__
        """
        row_frequency = self.parse_frequency(row_frequency)
        if path is None:
            path = name
        source = DataSource(
            name=name,
            datatype=datatype,
            row_frequency=row_frequency,
            path=path,
            dataset=self,
            **kwargs,
        )
        self._add_spec(name, source, overwrite)
        return source

    def add_sink(
        self, name, datatype, path=None, row_frequency=None, overwrite=False, **kwargs
    ):
        """Specify a data source in the dataset, which can then be referenced
        when connecting workflow inputs.

        Parameters
        ----------
        name : str
            The name used to reference the dataset "column" for the
            sink
        datatype : type
            The file-format (for file-sets) or datatype (for fields)
            that the sink will be stored in within the dataset
        path : str, default `name`
            The location of the sink within the dataset
        row_frequency : DataSpace, default self.leaf_freq
            The row_frequency of the sink within the dataset
        overwrite : bool
            Whether to overwrite an existing sink
        """
        row_frequency = self.parse_frequency(row_frequency)
        if path is None:
            path = name
        sink = DataSink(
            name=name,
            datatype=datatype,
            row_frequency=row_frequency,
            path=path,
            dataset=self,
            **kwargs,
        )
        self._add_spec(name, sink, overwrite)
        return sink

    def _add_spec(self, name, spec, overwrite):
        if name in self.columns:
            if overwrite:
                logger.info(
                    f"Overwriting {self.columns[name]} with {spec} in " f"{self}"
                )
            else:
                raise ArcanaNameError(
                    name,
                    f"Name clash attempting to add {spec} to {self} "
                    f"with {self.columns[name]}. Use 'overwrite' option "
                    "if this is desired",
                )
        self.columns[name] = spec

    def row(self, row_frequency=None, id=None, **id_kwargs):
        """Returns the row associated with the given row_frequency and ids dict

        Parameters
        ----------
        row_frequency : DataSpace or str
            The row_frequency of the row
        id : str or Tuple[str], optional
            The ID of the row to
        **id_kwargs : Dict[str, str]
            Alternatively to providing `id`, ID corresponding to the row to
            return passed as kwargs

        Returns
        -------
        DataRow
            The selected data row

        Raises
        ------
        ArcanaUsageError
            Raised when attempting to use IDs with the row_frequency associated
            with the root row
        ArcanaNameError
            If there is no row corresponding to the given ids
        """
        with self.tree:
            # Parse str to row_frequency enums
            if not row_frequency:
                if id is not None:
                    raise ArcanaUsageError(f"Root rows don't have any IDs ({id})")
                return self.root
            row_frequency = self.parse_frequency(row_frequency)
            if id_kwargs:
                if id is not None:
                    raise ArcanaUsageError(
                        f"ID ({id}) and id_kwargs ({id_kwargs}) cannot be both "
                        f"provided to `row` method of {self}"
                    )
                # Iterate through the tree to find the row (i.e. tree node) matching the
                # provided IDs
                row = self.root
                for freq, id in id_kwargs.items():
                    try:
                        children_dict = row.children[self.space[freq]]
                    except KeyError as e:
                        raise ArcanaNameError(
                            freq, f"{freq} is not a child row_frequency of {row}"
                        ) from e
                    try:
                        row = children_dict[id]
                    except KeyError as e:
                        raise ArcanaNameError(
                            id, f"{id} ({freq}) not a child row of {row}"
                        ) from e
                return row
            else:
                try:
                    return self.root.children[row_frequency][id]
                except KeyError as e:
                    raise ArcanaNameError(
                        id,
                        f"{id} not present in data tree "
                        f"({list(self.row_ids(row_frequency))})",
                    ) from e

    def rows(self, frequency=None, ids=None):
        """Return all the IDs in the dataset for a given frequency

        Parameters
        ----------
        frequency : DataSpace or None
            The "frequency" of the rows, e.g. per-session, per-subject. If
            None then all rows are returned
        ids : Sequence[str or Tuple[str]]
            The i

        Returns
        -------
        Sequence[DataRow]
            The sequence of the data row within the dataset
        """
        with self.tree:
            if frequency is None:
                return chain(*(d.values() for d in self.root.children.values()))
            frequency = self.parse_frequency(frequency)
            if frequency == self.root_freq:
                return [self.root]
            rows = self.root.children[frequency].values()
            if ids is not None:
                rows = (n for n in rows if n.id in set(ids))
            return rows

    def row_ids(self, row_frequency):
        """Return all the IDs in the dataset for a given row_frequency

        Parameters
        ----------
        row_frequency : DataSpace
            The "row_frequency" of the rows, e.g. per-session, per-subject...

        Returns
        -------
        Sequence[str]
            The IDs of the rows
        """
        with self.tree:
            row_frequency = self.parse_frequency(row_frequency)
            if row_frequency == self.root_freq:
                return [None]
            return self.root.children[row_frequency].keys()

    def __getitem__(self, name):
        """Return all data items across the dataset for a given source or sink

        Parameters
        ----------
        name : str
            Name of the column to return

        Returns
        -------
        DataColumn
            the column object
        """
        return self.columns[name]

    def apply_pipeline(
        self,
        name,
        workflow,
        inputs,
        outputs,
        row_frequency=None,
        overwrite=False,
        converter_args=None,
    ):
        """Connect a Pydra workflow as a pipeline of the dataset

        Parameters
        ----------
        name : str
            name of the pipeline
        workflow : pydra.Workflow
            pydra workflow to connect to the dataset as a pipeline
        inputs : list[arcana.core.analysis.pipeline.Input or tuple[str, str, type] or tuple[str, str]]
            List of inputs to the pipeline (see `arcana.core.analysis.pipeline.Pipeline.PipelineInput`)
        outputs : list[arcana.core.analysis.pipeline.Output or tuple[str, str, type] or tuple[str, str]]
            List of outputs of the pipeline (see `arcana.core.analysis.pipeline.Pipeline.PipelineOutput`)
        row_frequency : str, optional
            the frequency of the data rows the pipeline will be executed over, i.e.
            will it be run once per-session, per-subject or per whole dataset,
            by default the highest row frequency (e.g. per-session for Clinical)
        overwrite : bool, optional
            overwrite connections to previously connected sinks, by default False
        converter_args : dict[str, dict]
            keyword arguments passed on to the converter to control how the
            conversion is performed.

        Returns
        -------
        Pipeline
            the pipeline added to the dataset

        Raises
        ------
        ArcanaUsageError
            if overwrite is false and
        """
        from arcana.core.analysis.pipeline import Pipeline

        row_frequency = self.parse_frequency(row_frequency)

        # def parsed_conns(lst, conn_type):
        #     parsed = []
        #     for spec in lst:
        #         if isinstance(spec, conn_type):
        #             parsed.append(spec)
        #         elif len(spec) == 3:
        #             parsed.append(conn_type(*spec))
        #         else:
        #             col_name, field = spec
        #             parsed.append(conn_type(col_name, field, self[col_name].datatype))
        #     return parsed

        pipeline = Pipeline(
            name=name,
            dataset=self,
            row_frequency=row_frequency,
            workflow=workflow,
            inputs=inputs,
            outputs=outputs,
            converter_args=converter_args,
        )
        for outpt in pipeline.outputs:
            sink = self[outpt.name]
            if sink.pipeline_name is not None:
                if overwrite:
                    logger.info(
                        f"Overwriting pipeline of sink '{outpt.name}' "
                        f"{sink.pipeline_name} with {name}"
                    )
                else:
                    raise ArcanaUsageError(
                        f"Attempting to overwrite pipeline of '{outpt.name}' "
                        f"sink ({sink.pipeline_name}) with {name}. Use "
                        f"'overwrite' option if this is desired"
                    )
            sink.pipeline_name = pipeline.name
        self.pipelines[name] = pipeline

        return pipeline

    def derive(self, *sink_names, ids=None, cache_dir=None, **kwargs):
        """Generate derivatives from the workflows

        Parameters
        ----------
        *sink_names : Iterable[str]
            Names of the columns corresponding to the items to derive
        ids : Iterable[str]
            The IDs of the data rows in each column to derive
        cache_dir

        Returns
        -------
        Sequence[List[DataType]]
            The derived columns
        """
        from arcana.core.analysis.pipeline import Pipeline

        sinks = [self[s] for s in set(sink_names)]
        for pipeline, _ in Pipeline.stack(*sinks):
            # Execute pipelines in stack
            # FIXME: Should combine the pipelines into a single workflow and
            # dilate the IDs that need to be run when summarising over different
            # data axes
            with self.tree:
                pipeline(ids=ids, cache_dir=cache_dir)(**kwargs)

    def parse_frequency(self, freq):
        """Parses the data row_frequency, converting from string if necessary and
        checks it matches the dimensions of the dataset"""
        if freq is None:
            return max(self.space)
        try:
            if isinstance(freq, str):
                freq = self.space[freq]
            elif not isinstance(freq, self.space):
                raise KeyError
        except KeyError as e:
            raise ArcanaWrongDataSpaceError(
                f"{freq} is not a valid dimension for {self} " f"({self.space})"
            ) from e
        return freq

    @classmethod
    def _sink_path(cls, workflow_name, sink_name):
        return f"{workflow_name}/{sink_name}"

    @classmethod
    def parse_id_str(cls, id):
        parts = id.split("//")
        if len(parts) == 1:  # No store definition, default to the `DirTree` store
            store_name = "dirtree"
        else:
            store_name, id = parts
        parts = id.split("@")
        if len(parts) == 1:
            name = cls.DEFAULT_NAME
        else:
            id, name = parts
        return store_name, id, name

    def download_licenses(self, licenses: list[License]):
        """Install licenses from project-specific location in data store and
        install them at the destination location

        Parameters
        ----------
        licenses : list[License]
            the list of licenses stored in the dataset or in a site-wide location that
            need to be downloaded to the local file-system before a pipeline is run

        Raises
        ------
        ArcanaLicenseNotFoundError
            raised if the license of the given name isn't present in the project-specific
            location to retrieve
        """
        from arcana.core.deploy.image.components import License

        site_licenses_dataset = self.store.site_licenses_dataset()

        for lic in licenses:

            missing = False
            try:
                license_file = self._get_license_file(lic.name)
            except ArcanaDataMatchError:
                if site_licenses_dataset is not None:
                    try:
                        license_file = self._get_license_file(
                            lic.name, dataset=site_licenses_dataset
                        )
                    except ArcanaDataMatchError:
                        missing = True
                else:
                    missing = True
            if missing:
                msg = (
                    f"Did not find a license corresponding to '{lic.name}' at "
                    f"{License.column_path(lic.name)} in {self}"
                )
                if site_licenses_dataset:
                    msg += f" or {site_licenses_dataset}"
                raise ArcanaLicenseNotFoundError(
                    lic.name,
                    msg,
                )
            shutil.copyfile(license_file, lic.destination)

    def install_license(self, name: str, source_file: File):
        """Store project-specific license in dataset

        Parameters
        ----------
        name : str
            name of the license to install
        source_file : File
            path to the license file to install
        """
        from arcana.core.deploy.image.components import License

        self.store.post(
            item=File(source_file),
            path=License.column_path(name),
            datatype=File,
            row=self.root,
        )

    def _get_license_file(self, name, dataset=None) -> DataEntry:
        from arcana.core.deploy.image.components import License

        if dataset is None:
            dataset = self
        column = DataSink(
            name=f"{name}_license",
            datatype=File,
            row_frequency=self.root_freq,
            dataset=dataset,
            path=License.column_path(name),
        )
        return File(column.match_entry(dataset.root).item)


@attrs.define
class SplitDataset:
    """A dataset created by combining multiple datasets into a conglomerate

    Parameters
    ----------
    """

    source_dataset: Dataset = attrs.field()
    sink_dataset: Dataset = attrs.field()
