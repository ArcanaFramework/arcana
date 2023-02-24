from __future__ import annotations
import typing as ty
import attrs
from fileformats.core.base import DataType
from arcana.core.exceptions import ArcanaDataMatchError
from .quality import DataQuality

if ty.TYPE_CHECKING:
    from .row import DataRow


@attrs.define
class ItemMetadata:
    """Metadata that is either manually set at initialisation of the DataEntry (if
    easily extracted from the data store), or lazily loaded from the entry's item if the
    entry datatype"""

    loaded: dict = attrs.field(
        default=None, converter=lambda x: dict(x) if x is not None else {}
    )
    _entry: DataEntry = attrs.field(default=None, init=False, repr=False)
    _has_been_loaded: bool = attrs.field(default=False, init=False, repr=False)

    def __iter__(self):
        raise NotImplementedError

    def __getitem__(self, key):
        try:
            return self.loaded[key]
        except KeyError:
            if not self._has_been_loaded:
                self.load()
        return self.loaded[key]

    def load(self, overwrite=False):
        assert self._entry is not None
        if hasattr(self._entry.datatype, "load_metadata"):
            item_metadata = self._entry.item.metadata
            if not overwrite:
                mismatching = [
                    k
                    for k in set(self.loaded) & set(item_metadata)
                    if self.loaded[k] != item_metadata[k]
                ]
                if mismatching:
                    raise RuntimeError(
                        "Mismatch in values between loaded and loaded metadata values, "
                        "use 'load(overwrite=True)' to overwrite:\n"
                        + "\n".join(
                            f"{k}: loaded={self.loaded[k]}, loaded={item_metadata[k]}"
                            for k in mismatching
                        )
                    )
            self.loaded.update(item_metadata)
        self._has_been_loaded = True


@attrs.define()
class DataEntry:
    """An entry in a node of the dataset tree, such as a scan in an imaging
    session in a "session node" or group-level derivative in a "group node"

    Parameters
    ----------
    id : str
        the ID of the entry within the node
    datatype : type (subclass of DataType)
        the type of the data entry
    uri : str, optional
        a URI uniquely identifying the data entry
    item_metadata : dict[str, Any]
        metadata associated with the data item itself (e.g. pulled from a file header).
        Can be supplied either when the entry is initialised (i.e. from previously extracted
        fields stored within the data store), or read from the item itself.
    order : int, optional
        the order in which the entry appears in the node (where applicable)
    provenance : dict, optional
        the provenance associated with the derivation of the entry by Arcana
        (only applicable to derivatives not source data)
    checksums : dict[str, str], optional
        checksums for all of the files in the data entry
    """

    path: str
    datatype: type
    row: DataRow
    uri: str
    item_metadata: ItemMetadata = attrs.field(
        default=None, converter=ItemMetadata, repr=False, kw_only=True
    )
    order: int = None
    quality: DataQuality = DataQuality.usable
    checksums: dict[str, ty.Union[str, dict]] = attrs.field(
        default=None, repr=False, eq=False
    )

    def __attrs_post_init__(self):
        self.item_metadata._entry = self

    @property
    def item(self) -> DataType:
        return self.get_item()

    @item.setter
    def item(self, item):
        if isinstance(item, DataType):
            if not type(item).is_subtype_of(self.datatype):
                raise ArcanaDataMatchError(
                    f"Cannot put {item} into {self.datatype} entry of {self.row}"
                )
        else:
            item = self.datatype(item)
        self.row.dataset.store.put(self, item)

    def get_item(self, datatype=None):
        if datatype is None:
            datatype = self.datatype
        return self.row.dataset.store.get(self, datatype)

    @property
    def recorded_checksums(self):
        if self.provenance is None:
            return None
        else:
            return self.provenance.outputs[self.path]
