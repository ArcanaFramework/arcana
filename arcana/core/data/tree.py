from __future__ import annotations
import logging
import typing as ty
import re
import attrs
import attrs.filters
from arcana.core.utils.misc import NestedContext
from arcana.core.exceptions import (
    ArcanaNameError,
    ArcanaDataTreeConstructionError,
    ArcanaUsageError,
    ArcanaBadlyFormattedIDError,
)
from .row import DataRow

if ty.TYPE_CHECKING:
    from .set.base import Dataset


logger = logging.getLogger("arcana")


@attrs.define
class DataTree(NestedContext):

    dataset: Dataset = None
    root: DataRow = None

    def enter(self):
        assert self.root is None
        self._set_root()
        self.dataset.store.scan_tree(self)

    def exit(self):
        self.root = None

    @property
    def dataset_id(self):
        return self.dataset.id

    @property
    def hierarchy(self):
        return self.dataset.hierarchy

    def add_leaf(self, tree_path, additional_ids=None):
        """Creates a new row at a the path down the tree of the dataset as
        well as all "parent" rows upstream in the data tree

        Parameters
        ----------
        tree_path : list[str]
            The sequence of labels for each layer in the hierarchy of the
            dataset leading to the current row.
        additional_ids : dict[DataSpace, str]
            IDs for frequencies not in the dataset hierarchy that are to be
            set explicitly

        Raises
        ------
        ArcanaBadlyFormattedIDError
            raised if one of the IDs doesn't match the pattern in the
            `id_inference`
        ArcanaDataTreeConstructionError
            raised if one of the groups specified in the ID inference reg-ex
            doesn't match a valid row_frequency in the data dimensions
        """
        if self.root is None:
            self._set_root()
        if additional_ids is None:
            additional_ids = {}
        # Get basis frequencies covered at the given depth of the
        if len(tree_path) != len(self.dataset.hierarchy):
            raise ArcanaDataTreeConstructionError(
                f"Tree path ({tree_path}) should have the same length as "
                f"the hierarchy ({self.dataset.hierarchy}) of {self}"
            )
        # Set a default ID of None for all parent frequencies that could be
        # inferred from a row at this depth
        ids = {f: None for f in self.dataset.space}
        # Calculate the combined freqs after each layer is added
        row_frequency = self.dataset.space(0)
        for layer, label in zip(self.dataset.hierarchy, tree_path):
            ids[layer] = label
            regexes = [r for ln, r in self.dataset.id_inference if ln == layer]
            if not regexes:
                # If the layer introduces completely new axes then the axis
                # with the least significant bit (the order of the bits in the
                # DataSpace class should be arranged to account for this)
                # can be considered be considered to be equivalent to the label.
                # E.g. Given a hierarchy of ['subject', 'session']
                # no groups are assumed to be present by default (although this
                # can be overridden by the `id_inference` attr) and the `member`
                # ID is assumed to be equivalent to the `subject` ID. Conversely,
                # the timepoint can't be inferred from the `session` ID, since
                # the session ID could be expected to contain the `member` and
                # `group` ID in it, and should be explicitly extracted by
                # providing a regex to `id_inference`, e.g.
                #
                #       session ID: MRH010_CONTROL03_MR02
                #
                # with the '02' part representing as the timepoint can be
                # extracted with the
                #
                #       id_inference={
                #           'session': r'.*(?P<timepoint>0-9+)$'}
                if not (layer & row_frequency):
                    ids[layer.span()[-1]] = label
            else:
                for regex in regexes:
                    match = re.match(regex, label)
                    if match is None:
                        raise ArcanaBadlyFormattedIDError(
                            f"{layer} label '{label}', does not match ID inference"
                            f" pattern '{regex}'"
                        )
                    new_freqs = (layer ^ row_frequency) & layer
                    for target_freq, target_id in match.groupdict().items():
                        target_freq = self.dataset.space[target_freq]
                        if (target_freq & new_freqs) != target_freq:
                            raise ArcanaUsageError(
                                f"Inferred ID target, {target_freq}, is not a "
                                f"data row_frequency added by layer {layer}"
                            )
                        if ids[target_freq] is not None:
                            raise ArcanaUsageError(
                                f"ID '{target_freq}' is specified twice in the ID "
                                f"inference of {tree_path} ({ids[target_freq]} "
                                f"and {target_id} from {regex}"
                            )
                        ids[target_freq] = target_id
            row_frequency |= layer
        assert row_frequency == max(self.dataset.space)
        # Set or override any inferred IDs within the ones that have been
        # explicitly provided
        ids.update((self.dataset.space[str(k)], i) for k, i in additional_ids.items())
        # Create composite IDs for non-basis frequencies if they are not
        # explicitly in the layer dimensions
        for freq in set(self.dataset.space) - set(row_frequency.span()):
            if ids[freq] is None:
                id = tuple(ids[b] for b in freq.span() if ids[b] is not None)
                if id:
                    if len(id) == 1:
                        id = id[0]
                    ids[freq] = id
        # TODO: filter row based on dataset include & exclude attrs
        return self._add_row(ids, row_frequency)

    def _add_row(self, ids, row_frequency):
        """Adds a row to the dataset, creating all parent "aggregate" rows
        (e.g. for each subject, group or timepoint) where required

        Parameters
        ----------
        row: DataRow
            The row to add into the data tree

        Raises
        ------
        ArcanaDataTreeConstructionError
            If inserting a multiple IDs of the same class within the tree if
            one of their ids is None
        """
        logger.debug(
            "Adding new %s row to %s dataset: %s", row_frequency, self.dataset_id, ids
        )
        row_frequency = self.dataset.parse_frequency(row_frequency)
        row = DataRow(ids, row_frequency, self.dataset)
        # Create new data row
        row_dict = self.root.children[row.frequency]
        if row.id in row_dict:
            raise ArcanaDataTreeConstructionError(
                f"ID clash ({row.id}) between rows inserted into data " "tree"
            )
        row_dict[row.id] = row
        # Insert root row
        # Insert parent rows if not already present and link them with
        # inserted row
        for parent_freq, parent_id in row.ids.items():
            if not parent_freq:
                continue  # Don't need to insert root row again
            diff_freq = (row.frequency ^ parent_freq) & row.frequency
            if diff_freq:
                # logger.debug(f'Linking parent {parent_freq}: {parent_id}')
                try:
                    parent_row = self.dataset.row(parent_freq, parent_id)
                except ArcanaNameError:
                    # logger.debug(
                    #     f'Parent {parent_freq}:{parent_id} not found, adding')
                    parent_ids = {
                        f: i
                        for f, i in row.ids.items()
                        if (f.is_parent(parent_freq) or f == parent_freq)
                    }
                    parent_row = self._add_row(parent_ids, parent_freq)
                # Set reference to level row in new row
                diff_id = row.ids[diff_freq]
                children_dict = parent_row.children[row_frequency]
                if diff_id in children_dict:
                    raise ArcanaDataTreeConstructionError(
                        f"ID clash ({diff_id}) between rows inserted into "
                        f"data tree in {diff_freq} children of {parent_row} "
                        f"({children_dict[diff_id]} and {row}). You may "
                        f"need to set the `id_inference` attr of the dataset "
                        "to disambiguate ID components (e.g. how to extract "
                        "the timepoint ID from a session label)"
                    )
                children_dict[diff_id] = row
        return row

    def _set_root(self):
        self.root = DataRow(
            {self.dataset.root_freq: None}, self.dataset.root_freq, self.dataset
        )
