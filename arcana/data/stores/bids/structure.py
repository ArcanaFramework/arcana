import typing as ty
import json
import re
import logging
import attrs
from dataclasses import dataclass
import jq
from pathlib import Path
from ..common import FileSystem
from arcana.core.data.format import FileGroup
from arcana.exceptions import ArcanaUsageError, ArcanaEmptyDatasetError


logger = logging.getLogger("arcana")


@dataclass
class JsonEdit:

    path: str
    # a regular expression matching the paths of files to match (omitting
    # subject/session IDs and extension)
    jq_expr: str
    # a JQ expression (see https://stedolan.github.io/jq/manual/v1.6/) with the
    # exception that '{a_column_name}' will be substituted by the file path of
    # the item matching the column ('{' and '}' need to be escaped by duplicating,
    # i.e. '{{' and '}}').

    @classmethod
    def attr_converter(cls, json_edits: list) -> list:
        if json_edits is None or json_edits is attrs.NOTHING:
            return []
        parsed = []
        for x in json_edits:
            if isinstance(x, JsonEdit):
                parsed.append(x)
            elif isinstance(x, dict):
                parsed.append(JsonEdit(**x))
            else:
                parsed.append(JsonEdit(*x))
        return parsed


@attrs.define
class Bids(FileSystem):
    """Repository for working with data stored on the file-system in BIDS format

    Parameters
    ----------
    json_edits : list[tuple[str, str]], optional
        Specifications to edit JSON files as they are written to the store to
        enable manual modification of fields to correct metadata. List of
        tuples of the form: FILE_PATH - path expression to select the files,
        EDIT_STR - jq filter used to modify the JSON document.
    """

    json_edits: ty.List[JsonEdit] = attrs.field(
        factory=list, converter=JsonEdit.attr_converter
    )

    alias = "bids"

    def find_rows(self, dataset):
        """
        Find all rows within the dataset stored in the store and
        construct the data tree within the dataset

        Parameters
        ----------
        dataset : Dataset
            The dataset to construct the tree dimensions for
        """

        try:
            dataset.load_metadata()
        except ArcanaEmptyDatasetError:
            return

        for subject_id, participant in dataset.participants.items():
            try:
                explicit_ids = {"group": participant["group"]}
            except KeyError:
                explicit_ids = {}
            if dataset.is_multi_session():
                for sess_id in (dataset.root_dir / subject_id).iterdir():
                    dataset.add_leaf([subject_id, sess_id], explicit_ids=explicit_ids)
            else:
                dataset.add_leaf([subject_id], explicit_ids=explicit_ids)

    def find_items(self, row):
        rel_session_path = self.row_path(row)
        root_dir = row.dataset.root_dir
        session_path = root_dir / rel_session_path
        session_path.mkdir(exist_ok=True)
        for modality_dir in session_path.iterdir():
            self.find_items_in_dir(modality_dir, row)
        deriv_dir = root_dir / "derivatives"
        if deriv_dir.exists():
            for pipeline_dir in deriv_dir.iterdir():
                self.find_items_in_dir(pipeline_dir / rel_session_path, row)

    def file_group_stem_path(self, file_group):
        row = file_group.row
        fs_path = self.root_dir(row)
        parts = file_group.path.split("/")
        if parts[-1] == "":
            parts = parts[:-1]
        if parts[0] == "derivatives":
            if len(parts) < 2:
                raise ArcanaUsageError(
                    "Paths should have another part after 'derivatives'"
                )
            elif len(parts) == 2 and not file_group.is_dir:
                raise ArcanaUsageError(
                    "Single-level derivative paths must be of type directory "
                    f"({file_group.path}: {file_group.format})"
                )
            # append the first to parts of the path before the row ID (e.g. sub-01/ses-02)
            fs_path = fs_path.joinpath(*parts[:2])
            parts = parts[2:]
        fs_path /= self.row_path(row)
        if parts:  # The whole derivatives directories can be the output for a BIDS app
            for part in parts[:-1]:
                fs_path /= part
            fname = (
                "_".join(row.ids[h] for h in row.dataset.hierarchy) + "_" + parts[-1]
            )
            fs_path /= fname
        return fs_path

    def fields_json_path(self, field):
        parts = field.path.split("/")
        if parts[0] != "derivatives":
            assert False, "Non-derivative fields should be taken from participants.tsv"
        return (
            field.row.dataset.root_dir.joinpath(parts[:2])
            / self.row_path(field.row)
            / self.FIELDS_FNAME
        )

    def get_field_val(self, field):
        row = field.row
        dataset = row.dataset
        if field.name in dataset.participant_attrs:
            val = dataset.participants[row.ids["subject"]]
        else:
            val = super().get_field_val(field)
        return val

    def put_file_group_paths(self, file_group: FileGroup, fs_paths: ty.Iterable[Path]):

        stored_paths = super().put_file_group_paths(file_group, fs_paths)
        for fs_path in stored_paths:
            if fs_path.suffix == ".json":
                # Ensure TaskName field is present in the JSON side-car if task
                # is in the filename
                self._edit_json(file_group, fs_path)
        return stored_paths

    def _edit_json(self, file_group: FileGroup, fs_path: str):
        """Edit JSON files as they are written to manually modify the JSON
        generated by the dcm2niix where required

        Parameters
        ----------
        fs_path : str
            Path of the JSON to potentially edit
        """
        dct = None

        def lazy_load_json():
            if dct is not None:
                return dct
            else:
                with open(fs_path) as f:
                    return json.load(f)

        # Ensure there is a value for TaskName for files that include 'task-taskname'
        # in their file path
        if match := re.match(r".*task-([a-zA-Z]+).*", file_group.path):
            dct = lazy_load_json()
            if "TaskName" not in dct:
                dct["TaskName"] = match.group(1)
        # Get dictionary containing file paths for all items in the same row
        # as the file-group so they can be used in the edits using Python
        # string templating
        col_paths = {}
        for col_name, item in file_group.row.items():
            rel_path = self.file_group_stem_path(item).relative_to(
                file_group.row.dataset.root_dir / self.row_path(file_group.row)
            )
            col_paths[col_name] = str(rel_path) + "." + file_group.ext

        for jedit in self.json_edits:
            jq_expr = jedit.jq_expr.format(**col_paths)  # subst col file paths
            if re.match(jedit.path, file_group.path):
                dct = jq.compile(jq_expr).input(lazy_load_json()).first()
        # Write dictionary back to file if it has been loaded
        if dct is not None:
            with open(fs_path, "w") as f:
                json.dump(dct, f)


def outputs_converter(outputs):
    """Sets the path of an output to '' if not provided or None"""
    return [o[:2] + ("",) if len(o) < 3 or o[2] is None else o for o in outputs]
