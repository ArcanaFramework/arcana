import typing as ty
import json
import re
import logging
import attr
from attr.converters import default_if_none
from pathlib import Path
import jsonpath_ng
import numexpr
from arcana import __version__
from ..common import FileSystem
from arcana.core.data.format import FileGroup
from arcana.exceptions import ArcanaUsageError, ArcanaEmptyDatasetError


logger = logging.getLogger('arcana')


@attr.s
class Bids(FileSystem):
    """Repository for working with data stored on the file-system in BIDS format

    Parameters
    ----------
    json_edits : list[tuple[str, str, str]], optional
        Specifications to edit JSON files as they are written to the store to
        enable manual modification of fields to correct metadata. List of
        tuples of the form: FILE_PATH - path expression to select the files,
        JSON_PATH - JSONPath expression to the fields to edit, EDIT_STR -
        mathematical expression used to edit the field.
    """

    json_edits: ty.List[ty.Tuple[str, str, str]] = attr.ib(
        factory=list, converter=default_if_none(factory=list))

    alias = "bids"

    def find_nodes(self, dataset):
        """
        Find all nodes within the dataset stored in the store and
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
                explicit_ids = {'group': participant['group']}
            except KeyError:
                explicit_ids = {}
            if dataset.is_multi_session():
                for sess_id in (dataset.root_dir / subject_id).iterdir():
                    dataset.add_leaf_node([subject_id, sess_id],
                                          explicit_ids=explicit_ids)
            else:
                dataset.add_leaf_node([subject_id],
                                      explicit_ids=explicit_ids)

    def find_items(self, data_node):
        rel_session_path = self.node_path(data_node)
        root_dir = data_node.dataset.root_dir
        session_path = (root_dir / rel_session_path)
        session_path.mkdir(exist_ok=True)
        for modality_dir in session_path.iterdir():
            self.find_items_in_dir(modality_dir, data_node)
        deriv_dir = (root_dir / 'derivatives')
        if deriv_dir.exists():
            for pipeline_dir in deriv_dir.iterdir():
                self.find_items_in_dir(pipeline_dir / rel_session_path,
                                       data_node)        

    def file_group_stem_path(self, file_group):
        dn = file_group.data_node
        fs_path = self.root_dir(dn)
        parts = file_group.path.split('/')
        if parts[-1] == '':
            parts = parts[:-1]
        if is_derivative:= (parts[0] == 'derivatives'):
            if len(parts) < 2:
                raise ArcanaUsageError(
                    f"Paths should have another part after 'derivatives'")
            elif len(parts) == 2 and not file_group.is_dir:
                raise ArcanaUsageError(
                    "Single-level derivative paths must be of type directory "
                    f"({file_group.path}: {file_group.format})")
            # append the first to parts of the path before the row ID (e.g. sub-01/ses-02)
            fs_path = fs_path.joinpath(*parts[:2])
            parts = parts[2:]
        fs_path /= self.node_path(dn)
        if parts:  # The whole derivatives directories can be the output for a BIDS app
            for part in parts[:-1]:
                fs_path /= part
            fname = '_'.join(dn.ids[h] for h in dn.dataset.hierarchy) + '_' + parts[-1]
            fs_path /= fname
        return fs_path

    def fields_json_path(self, field):
        parts = field.path.split('/')
        if parts[0] != 'derivatives':
            assert False, "Non-derivative fields should be taken from participants.tsv"
        return (field.data_node.dataset.root_dir.joinpath(parts[:2])
                / self.node_path(field.data_node) / self.FIELDS_FNAME)

    def get_field_val(self, field):
        data_node = field.data_node
        dataset = data_node.dataset
        if field.name in dataset.participant_attrs:
            val = dataset.participants[data_node.ids['subject']]
        else:
            val = super().get_field_val(field)
        return val

    def put_file_group_paths(self, file_group: FileGroup, fs_paths: ty.Iterable[Path]):
        
        stored_paths = super().put_file_group_paths(file_group, fs_paths)
        for fs_path in stored_paths:
            if fs_path.suffix == '.json':
                # Ensure TaskName field is present in the JSON side-car if task
                # is in the filename
                self._edit_json(file_group.path, fs_path)
        return stored_paths

    def _edit_json(self, name_path: str, fs_path: str):
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
        if match:= re.match(r'.*task-([a-zA-Z]+).*', name_path):
            dct = lazy_load_json()
            if 'TaskName' not in dct:
                dct['TaskName'] = match.group(1)
        for name_path_re, jpath_str, edit_str in self.json_edits:
            if re.match(name_path_re, name_path):
                dct = lazy_load_json()
                jpath = jsonpath_ng.parse(jpath_str)
                for match in jpath.find(dct):
                    new_value = numexpr.evaluate(
                        edit_str.format(value=match.value)).item()
                    match.full_path.update(dct, new_value)
        # Write dictionary back to file if it has been loaded
        if dct is not None:
            with open(fs_path, 'w') as f:
                json.dump(dct, f)


def outputs_converter(outputs):
    """Sets the path of an output to '' if not provided or None"""
    return [o[:2] + ('',) if len(o) < 3 or o[2] is None else o for o in outputs]

