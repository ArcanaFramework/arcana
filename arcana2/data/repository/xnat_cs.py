import os
import os.path as op
import logging
import json
from arcana2.data import FileGroup, Field
from ..provenance import Record
from arcana2.exceptions import (
    ArcanaRepositoryError)
from arcana2.utils import split_extension
from arcana2.exceptions import ArcanaUsageError
from .xnat import XnatRepo


logger = logging.getLogger('arcana2')

COMMAND_INPUT_TYPES = {
    bool: 'bool',
    str: 'string',
    int: 'number',
    float: 'number'}


class XnatCSRepo(XnatRepo):
    """
    A 'Repository' class for data stored within a XNAT repository and accessed
    via the XNAT container service.

    Parameters
    ----------
    root_dir : str (path)
        Path to local directory containing data
    """

    type = 'xnat_cs'
    SUMMARY_NAME = '__ALL__'
    FIELDS_FNAME = 'fields.json'
    PROV_DIR = '__prov__'
    LOCK_SUFFIX = '.lock'
    MAX_DEPTH = 2

    def __init__(self, input_dir, output_dir, session_id=None):
        super().__init__(server=os.environ['XNAT_HOST'],
                         cache_dir=None,
                         user=os.environ['XNAT_USER'],
                         password=os.environ['XNAT_PASS'])
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.session_id = session_id

    def __eq__(self, other):
        try:
            return self.server == other.server
        except AttributeError:
            return False

    def __hash__(self):
        return hash(self.server)

    def __repr__(self):
        return ("{}(server={})"
                .format(type(self).__name__, self.server))

    def dataset_cache_dir(self, dataset_name):
        return None

    # root_dir=None, all_from_analysis=None,
    def find_data(self, dataset, subject_ids=None, visit_ids=None, **kwargs):
        """
        Find all data within a repository, registering file_groups, fields and
        provenance with the found_file_group, found_field and found_provenance
        methods, respectively

        Parameters
        ----------
        dataset : Dataset
            A dataset from which the root directory will be extracted from its
            name
        subject_ids : list(str)
            List of subject IDs with which to filter the tree with. If
            None all are returned
        visit_ids : list(str)
            List of visit IDs with which to filter the tree with. If
            None all are returned

        Returns
        -------
        file_groups : list[FileGroup]
            All the file_groups found in the repository
        fields : list[Field]
            All the fields found in the repository
        records : list[Record]
            The provenance records found in the repository
        """
        all_file_groups = []
        all_fields = []
        all_records = []

        project_id = dataset.name

        with self:
            xproject = self.login.projects[project_id]
            if self.session_id:
                xsessions = [xproject.experiments[self.session_id]]
            else:
                xsessions = xproject.experiment.values()

            for xsession in xsessions:
                xsubject = xsession.subject()
                subject_id = xsubject.label
                # Strip subject ID from session label if required
                if xsession.label.startswith(subject_id + '_'):
                    visit_id = xsession.label[len(subject_id) + 1:]
                else:
                    visit_id = xsession.label
                session_json = self.login.get_json(
                    '/data/projects/{}/experiments/{}'.format(
                        project_id, xsession.id))['items'][0]
                session_path = op.join(self.input_dir, xsession.label)
                # Get field values. We do this first so we can check for the
                # DERIVED_FROM_FIELD to determine the correct session label and
                # analysis name
                field_values = {}
                try:
                    fields_json = next(
                        c['items'] for c in session_json['children']
                        if c['field'] == 'fields/field')
                except StopIteration:
                    pass
                else:
                    for js in fields_json:
                        try:
                            value = js['data_fields']['field']
                        except KeyError:
                            pass
                        else:
                            field_values[js['data_fields']['name']] = value
                for name, value in field_values.items():
                    value = value.replace('&quot;', '"')
                    all_fields.append(Field(
                        name=name, value=value,
                        dataset=dataset,
                        subject_id=subject_id,
                        visit_id=visit_id
                        **kwargs))

            filtered_files = self._filter_files(files, session_path)
            for fname in filtered_files:
                basename = split_extension(fname)[0]
                all_file_groups.append(
                    FileGroup.from_path(
                        op.join(session_path, fname),
                        tree_level=tree_level,
                        subject_id=subj_id, visit_id=visit_id,
                        dataset=dataset,
                        from_analysis=from_analysis,
                        potential_aux_files=[
                            f for f in filtered_files
                            if (split_extension(f)[0] == basename
                                and f != fname)],
                        **kwargs))
            for fname in self._filter_dirs(dirs, session_path):
                all_file_groups.append(
                    FileGroup.from_path(
                        op.join(session_path, fname),
                        tree_level=tree_level,
                        subject_id=subj_id, visit_id=visit_id,
                        dataset=dataset,
                        from_analysis=from_analysis,
                        **kwargs))
            if self.FIELDS_FNAME in files:
                with open(op.join(session_path,
                                  self.FIELDS_FNAME), 'r') as f:
                    dct = json.load(f)
                all_fields.extend(
                    Field(name=k, value=v, tree_level=tree_level,
                          subject_id=subj_id, visit_id=visit_id,
                          dataset=dataset, from_analysis=from_analysis,
                          **kwargs)
                    for k, v in list(dct.items()))
            if self.PROV_DIR in dirs:
                if from_analysis is None:
                    raise ArcanaRepositoryError(
                        "Found provenance directory in session directory (i.e."
                        " not in analysis-specific sub-directory)")
                base_prov_dir = op.join(session_path, self.PROV_DIR)
                for fname in os.listdir(base_prov_dir):
                    all_records.append(Record.load(
                        split_extension(fname)[0],
                        tree_level, subj_id, visit_id, from_analysis,
                        op.join(base_prov_dir, fname)))
        return all_file_groups, all_fields, all_records

    def file_group_path(self, item, dataset=None, fname=None):
        pass

    def fields_json_path(self, field, dataset=None):
        return self.file_group_path(field, fname=self.FIELDS_FNAME,
                                 dataset=dataset)

    def prov_json_path(self, record, dataset):
        return self.file_group_path(record,
                                 dataset=dataset,
                                 fname=op.join(self.PROV_DIR,
                                               record.pipeline_name + '.json'))

    @classmethod
    def make_command_json(cls, image_name, analysis_cls, inputs, outputs,
                          parameters, desc, tree_level='per_session',
                          docker_index="https://index.docker.io/v1/"):

        if tree_level != 'per_session':
            raise NotImplementedError(
                "Support for frequencies other than '{}' haven't been "
                "implemented yet".format(tree_level))
        try:
            analysis_name, version = image_name.split('/')[1].split(':')
        except (IndexError, ValueError):
            raise ArcanaUsageError(
                "The Docker organisation and tag needs to be provided as part "
                "of the image, e.g. australianimagingservice/dwiqa:0.1")

        cmd_inputs = []
        input_names = []
        for inpt in inputs:
            input_name = inpt if isinstance(inpt, str) else inpt[0]
            input_names.append(input_name)
            spec = analysis_cls.data_spec(input_name)
            desc = spec.desc if spec.desc else ""
            if spec.is_file_group:
                desc = ("Scan match: {} [SCAN_TYPE [ORDER [TAG=VALUE, ...]]]"
                        .format(desc))
            else:
                desc = "Field match: {} [FIELD_NAME]".format(desc)
            cmd_inputs.append({
                "name": input_name,
                "description": desc,
                "type": "string",
                "default-value": "",
                "required": True,
                "user-settable": True,
                "replacement-key": "#{}_INPUT#".format(input_name.upper())})

        for param in parameters:
            spec = analysis_cls.param_spec(param)
            desc = "Parameter: " + spec.desc
            if spec.choices:
                desc += " (choices: {})".format(','.join(spec.choices))

            cmd_inputs.append({
                "name": param,
                "description": desc,
                "type": COMMAND_INPUT_TYPES[spec.dtype],
                "default-value": (spec.default
                                  if spec.default is not None else ""),
                "required": spec.default is None,
                "user-settable": True,
                "replacement-key": "#{}_PARAM#".format(param.upper())})

        cmd_inputs.append(
            {
                "name": "project-id",
                "description": "Project ID",
                "type": "string",
                "required": True,
                "user-settable": False,
                "replacement-key": "#PROJECT_ID#"
            })


        cmdline = (
            "banana derive /input {cls} {name} {derivs} {inputs} {params}"
            " --scratch /work --repository xnat_cs #PROJECT_URI#"
            .format(
                cls='.'.join((analysis_cls.__module__, analysis_cls.__name__)),
                name=analysis_name,
                derivs=' '.join(outputs),
                inputs=' '.join('-i {} #{}_INPUT#'.format(i, i.upper())
                                for i in input_names),
                params=' '.join('-p {} #{}_PARAM#'.format(p, p.upper())
                                for p in parameters)))

        if tree_level == 'per_session':
            cmd_inputs.append(
                {
                    "name": "session-id",
                    "description": "",
                    "type": "string",
                    "required": True,
                    "user-settable": False,
                    "replacement-key": "#SESSION_ID#"
                })
            cmdline += "#SESSION_ID# --session_ids #SESSION_ID# "

        return {
            "name": analysis_name,
            "description": desc,
            "label": analysis_name,
            "version": version,
            "schema-version": "1.0",
            "image": image_name,
            "index": docker_index,
            "type": "docker",
            "command-line": cmdline,
            "override-entrypoint": True,
            "mounts": [
                {
                    "name": "in",
                    "writable": False,
                    "path": "/input"
                },
                {
                    "name": "output",
                    "writable": True,
                    "path": "/output"
                },
                {
                    "name": "work",
                    "writable": True,
                    "path": "/work"
                }
            ],
            "ports": {},
            "inputs": cmd_inputs,
            "outputs": [
                {
                    "name": "output",
                    "description": "Derivatives",
                    "required": True,
                    "mount": "out",
                    "path": None,
                    "glob": None
                },
                {
                    "name": "working",
                    "description": "Working directory",
                    "required": True,
                    "mount": "work",
                    "path": None,
                    "glob": None
                }
            ],
            "xnat": [
                {
                    "name": analysis_name,
                    "description": desc,
                    "contexts": ["xnat:imageSessionData"],
                    "external-inputs": [
                        {
                            "name": "session",
                            "description": "Imaging session",
                            "type": "Session",
                            "matcher": None,
                            "default-value": None,
                            "required": True,
                            "replacement-key": None,
                            "sensitive": None,
                            "provides-value-for-command-input": None,
                            "provides-files-for-command-mount": "in",
                            "via-setup-command": None,
                            "user-settable": None,
                            "load-children": True
                        }
                    ],
                    "derived-inputs": [
                        {
                            "name": "session-id",
                            "type": "string",
                            "required": True,
                            "load-children": True,
                            "derived-from-wrapper-input": "session",
                            "derived-from-xnat-object-property": "id",
                            "provides-value-for-command-input": "session-id"
                        },
                        {
                            "name": "subject",
                            "type": "Subject",
                            "required": True,
                            "user-settable": False,
                            "load-children": True,
                            "derived-from-wrapper-input": "session"
                        },
                        {
                            "name": "project-id",
                            "type": "string",
                            "required": True,
                            "load-children": True,
                            "derived-from-wrapper-input": "subject",
                            "derived-from-xnat-object-property": "id",
                            "provides-value-for-command-input": "subject-id"
                        }
                    ],
                    "output-handlers": [
                        {
                            "name": "output-resource",
                            "accepts-command-output": "output",
                            "via-wrapup-command": None,
                            "as-a-child-of": "session",
                            "type": "Resource",
                            "label": "Derivatives",
                            "format": None
                        },
                        {
                            "name": "working-resource",
                            "accepts-command-output": "working",
                            "via-wrapup-command": None,
                            "as-a-child-of": "session",
                            "type": "Resource",
                            "label": "Work",
                            "format": None
                        }
                    ]
                }
            ]
        }
