import attr
import re
import json
import tempfile
import docker
from copy import copy
from dataclasses import dataclass
from pathlib import Path
from pydra import Workflow, mark
from pydra.engine.task import FunctionTask, ShellCommandTask
from pydra.engine.specs import BaseSpec, SpecInfo, ShellSpec
from arcana2.core.data.set import Dataset
from arcana2.data.types.general import directory
from arcana2.data.spaces.clinical import Clinical
from ..repositories import FileSystem
from arcana2.exceptions import ArcanaUsageError


@dataclass
class SourceMetadata():

    name: str
    version: str
    description: str
    url: str
    container: str

    def to_bids(self):
        return {
            'Name': self.name,
            'Version': self.version,
            'Description': self.description,
            'CodeURL': self.url,
            'Container': self.container}

    @classmethod
    def from_bids(cls, dct):
        return SourceMetadata(
            name=dct['Name'],
            version=dct['Version'],
            description=dct['Description'],
            url=dct['CodeURL'],
            container=dct['Container'])


@attr.s
class BidsDataset(Dataset):
    """ A representation of a "dataset", the complete collection of data
    (file-sets and fields) to be used in an analysis.

    Parameters
    ----------
    name : str
        The name/path that uniquely identifies the datset within the
        repository it is stored (e.g. FS directory path or project name)
    is_multi_session : bool
        For empty datasets, specify whether the dataset contains multiple
        timepoints or not. For non-empty datasets, this value will be ignored
        and multi-session will be determined by the presence of corresponding
        sub-directories
    column_specs : Dict[str, DataSource or DataSink]
        The sources and sinks to be initially added to the dataset (columns are
        explicitly added when workflows are applied to the dataset).
    included : Dict[DataSpace, List[str]]
        The IDs to be included in the dataset per frequency. E.g. can be
        used to limit the subject IDs in a project to the sub-set that passed
        QC. If a frequency is omitted or its value is None, then all available
        will be used
    excluded : Dict[DataSpace, List[str]]
        The IDs to be excluded in the dataset per frequency. E.g. can be
        used to exclude specific subjects that failed QC. If a frequency is
        omitted or its value is None, then all available will be used
    workflows : Dict[str, pydra.Workflow]
        Workflows that have been applied to the dataset to generate sink
    access_args: dict[str, Any]
        Repository specific args used to control the way the dataset is accessed
    """
    participants: dict[str, dict[str, str]] = attr.ib(factory=dict, repr=False)
    acknowledgements: str = attr.ib(default="Generic BIDS dataset", repr=False)
    authors: list[str] = attr.ib(default=[], repr=False)
    bids_version: str = attr.ib(default='1.0.1', repr=False)
    doi: str = attr.ib(default=None, repr=False)
    funding: list[str] = attr.ib(factory=list, repr=False)
    bids_type: str = attr.ib(default='derivative', repr=False)
    license: str = attr.ib(default='CC0', repr=False)
    references: list[str] = attr.ib(factory=list)
    how_to_acknowledge: str = attr.ib(default="see licence")
    ethics_approvals: list[str] = attr.ib(factory=list)
    generated_by: list = attr.ib(factory=list)
    sources: list[SourceMetadata] = attr.ib(factory=list)

    @classmethod
    def load(cls, name):
        if list(Path(name).glob('**/sub-*/ses-*')):
            hierarchy = [Clinical.subject, Clinical.session]
        else:
            hierarchy = [Clinical.session]    
        dataset = BidsDataset(name, repository=BidsFormat(),
                              hierarchy=hierarchy)
        dataset.load_metadata()
        return dataset

    @classmethod
    def create(cls, name, subject_ids, session_ids=None, **kwargs):
        if session_ids is not None:
            hierarchy = [Clinical.subject, Clinical.session]
        else:
            hierarchy = [Clinical.session]
        dataset = BidsDataset(
            name, repository=BidsFormat(), hierarchy=hierarchy, **kwargs)
        # Create nodes
        for subject_id in subject_ids:
            if session_ids:
                for session_id in session_ids:
                    dataset.add_leaf_node([subject_id, session_id])
            else:
                dataset.add_leaf_node([subject_id])
        dataset.save_metadata()
        return dataset

    def is_multi_session(self):
        return len(self.hierarchy) > 1

    @property
    def participant_attrs(self):
        return next(iter(self.participants.values())).keys()

    @property
    def root_dir(self):
        return Path(self.name)

    def save_metadata(self):
        if not self.participants:
            raise ArcanaUsageError(
                "Dataset needs at least one participant before the metadata "
                "can be saved")
        dct = {
            'Name': self.bids_name,
            'BIDSVersion': self.bids_version,
            'DatasetType': self.bids_type,
            'Licence': self.license,
            'Authors': self.authors,
            'Acknowledgements': self.acknowledgements,
            'HowToAcknowledge': self.how_to_acknowledge,
            'Funding': self.funding,
            'EthicsApprovals': self.ethics_approvals,
            'ReferencesAndLinks': self.references,
            'DatasetDOI': self.doi}
        if self.bids_type == 'derivative':
            dct['GeneratedBy'] = self.generated_by
            dct['sourceDatasets'] = [d.bids_dict() for d in self.sources]
        with open(self.root_dir / 'dataset_description.json', 'w') as f:
            json.dump(dct, f)

        with open(self.root_dir / 'participants.tsv', 'w') as f:
            f.write('\t'.join(self.participant_attrs) + '\n')
            for d in self.participants.values():
                f.write('\t'.join(d[c] for c in self.participants_attrs) + '\n')

    def load_metadata(self):
        if not self.root_dir.exists():
            raise ArcanaUsageError(
                f"Could not find a directory at '{self.name}' to be the "
                "root node of the dataset")

        with open(self.root_dir / 'dataset_description.json', 'w') as f:
            dct = json.load(f)               
        self.bids_name = dct['Name']
        self.bids_version = dct['BIDSVersion']
        self.bids_type = dct['DatasetType']
        self.license = dct['Licence']
        self.authors = dct['Authors']
        self.acknowledgements = dct['Acknowledgements']
        self.how_to_acknowledge = dct['HowToAcknowledge']
        self.funding = dct['Funding']
        self.ethics_approvals = dct['EthicsApprovals']
        self.references = dct['ReferencesAndLinks']
        self.doi = dct['DatasetDOI']
        if self.bids_type == 'derivative':
            self.generated_by = dct['GeneratedBy']
            self.sources = [SourceMetadata.from_dict(d)
                            for d in dct['sourceDatasets']]

        self.participants = {}
        with open(self.root_dir / 'participants.tsv') as f:
            cols = f.readline().split('\t')
            while line:= f.readline():
                d = dict(zip(cols, line.split('\t')))
                self.participants[d['participant_id']] = d


class BidsFormat(FileSystem):
    """Repository for working with data stored on the file-system in BIDS format 
    """

    def find_nodes(self, dataset: BidsDataset):
        """
        Find all nodes within the dataset stored in the repository and
        construct the data tree within the dataset

        Parameters
        ----------
        dataset : Dataset
            The dataset to construct the tree dimensions for
        """

        dataset.load_metadata()

        for subject_id, participant in dataset.participants.items():
            base_ids = {Clinical.group: participant.get('group'),
                        Clinical.subject: subject_id}
            if dataset.is_multi_session():
                for sess_id in (dataset.root_dir / subject_id).iterdir():
                    ids = copy(base_ids)
                    ids[Clinical.timepoint] = sess_id
                    ids[Clinical.session] = subject_id + '_' + sess_id
                    dataset.add_node(ids, Clinical.session)
            else:
                ids = copy(base_ids)
                ids[Clinical.session] = subject_id
                dataset.add_node(ids, Clinical.session)

    def find_items(self, data_node):
        session_path = self.node_path(data_node)
        root_dir = data_node.dataset.root_dir
        for modality_dir in (root_dir / session_path).iterdir():
            self.find_items_in_dir(modality_dir, data_node)
        for deriv_dir in (root_dir / 'derivatives').iterdir():
            self.find_items_in_dir(deriv_dir / session_path, data_node)        

    def file_group_path(self, file_group):
        fs_path = self.root_dir
        parts = file_group.path.split('/')
        if parts[0] == 'derivatives':
            if len(parts) < 2:
                raise ArcanaUsageError(
                    f"Derivative paths should have at least 3 parts ({file_group.path}")
            elif len(parts) == 2 and file_group.datatype != directory:
                raise ArcanaUsageError(
                    "Derivative paths with 2 parts must be of type directory "
                    f"({file_group.path}")
            fs_path /= parts[:2]
            parts = parts[2:]
        fs_path /= self.node_path(file_group.data_node)
        for part in parts:
            fs_path /= part
        if file_group.datatype.extension:
            fs_path = fs_path.with_suffix(file_group.datatype.extension)
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
            val = dataset.participants[data_node.ids[Clinical.subject]]
        else:
            val = super().get_field_val(field)
        return val

    @classmethod
    def wrap_app(cls,
                 name,
                 image_tag,
                 inputs: dict[str, type],
                 outputs: dict[str, type]=None,
                 frequency: Clinical=Clinical.session,
                 parameters: dict[str, str]=None,
                 container_type: str='docker') -> Workflow:
        """Creates a Pydra workflow which takes inputs and maps them to
        a BIDS dataset, executes a BIDS app and extracts outputs from
        the derivatives stored back in the BIDS dataset

        Parameters
        ----------
        image_tag : str
            Name of the BIDS app image to wrap
        inputs : dict[str, type]
            The inputs to be stored in a BIDS dataset, mapping a sanitized name
            to be added in the workflow input interface and the location within
            the BIDS app to put it
        outputs : dict[str, type]
            The outputs to be extracted from the output directory mounted to the
            BIDS app to be added in the workflow input interface and the location within
            the BIDS app to find it
        parameters : list[tuple[str, dtype]]
            The parameters of the app to be exposed to the interface
        container_type : str
            The container technology to use to run the app (either 'docker' or'singularity')
        Returns
        -------
        pydra.Workflow
            A Pydra workflow 
        """
        if parameters is None:
            parameters = {}
        if outputs is None:
            outputs = {f'derivatives/{name}': directory}
        # Ensure output paths all start with 'derivatives
        input_names = [cls.escape_name(i) for i in inputs]
        output_names = [cls.escape_name(o) for o in outputs]
        workflow = Workflow(
            name=name,
            input_spec=input_names + ['id'])

        def to_bids(frequency, id, inputs, app_name, **input_values):
            dataset = BidsDataset.create(tempfile.mkdtemp(), subject_ids=[id])
            for inpt_path, inpt_type in inputs.items():
                dataset.add_sink(cls.escape_name(inpt_path), inpt_type,
                                 path=inpt_path)
            data_node = dataset.node(frequency, id)
            with dataset.repository:
                for inpt_name, inpt_value in input_values.items():
                    node_item = data_node[inpt_name]
                    node_item.put(inpt_value) # Store value/path in repository
            derivatives_path = dataset.name / 'derivatives' / app_name
            return (dataset, dataset.name, derivatives_path)

        # Can't use a decorated function as we need to allow for dynamic
        # arguments
        workflow.add(
            FunctionTask(
                to_bids,
                input_spec=SpecInfo(
                    name='ToBidsInputs', bases=(BaseSpec,), fields=(
                        [('frequency', Clinical),
                        ('id', str),
                        ('inputs', dict[str, type]),
                        ('app_name', str)]
                        + [(i, str) for i in input_names])),
                output_spec=SpecInfo(
                    name='ToBidsOutputs', bases=(BaseSpec,), fields=[
                        ('dataset', BidsDataset),
                        ('dataset_path', Path),
                        ('derivatives_path', Path)]),
                name='to_bids',
                frequency=frequency,
                id=workflow.lzin.id,
                inputs=inputs,
                app_name=name,
                **{i: getattr(workflow.lzin, i) for i in input_names}))

        app_kwargs = copy(parameters)
        if frequency == Clinical.session:
            app_kwargs['analysis_level'] = 'participant'
            app_kwargs['participant_label'] = workflow.lzin.id
        else:
            app_kwargs['analysis_level'] = 'group'
            
        workflow.add(cls.make_app_task(
            name='bids_app',
            image_tag=image_tag,
            parameters={p: type(p) for p in parameters},
            container_type=container_type,
            out_dir=workflow.to_bids.lzout.derivatives_path,
            dataset_path=workflow.to_bids.lzout.dataset_path,
            **app_kwargs))

        @mark.task
        @mark.annotate(
            {'frequency': Clinical,
             'id': str,
             'outputs': dict[str, type],
             'return': {o: str for o in output_names}})
        def extract_bids(dataset, frequency, id, outputs):
            """Selects the items from the dataset corresponding to the input 
            sources and retrieves them from the repository to a cache on 
            the host"""
            output_paths = []
            data_node = dataset.node(frequency, id)
            for output_path, output_type in outputs.items():
                dataset.add_sink(cls.escape_name(output_path), output_type,
                                 path='derivatives/' + output_path)
            with dataset.repository:
                for output_name in outputs:
                    item = data_node[cls.escape_name(output_name)]
                    item.get()  # download to host if required
                    output_paths.append(item.value)
            return tuple(output_paths) if len(outputs) > 1 else outputs[0]
        
        workflow.add(extract_bids(
            name='extract_bids',
            dataset=workflow.to_bids.lzout.dataset,
            frequency=frequency,
            id=workflow.lzin.id,
            outputs=outputs))

        for output_name in output_names:
            workflow.set_output(
                (output_name, getattr(workflow.extract_bids.lzout, output_name)))

        return workflow

    @classmethod
    def make_app_task(cls, name,
                      image_tag: str,
                      out_dir: Path,
                      dataset_path: Path,
                      parameters: dict[str, type]=None,
                      container_type: str='docker',
                      **kwargs) -> ShellCommandTask:

        if parameters is None:
            parameters = {}

        dc = docker.from_env()

        dc.images.pull(image_tag)

        image_attrs = dc.api.inspect_image(image_tag)['Config']

        executable = image_attrs['Entrypoint']
        if executable is None:
            executable = image_attrs['Cmd']

        input_fields = [
            ("dataset_path", Path,
                {"help_string": "Path to BIDS dataset",
                 "position": 1,
                 "mandatory": True}),
            ("out_dir", Path,
                {"help_string": "Path where outputs will be written",
                  "position": 2,
                  "mandatory": True,
                  "output_file_template": "/out"}),
            ("analysis_level", str,
                {"help_string": "The analysis level the app will be run at",
                 "position": 3}),
            ("participant_label", list[str],
                {"help_string": "The IDs to include in the analysis",
                 "argstr": "--participant_label %s",
                 "position": 4})]

        for param, dtype in parameters.items():
            argstr = f'--{param}'
            if dtype is not bool:
                argstr += ' %s'
            input_fields.append((
                param, dtype, {
                    "help_string": f"Optional parameter {param}",
                    "argstr": argstr}))
        
        return ShellCommandTask(
            name=name,
            executable=executable,
            input_spec=SpecInfo(name="Input", fields=input_fields,
                                bases=(ShellSpec,)),
            container_info=(container_type, image_tag),
            out_dir=out_dir,
            dataset_path=dataset_path,
            **kwargs)

    @classmethod
    def escape_name(cls, path):
        """Escape the name of an item by replacing '/' with a valid substring

        Parameters
        ----------
        item : FileGroup | Provenance
            The item to generate a derived name for

        Returns
        -------
        `str`
            The derived name
        """
        return cls.PATH_SEP.join(str(path).split('/'))

    
    @classmethod
    def unescape_name(cls, name):
        return '/'.join(name.split(cls.PATH_SEP))

    PATH_SEP = '__l__'
