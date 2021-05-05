import os
import os.path as op
import tempfile
import stat
from glob import glob
import time
import logging
import errno
import json
import re
from zipfile import ZipFile, BadZipfile
import shutil
from tqdm import tqdm
import xnat
from arcana2.utils import JSON_ENCODING
from arcana2.utils import makedirs
from arcana2.data import FileGroup, Field
from .base import Repository
from arcana2.exceptions import (
    ArcanaError, ArcanaUsageError, ArcanaFileFormatError,
    ArcanaRepositoryError, ArcanaWrongRepositoryError)
from ..provenance import Record
from arcana2.utils import dir_modtime, get_class_info, parse_value
from ..dataset import Dataset


logger = logging.getLogger('arcana2')

special_char_re = re.compile(r'[^a-zA-Z_0-9]')
tag_parse_re = re.compile(r'\((\d+),(\d+)\)')

RELEVANT_DICOM_TAG_TYPES = set(('UI', 'CS', 'DA', 'TM', 'SH', 'LO',
                                'PN', 'ST', 'AS'))


class XnatDataset(Dataset):
    """
    A representation of an XNAT "dataset", the complete collection of data
    (file-sets and fields) to be used in an analysis.

    Parameters
    ----------
    name : str
        The name/id/path that uniquely identifies the datset within the
        repository it is stored
    repository : Repository
        The repository the dataset belongs to
    subject_label_format : str
        A string used to generate the subject label from project and
        subject IDs, e.g. "{project}_{subject}"
    session_label_format : str
        A string used to generate the session label from project and
        subject and visit IDs, e.g. "{project}_{subject}_{visit}"
    subject_ids : list[str]
        Subject IDs to be included in the analysis. All other subjects are
        ignored
    visit_ids : list[str]
        Visit IDs to be included in the analysis. All other visits are ignored
    fill_tree : bool
        Whether to fill the tree of the destination repository with the
        provided subject and/or visit IDs. Intended to be used when the
        destination repository doesn't contain any of the the input
        file_groups/fields (which are stored in external repositories) and
        so the sessions will need to be created in the destination
        repository.
    depth : int (0|1|2)
        The depth of the dataset (i.e. whether it has subjects and sessions).
            0 -> single session
            1 -> multiple subjects
            2 -> multiple subjects and visits
    subject_id_map : dict[str, str]
        Maps subject IDs in dataset to a global name-space
    visit_id_map : dict[str, str]
        Maps visit IDs in dataset to a global name-space
    """

    def __init__(self, name, repository, subject_label_format="{subject}",
                 session_label_format="{subject}_{visit}", **kwargs):
        super().__init__(name, repository, **kwargs)
        self.subject_label_format = subject_label_format
        self.session_label_format = session_label_format

    def subject_label(self, subject_id):
        return self.subject_label_format.format(
            project=self.name,
            subject=self.inv_map_subject_id(subject_id))


    def session_label(self, subject_id, visit_id):
        return self.session_label_format.format(
            project=self.name,
            subject=self.inv_map_subject_id(subject_id),
            visit=self.inv_map_visit_id(visit_id))

class XnatRepo(Repository):
    """
    A 'Repository' class for XNAT repositories

    Parameters
    ----------
    server : str (URI)
        URI of XNAT server to connect to
    project_id : str
        The ID of the project in the XNAT repository
    cache_dir : str (path)
        Path to local directory to cache remote data in
    user : str
        Username with which to connect to XNAT with
    password : str
        Password to connect to the XNAT repository with
    check_md5 : bool
        Whether to check the MD5 digest of cached files before using. This
        checks for updates on the server since the file was cached
    race_cond_delay : int
        The amount of time to wait before checking that the required
        file_group has been downloaded to cache by another process has
        completed if they are attempting to download the same file_group
    session_filter : str
        A regular expression that is used to prefilter the discovered sessions
        to avoid having to retrieve metadata for them, and potentially speeding
        up the initialisation of the Analysis. Note that if the processing
        relies on summary derivatives (i.e. of 'per_visit/subject/analysis'
        frequency) then the filter should match all sessions in the Analysis's
        subject_ids and visit_ids.
    """

    type = 'xnat'

    MD5_SUFFIX = '.md5.json'
    PROV_RESOURCE = 'PROVENANCE__'
    depth = 2

    def __init__(self, server, cache_dir, user=None,
                 password=None, check_md5=True, race_cond_delay=30,
                 session_filter=None):
        super().__init__()
        if not isinstance(server, str):
            raise ArcanaUsageError(
                "Invalid server url {}".format(server))
        self._server = server
        self.cache_dir = cache_dir
        makedirs(self.cache_dir, exist_ok=True)
        self.user = user
        self.password = password
        self._race_cond_delay = race_cond_delay
        self.check_md5 = check_md5
        self.session_filter = session_filter
        self._login = None

    def __hash__(self):
        return (hash(self.server)
                ^ hash(self.cache_dir)
                ^ hash(self._race_cond_delay)
                ^ hash(self.check_md5))

    def __repr__(self):
        return ("{}(server={}, cache_dir={})"
                .format(type(self).__name__,
                        self.server, self.cache_dir))

    def __eq__(self, other):
        try:
            return (self.server == other.server
                    and self.cache_dir == other.cache_dir
                    and self.cache_dir == other.cache_dir
                    and self._race_cond_delay == other._race_cond_delay
                    and self.check_md5 == other.check_md5)
        except AttributeError:
            return False  # For comparison with other types

    def __getstate__(self):
        dct = self.__dict__.copy()
        del dct['_login']
        del dct['_connection_depth']
        return dct

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._login = None
        self._connection_depth = 0

    @property
    def prov(self):
        return {
            'type': get_class_info(type(self)),
            'server': self.server}

    @property
    def login(self):
        if self._login is None:
            raise ArcanaError("XNAT repository has been disconnected before "
                              "exiting outer context")
        return self._login

    @property
    def server(self):
        return self._server

    def dataset_cache_dir(self, dataset_name):
        return op.join(self.cache_dir, dataset_name)

    def connect(self):
        """
        Parameters
        ----------
        prev_login : xnat.XNATSession
            An XNAT login that has been opened in the code that calls
            the method that calls login. It is wrapped in a
            NoExitWrapper so the returned connection can be used
            in a "with" statement in the method.
        """
        sess_kwargs = {}
        if self.user is not None:
            sess_kwargs['user'] = self.user
        if self.password is not None:
            sess_kwargs['password'] = self.password
        self._login = xnat.connect(server=self._server, **sess_kwargs)

    def disconnect(self):
        self.login.disconnect()
        self._login = None

    def dataset(self, name, **kwargs):
        """
        Returns a dataset from the XNAT repository

        Parameters
        ----------
        name : str
            The name, path or ID of the dataset within the repository
        subject_label_format : str
            A string used to generate the subject label from project and
            subject IDs, e.g. "{project}_{subject}"
        session_label_format : str
            A string used to generate the session label from project and
            subject and visit IDs, e.g. "{project}_{subject}_{visit}"
        subject_ids : list[str]
            Subject IDs to be included in the analysis. All other subjects are
            ignored
        visit_ids : list[str]
            Visit IDs to be included in the analysis. All other visits are ignored
        fill_tree : bool
            Whether to fill the tree of the destination repository with the
            provided subject and/or visit IDs. Intended to be used when the
            destination repository doesn't contain any of the the input
            file_groups/fields (which are stored in external repositories) and
            so the sessions will need to be created in the destination
            repository.
        subject_id_map : dict[str, str]
            Maps subject IDs in dataset to a global name-space
        visit_id_map : dict[str, str]
            Maps visit IDs in dataset to a global name-space
        """
        return XnatDataset(name, repository=self, depth=2, **kwargs)

    def get_file_group(self, file_group):
        """
        Caches a single file_group (if the 'path' attribute is accessed
        and it has not been previously cached for example

        Parameters
        ----------
        file_group : FileGroup
            The file_group to cache
        prev_login : xnat.XNATSession
            An XNATSession object to use for the connection. A new
            one is created if one isn't provided

        Returns
        -------
        primary_path : str
            The path of the primary file once it has been cached
        aux_paths : dict[str, str]
            A dictionary containing a mapping of auxiliary file names to
            paths
        """
        if file_group.format is None:
            raise ArcanaUsageError(
                "Attempting to download {}, which has not been assigned a "
                "file format (see FileGroup.formatted)".format(file_group))
        self._check_repository(file_group)
        with self:  # Connect to the XNAT repository if haven't already
            xnode = self.get_xnode(file_group)
            base_uri = self.standard_uri(xnode)
            if file_group.derived:
                xresource = xnode.resources[self.derived_name(file_group)]
            else:
                # If file_group is a primary 'scan' (rather than a derivative)
                # we need to get the resource of the scan instead of
                # the session
                xscan = xnode.scans[file_group.name]
                file_group.id = xscan.id
                base_uri += '/scans/' + xscan.id
                xresource = xscan.resources[file_group.resource_name]
            # Set URI so we can retrieve checksums if required. We ensure we
            # use the resource name instead of its ID in the URI for
            # consistency with other locations where it is set and to keep the
            # cache path consistent
            file_group.uri = base_uri + '/resources/' + xresource.label
            cache_path = self.cache_path(file_group)
            need_to_download = True
            if op.exists(cache_path):
                if self.check_md5:
                    try:
                        with open(cache_path + self.MD5_SUFFIX, 'r') as f:
                            cached_checksums = json.load(f)
                    except IOError:
                        pass
                    else:
                        if cached_checksums == file_group.checksums:
                            need_to_download = False
                else:
                    need_to_download = False
            if need_to_download:
                # The path to the directory which the files will be
                # downloaded to.
                tmp_dir = cache_path + '.download'
                try:
                    # Attempt to make tmp download directory. This will
                    # fail if another process (or previous attempt) has
                    # already created it. In that case this process will
                    # wait to see if that download finishes successfully,
                    # and if so use the cached version.
                    os.makedirs(tmp_dir)
                except OSError as e:
                    if e.errno == errno.EEXIST:
                        # Another process may be concurrently downloading
                        # the same file to the cache. Wait for
                        # 'race_cond_delay' seconds and then check that it
                        # has been completed or assume interrupted and
                        # redownload.
                        self._delayed_download(
                            tmp_dir, xresource, file_group, cache_path,
                            delay=self._race_cond_delay)
                    else:
                        raise
                else:
                    self.download_file_group(tmp_dir, xresource, file_group,
                                          cache_path)
                    shutil.rmtree(tmp_dir)
        if not file_group.format.directory:
            (primary_path, aux_paths) = file_group.format.assort_files(
                op.join(cache_path, f) for f in os.listdir(cache_path))
        else:
            primary_path = cache_path
            aux_paths = None
        return primary_path, aux_paths

    def get_field(self, field):
        self._check_repository(field)
        with self:
            xsession = self.get_xnode(field)
            val = xsession.fields[self.derived_name(field)]
            val = val.replace('&quot;', '"')
            val = parse_value(val)
        return val

    def put_file_group(self, file_group):
        if file_group.format is None:
            raise ArcanaFileFormatError(
                "Format of {} needs to be set before it is uploaded to {}"
                .format(file_group, self))
        self._check_repository(file_group)
        # Open XNAT session
        with self:
            # Add session for derived scans if not present
            xnode = self.get_xnode(file_group)
            name = self.derived_name(file_group)
            # Set the uri of the file_group
            file_group.uri = '{}/resources/{}'.format(self.standard_uri(xnode),
                                                   name)
            # Copy file_group to cache
            cache_path = self.cache_path(file_group)
            if os.path.exists(cache_path):
                shutil.rmtree(cache_path)
            os.makedirs(cache_path, stat.S_IRWXU | stat.S_IRWXG)
            if file_group.format.directory:
                shutil.copytree(file_group.path, cache_path)
            else:
                # Copy primary file
                shutil.copyfile(file_group.path,
                                op.join(cache_path, file_group.fname))
                # Copy auxiliaries
                for sc_fname, sc_path in file_group.aux_file_fnames_and_paths:
                    shutil.copyfile(sc_path, op.join(cache_path, sc_fname))
            with open(cache_path + self.MD5_SUFFIX, 'w',
                      **JSON_ENCODING) as f:
                json.dump(file_group.calculate_checksums(), f, indent=2)
            # Delete existing resource (if present)
            try:
                xresource = xnode.resources[name]
            except KeyError:
                pass
            else:
                # Delete existing resource. We could possibly just use the
                # 'overwrite' option of upload but this would leave files in
                # the previous file_group that aren't in the current
                xresource.delete()
            # Create the new resource for the file_group
            xresource = self.login.classes.ResourceCatalog(
                parent=xnode, label=name, format=file_group.format_name)
            # Upload the files to the new resource                
            if file_group.format.directory:
                for dpath, _, fnames  in os.walk(file_group.path):
                    for fname in fnames:
                        fpath = op.join(dpath, fname)
                        frelpath = op.relpath(fpath, file_group.path)
                        xresource.upload(fpath, frelpath)
            else:
                xresource.upload(file_group.path, file_group.fname)
                for sc_fname, sc_path in file_group.aux_file_fnames_and_paths:
                    xresource.upload(sc_path, sc_fname)

    def put_field(self, field):
        self._check_repository(field)
        val = field.value
        if field.array:
            if field.dtype is str:
                val = ['"{}"'.format(v) for v in val]
            val = '[' + ','.join(str(v) for v in val) + ']'
        if field.dtype is str:
            val = '"{}"'.format(val)
        with self:
            xsession = self.get_xnode(field)
            xsession.fields[self.derived_name(field)] = val

    def put_record(self, record, dataset):
        xnode = self.get_xnode(record, dataset=dataset)
        resource_name = self.prepend_analysis(self.PROV_RESOURCE,
                                              record.from_analysis)
        uri = '{}/resources/{}'.format(self.standard_uri(xnode), resource_name)
        cache_dir = self.cache_path(uri)
        os.makedirs(cache_dir, exist_ok=True)
        cache_path = op.join(cache_dir, record.pipeline_name + '.json')
        record.save(cache_path)
        # TODO: Should also save digest of prov.json to check to see if it
        #       has been altered remotely
        try:
            xresource = xnode.resources[resource_name]
        except KeyError:
            xresource = self.login.classes.ResourceCatalog(
                parent=xnode, label=resource_name,
                format='PROVENANCE')
            # Until XnatPy adds a create_resource to projects, subjects &
            # sessions
            # xresource = xnode.create_resource(resource_name)
        xresource.upload(cache_path, op.basename(cache_path))

    def get_checksums(self, file_group):
        """
        Downloads the MD5 digests associated with the files in the file-set.
        These are saved with the downloaded files in the cache and used to
        check if the files have been updated on the server

        Parameters
        ----------
        resource : xnat.ResourceCatalog
            The xnat resource
        file_format : FileFormat
            The format of the file_group to get the checksums for. Used to
            determine the primary file within the resource and change the
            corresponding key in the checksums dictionary to '.' to match
            the way it is generated locally by Arcana.
        """
        if file_group.uri is None:
            raise ArcanaUsageError(
                "Can't retrieve checksums as URI has not been set for {}"
                .format(file_group))
        with self:
            checksums = {r['Name']: r['digest']
                         for r in self.login.get_json(file_group.uri + '/files')[
                             'ResultSet']['Result']}
        if not file_group.format.directory:
            # Replace the key corresponding to the primary file with '.' to
            # match the way that checksums are created by Arcana
            primary = file_group.format.assort_files(checksums.keys())[0]
            checksums['.'] = checksums.pop(primary)
        return checksums

    def find_data(self, dataset, subject_ids=None, visit_ids=None, **kwargs):
        """
        Find all file_groups, fields and provenance records within an XNAT project

        Parameters
        ----------
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
        # Add derived visit IDs to list of visit ids to filter
        file_groups = []
        fields = []
        records = []
        project_id = dataset.name
        # Note we prefer the use of raw REST API calls here for performance
        # reasons over using XnatPy's data structures.
        with self:
            # Get per_dataset level derivatives and fields
            project_uri = '/data/archive/projects/{}'.format(project_id)
            project_json = self.login.get_json(project_uri)['items'][0]
            fields.extend(self.find_fields(
                project_json, dataset, frequency='per_dataset'))
            fsets, recs = self.find_derivatives(
                project_json, project_uri, dataset, frequency='per_dataset')
            file_groups.extend(fsets)
            records.extend(recs)
            # Get map of internal subject IDs to subject labels in project
            subject_xids_to_labels = {
                s['ID']: s['label'] for s in self.login.get_json(
                    '/data/projects/{}/subjects'.format(project_id))[
                        'ResultSet']['Result']}
            # Get list of all sessions within project
            session_xids = [
                s['ID'] for s in self.login.get_json(
                    '/data/projects/{}/experiments'.format(project_id))[
                        'ResultSet']['Result']
                if (self.session_filter is None
                    or re.match(self.session_filter, s['label']))]
            subject_xids = set()
            for session_xid in tqdm(session_xids,
                                    "Scanning sessions in '{}' project"
                                    .format(project_id)):
                session_json = self.login.get_json(
                    '/data/projects/{}/experiments/{}'.format(
                        project_id, session_xid))['items'][0]
                subject_xid = session_json['data_fields']['subject_ID']
                subject_xids.add(subject_xid)
                subject_id = subject_xids_to_labels[subject_xid]
                session_label = session_json['data_fields']['label']
                session_uri = (
                    '/data/archive/projects/{}/subjects/{}/experiments/{}'
                    .format(project_id, subject_id, session_label))
                # Extract analysis name and derived-from session
                # Strip subject ID from session label if required
                if session_label.startswith(subject_id + '_'):
                    visit_id = session_label[len(subject_id) + 1:]
                else:
                    visit_id = session_label
                # Strip project ID from subject ID if required
                if subject_id.startswith(project_id + '_'):
                    subject_id = subject_id[len(project_id) + 1:]
                        # Extract part of JSON relating to files
                file_groups.extend(self.find_scans(
                    session_json, session_uri, subject_id, visit_id,
                    dataset, **kwargs))
                fields.extend(self.find_fields(
                    session_json, dataset, frequency='per_session',
                    subject_id=subject_id, visit_id=visit_id, **kwargs))
                fsets, recs = self.find_derivatives(
                    session_json, session_uri, dataset, subject_id=subject_id,
                    visit_id=visit_id, frequency='per_session')
                file_groups.extend(fsets)
                records.extend(recs)
            # Get subject level resources and fields
            for subject_xid in subject_xids:
                subject_id = subject_xids_to_labels[subject_xid]
                subject_uri = ('/data/archive/projects/{}/subjects/{}'
                               .format(project_id, subject_id))
                subject_json = self.login.get_json(subject_uri)['items'][0]
                fields.extend(self.find_fields(
                    subject_json, dataset, frequency='per_subject',
                    subject_id=subject_id))
                fsets, recs = self.find_derivatives(
                    subject_json, subject_uri, dataset,
                    frequency='per_subject', subject_id=subject_id)
                file_groups.extend(fsets)
                records.extend(recs)
        return file_groups, fields, records

    def find_derivatives(self, node_json, node_uri, dataset, frequency,
                         subject_id=None, visit_id=None, **kwargs):
        try:
            resources_json = next(
                c['items'] for c in node_json['children']
                if c['field'] == 'resources/resource')
        except StopIteration:
            return [], []
        file_groups = []
        records = []
        for d in resources_json:
            label = d['data_fields']['label']
            resource_uri = '{}/resources/{}'.format(node_uri, label)
            (name, from_analysis,
             file_group_visit_id, file_group_freq) = self.split_derived_name(
                 label, visit_id=visit_id, frequency=frequency)
            if name != self.PROV_RESOURCE:
                # Use the visit from the derived name if present
                file_groups.append(FileGroup(
                    name, uri=resource_uri, dataset=dataset,
                    from_analysis=from_analysis, frequency=file_group_freq,
                    subject_id=subject_id, visit_id=file_group_visit_id,
                    resource_name=d['data_fields']['format'], **kwargs))
            else:
                # Download provenance JSON files and parse into
                # records
                temp_dir = tempfile.mkdtemp()
                try:
                    with tempfile.TemporaryFile() as temp_zip:
                        self.login.download_stream(
                            resource_uri + '/files', temp_zip, format='zip')
                        with ZipFile(temp_zip) as zip_file:
                            zip_file.extractall(temp_dir)
                    for base_dir, _, fnames in os.walk(temp_dir):
                        for fname in fnames:
                            if fname.endswith('.json'):
                                pipeline_name = fname[:-len('.json')]
                                json_path = op.join(base_dir, fname)
                                records.append(
                                    Record.load(
                                        pipeline_name,
                                        path=json_path,
                                        frequency=frequency,
                                        subject_id=subject_id,
                                        visit_id=visit_id,
                                        from_analysis=from_analysis))
                finally:
                    shutil.rmtree(temp_dir, ignore_errors=True)
        return file_groups, records

    def find_fields(self, node_json, dataset, frequency, subject_id=None,
                    visit_id=None, **kwargs):
        try:
            fields_json = next(
                c['items'] for c in node_json['children']
                if c['field'] == 'fields/field')
        except StopIteration:
            return []
        fields = []
        for js in fields_json:
            try:
                value = js['data_fields']['field']
            except KeyError:
                continue
            value = value.replace('&quot;', '"')
            name = js['data_fields']['name']
            # field_names = set([(name, None, visit_id, frequency)])
            # # Potentially add the field twice, once
            # # as a field name in its own right (for externally created fields)
            # # and second as a field name prefixed by an analysis name. Would
            # # ideally have the generated fields (and file_groups) in a separate
            # # assessor so there was no chance of a conflict but there should
            # # be little harm in having the field referenced twice, the only
            # # issue being with pattern matching
            # field_names.add(self.split_derived_name(name, visit_id=visit_id,
            #                                         frequency=frequency))
            # for name, from_analysis, field_visit_id, field_freq in field_names:
            (name, from_analysis,
             field_visit_id, field_freq) = self.split_derived_name(
                 name, visit_id=visit_id, frequency=frequency)
            fields.append(Field(
                name=name,
                value=value,
                from_analysis=from_analysis,
                dataset=dataset,
                subject_id=subject_id,
                visit_id=field_visit_id,
                frequency=field_freq,
                **kwargs))
        return fields

    def find_scans(self, session_json, session_uri, subject_id,
                   visit_id, dataset, **kwargs):
        try:
            scans_json = next(
                c['items'] for c in session_json['children']
                if c['field'] == 'scans/scan')
        except StopIteration:
            return []
        file_groups = []
        for scan_json in scans_json:
            scan_id = scan_json['data_fields']['ID']
            scan_type = scan_json['data_fields'].get('type', '')
            scan_quality = scan_json['data_fields'].get('quality', None)
            try:
                resources_json = next(
                    c['items'] for c in scan_json['children']
                    if c['field'] == 'file')
            except StopIteration:
                resources = set()
            else:
                resources = set(js['data_fields']['label']
                                for js in resources_json)
            # Remove auto-generated snapshots directory
            resources.discard('SNAPSHOTS')
            for resource in resources:
                file_groups.append(FileGroup(
                    scan_type, id=scan_id,
                    uri='{}/scans/{}/resources/{}'.format(session_uri, scan_id,
                                                          resource),
                    dataset=dataset, subject_id=subject_id, visit_id=visit_id,
                    quality=scan_quality, resource_name=resource, **kwargs))
        logger.debug("Found node %s:%s", subject_id, visit_id)
        return file_groups

    def extract_subject_id(self, xsubject_label):
        """
        This assumes that the subject ID is prepended with
        the project ID.
        """
        return xsubject_label.split('_')[1]

    def extract_visit_id(self, xsession_label):
        """
        This assumes that the session ID is preprended
        """
        return '_'.join(xsession_label.split('_')[2:])

    def dicom_header(self, file_group):
        def convert(val, code):
            if code == 'TM':
                try:
                    val = float(val)
                except ValueError:
                    pass
            elif code == 'CS':
                val = val.split('\\')
            return val
        with self:
            scan_uri = '/' + '/'.join(file_group.uri.split('/')[2:-2])
            response = self.login.get(
                '/REST/services/dicomdump?src='
                + scan_uri).json()['ResultSet']['Result']
        hdr = {tag_parse_re.match(t['tag1']).groups(): convert(t['value'],
                                                               t['vr'])
               for t in response if (tag_parse_re.match(t['tag1'])
                                     and t['vr'] in RELEVANT_DICOM_TAG_TYPES)}
        return hdr

    def download_file_group(self, tmp_dir, xresource, file_group, cache_path):
        # Download resource to zip file
        zip_path = op.join(tmp_dir, 'download.zip')
        with open(zip_path, 'wb') as f:
            xresource.xnat_session.download_stream(
                xresource.uri + '/files', f, format='zip', verbose=True)
        checksums = self.get_checksums(file_group)
        # Extract downloaded zip file
        expanded_dir = op.join(tmp_dir, 'expanded')
        try:
            with ZipFile(zip_path) as zip_file:
                zip_file.extractall(expanded_dir)
        except BadZipfile as e:
            raise ArcanaError(
                "Could not unzip file '{}' ({})"
                .format(xresource.id, e))
        data_path = glob(expanded_dir + '/**/files', recursive=True)[0]
        # Remove existing cache if present
        try:
            shutil.rmtree(cache_path)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise e
        shutil.move(data_path, cache_path)
        with open(cache_path + XnatRepo.MD5_SUFFIX, 'w',
                  **JSON_ENCODING) as f:
            json.dump(checksums, f, indent=2)

    def _delayed_download(self, tmp_dir, xresource, file_group, cache_path,
                          delay):
        logger.info("Waiting %s seconds for incomplete download of '%s' "
                    "initiated another process to finish", delay, cache_path)
        initial_mod_time = dir_modtime(tmp_dir)
        time.sleep(delay)
        if op.exists(cache_path):
            logger.info("The download of '%s' has completed "
                        "successfully in the other process, continuing",
                        cache_path)
            return
        elif initial_mod_time != dir_modtime(tmp_dir):
            logger.info(
                "The download of '%s' hasn't completed yet, but it has"
                " been updated.  Waiting another %s seconds before "
                "checking again.", cache_path, delay)
            self._delayed_download(tmp_dir, xresource, file_group, cache_path,
                                   delay)
        else:
            logger.warning(
                "The download of '%s' hasn't updated in %s "
                "seconds, assuming that it was interrupted and "
                "restarting download", cache_path, delay)
            shutil.rmtree(tmp_dir)
            os.mkdir(tmp_dir)
            self.download_file_group(tmp_dir, xresource, file_group, cache_path)

    def get_xnode(self, item, dataset=None):
        """
        Returns the XNAT session and cache dir corresponding to the
        item.
        """
        if dataset is None:
            dataset = item.dataset
        subj_label = dataset.subject_label(item.subject_id)
        sess_label = dataset.session_label(item.subject_id, item.visit_id)
        with self:
            xproject = self.login.projects[dataset.name]
            if item.frequency not in ('per_subject', 'per_session'):
                return xproject
            try:
                xsubject = xproject.subjects[subj_label]
            except KeyError:
                xsubject = self.login.classes.SubjectData(
                    label=subj_label, parent=xproject)
            if item.frequency == 'per_subject':
                return xsubject
            elif item.frequency != 'per_session':
                raise ArcanaUsageError(
                    "Unrecognised item frequency '{}'".format(item.frequency))
            try:
                xsession = xsubject.experiments[sess_label]
            except KeyError:
                xsession = self.login.classes.MrSessionData(
                    label=sess_label, parent=xsubject)
            return xsession

    def cache_path(self, item):
        """Path to the directory where the item is/should be cached. Note that
        the URI of the item needs to be set beforehand

        Parameters
        ----------
        item : FileGroup | `str`
            The file_group record that has been, or will be, cached

        Returns
        -------
        `str`
            The path to the directory where the item will be cached
        """
        # Append the URI after /projects as a relative path from the base
        # cache directory
        if not isinstance(item, str):
            uri = item.uri
        else:
            uri = item
        if uri is None:
            raise ArcanaError("URI of item needs to be set before cache path")
        return op.join(self.cache_dir, *uri.split('/')[3:])

    def _check_repository(self, item):
        if item.dataset.repository is not self:
            raise ArcanaWrongRepositoryError(
                "{} is from {} instead of {}".format(
                    item, item.dataset.repository, self))

    @classmethod
    def derived_name(cls, item):
        """Escape the name of an item by prefixing the name of the current
        analysis

        Parameters
        ----------
        item : FileGroup | Record
            The item to generate a derived name for

        Returns
        -------
        `str`
            The derived name
        """
        if item.derived:
            name = cls.prepend_analysis(item.name, item.from_analysis)
        else:
            name = item.name
        if item.frequency == 'per_visit':
            name = 'VISIT_{}--{}'.format(item.visit_id, name)
        return name

    @classmethod
    def prepend_analysis(cls, name, from_analysis):
        return from_analysis + '-' + name

    @classmethod
    def split_derived_name(cls, name, visit_id=None, frequency='per_session'):
        """Reverses the escape of an item name by `derived_name`

        Parameters
        ----------
        name : `str`
            An name escaped by `derived_name`
        visit_id : `str`
            The visit ID of the node that name is found in. Will be overridden
             if 'vis_<visit_id>' is found in the name
        frequency : `str`
            The frequency of the node the derived name is found in.

        Returns
        -------
        name : `str`
            The unescaped name of an item
        from_analysis : `str` | `NoneType`
            The name of the analysis the item was generated by
        visit_id : `str` | `NoneType`
            The visit ID of the derived_name, overridden from the value passed
            to the method if 'vis_<visit_id>' is found in the name
        frequency : `str`
            The frequency of the derived name, overridden from the value passed
            to the method if 'vis_<visit_id>' is found in the name
        """
        from_analysis = None
        if '-' in name:
            match = re.match(
                (r'(?:VISIT_(?P<visit>\w+)--)?(?:(?P<analysis>\w+)-)?'
                 + r'(?P<name>.+)'),
                name)
            name = match.group('name')
            from_analysis = match.group('analysis')
            if match.group('visit') is not None:
                if frequency != 'per_dataset':
                    raise ArcanaRepositoryError(
                        "Visit prefixed resource ({}) found in non-project"
                        " level node".format(name))
                frequency = 'per_visit'
                visit_id = match.group('visit')
        return name, from_analysis, visit_id, frequency

    @classmethod
    def standard_uri(cls, xnode):
        """Get the URI of the XNAT node (ImageSession | Subject | Project)
        using labels rather than IDs for subject and sessions, e.g

        >>> xnode = repo.login.experiments['MRH017_100_MR01']
        >>> repo.standard_uri(xnode)

        '/data/archive/projects/MRH017/subjects/MRH017_100/experiments/MRH017_100_MR01'

        Parameters
        ----------
        xnode : xnat.ImageSession | xnat.Subject | xnat.Project
            A node of the XNAT data tree
        """
        uri = xnode.uri
        if 'experiments' in uri:
            # Replace ImageSession ID with label in URI.
            uri = re.sub(r'(?<=/experiments/)[^/]+', xnode.label, uri)
        if 'subjects' in uri:
            try:
                # If xnode is a ImageSession
                subject_id = xnode.subject.label
            except AttributeError:
                # If xnode is a Subject
                subject_id = xnode.label
            except KeyError:
                # There is a bug where the subject isn't appeared to be cached
                # so we use this as a workaround
                subject_json = xnode.xnat_session.get_json(
                    xnode.uri.split('/experiments')[0])
                subject_id = subject_json['items'][0]['data_fields']['label']
            # Replace subject ID with subject label in URI
            uri = re.sub(r'(?<=/subjects/)[^/]+', subject_id, uri)

        return uri
        