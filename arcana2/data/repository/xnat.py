import os
import os.name_path as op
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
from ..item import Provenance
from arcana2.utils import dir_modtime, get_class_info, parse_value
from ..dataset import Dataset
from ..enum import Clinical


logger = logging.getLogger('arcana2')

special_char_re = re.compile(r'[^a-zA-Z_0-9]')
tag_parse_re = re.compile(r'\((\d+),(\d+)\)')

RELEVANT_DICOM_TAG_TYPES = set(('UI', 'CS', 'DA', 'TM', 'SH', 'LO',
                                'PN', 'ST', 'AS'))


class Xnat(Repository):
    """
    A 'Repository' class for XNAT repositories

    Parameters
    ----------
    server : str (URI)
        URI of XNAT server to connect to
    project_id : str
        The ID of the project in the XNAT repository
    cache_dir : str (name_path)
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
        relies on summary derivatives (i.e. of 'per_timepoint/subject/analysis'
        tree_level) then the filter should match all sessions in the Analysis's
        subject_ids and timepoint_ids.
    id_maps : Dict[DataFrequency, Dict[DataFrequency, str]] or Callable
        Either a dictionary of dictionaries that is used to extract IDs from
        subject and session labels. Keys of the outer dictionary correspond to
        the frequency to extract (typically group and/or subject) and the keys
        of the inner dictionary the frequency to extract from (i.e.
        subject or session). The values of the inner dictionary are regular
        expression patterns that match the ID to extract in the 'ID' regular
        expression group. Otherwise, it is a function with signature
        `f(ids)` that returns a dictionary with the mapped IDs included
    """

    type = 'xnat'

    MD5_SUFFIX = '.md5.json'
    PROV_RESOURCE = 'PROVENANCE__'
    depth = 2

    def __init__(self, server, cache_dir, user=None, password=None,
                 check_md5=True, race_cond_delay=30,
                 session_filter=None, id_maps=None):
        super().__init__(id_maps)
        if not isinstance(server, str):
            raise ArcanaUsageError(
                "Invalid server url {}".format(server))
        self._server = server
        self.cache_dir = cache_dir
        makedirs(self.cache_dir, exist_ok=True)
        self.user = user
        self.password = password
        self.id_maps = id_maps
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
    def frequency_enum(self):
        return Clinical

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

    def get_file_group(self, file_group):
        """
        Caches a single file_group (if the 'cache_name_path' attribute is accessed
        and it has not been previously cached for example)

        Parameters
        ----------
        file_group : FileGroup
            The file_group to cache
        prev_login : xnat.XNATSession
            An XNATSession object to use for the connection. A new
            one is created if one isn't provided

        Returns
        -------
        primary_name_path : str
            The name_path of the primary file once it has been cached
        aux_name_paths : dict[str, str]
            A dictionary containing a mapping of auxiliary file names to
            name_paths
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
            # cache name_path consistent
            file_group.uri = base_uri + '/resources/' + xresource.label
            cache_name_path = self.cache_name_path(file_group)
            need_to_download = True
            if op.exists(cache_name_path):
                if self.check_md5:
                    try:
                        with open(cache_name_path + self.MD5_SUFFIX, 'r') as f:
                            cached_checksums = json.load(f)
                    except IOError:
                        pass
                    else:
                        if cached_checksums == file_group.checksums:
                            need_to_download = False
                else:
                    need_to_download = False
            if need_to_download:
                # The name_path to the directory which the files will be
                # downloaded to.
                tmp_dir = cache_name_path + '.download'
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
                            tmp_dir, xresource, file_group, cache_name_path,
                            delay=self._race_cond_delay)
                    else:
                        raise
                else:
                    self.download_file_group(tmp_dir, xresource, file_group,
                                          cache_name_path)
                    shutil.rmtree(tmp_dir)
        if not file_group.format.directory:
            (primary_name_path, aux_name_paths) = file_group.format.assort_files(
                op.join(cache_name_path, f) for f in os.listdir(cache_name_path))
        else:
            primary_name_path = cache_name_path
            aux_name_paths = None
        return primary_name_path, aux_name_paths

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
            cache_name_path = self.cache_name_path(file_group)
            if os.name_path.exists(cache_name_path):
                shutil.rmtree(cache_name_path)
            os.makedirs(cache_name_path, stat.S_IRWXU | stat.S_IRWXG)
            if file_group.format.directory:
                shutil.copytree(file_group.name_path, cache_name_path)
            else:
                # Copy primary file
                shutil.copyfile(file_group.name_path,
                                op.join(cache_name_path, file_group.fname))
                # Copy auxiliaries
                for sc_fname, sc_name_path in file_group.aux_file_fnames_and_name_paths:
                    shutil.copyfile(sc_name_path, op.join(cache_name_path, sc_fname))
            with open(cache_name_path + self.MD5_SUFFIX, 'w',
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
                for dname_path, _, fnames  in os.walk(file_group.name_path):
                    for fname in fnames:
                        fname_path = op.join(dname_path, fname)
                        frelname_path = op.relname_path(fname_path, file_group.name_path)
                        xresource.upload(fname_path, frelname_path)
            else:
                xresource.upload(file_group.name_path, file_group.fname)
                for sc_fname, sc_name_path in file_group.aux_file_fnames_and_name_paths:
                    xresource.upload(sc_name_path, sc_fname)

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

    def put_provenance(self, provenance, dataset):
        xnode = self.get_xnode(provenance, dataset=dataset)
        resource_name = self.prepend_analysis(self.PROV_RESOURCE,
                                              provenance.namespace)
        uri = '{}/resources/{}'.format(self.standard_uri(xnode), resource_name)
        cache_dir = self.cache_name_path(uri)
        os.makedirs(cache_dir, exist_ok=True)
        cache_name_path = op.join(cache_dir, provenance.pipeline_name + '.json')
        provenance.save(cache_name_path)
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
        xresource.upload(cache_name_path, op.basename(cache_name_path))

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

    def construct_dataset(self, dataset, **kwargs):
        """
        Find all file_groups, fields and provenance provenances within an XNAT project

        Parameters
        ----------
        subject_ids : list(str)
            List of subject IDs with which to filter the tree with. If
            None all are returned
        timepoint_ids : list(str)
            List of timepoint IDs with which to filter the tree with. If
            None all are returned

        Returns
        -------
        file_groups : list[FileGroup]
            All the file_groups found in the repository
        fields : list[Field]
            All the fields found in the repository
        provenances : list[Provenance]
            The provenance provenances found in the repository
        """
        # Add derived timepoint IDs to list of timepoint ids to filter
        file_groups = []
        fields = []
        provenances = []
        project_id = dataset.name
        # Note we prefer the use of raw REST API calls here for performance
        # reasons over using XnatPy's data structures.
        with self:
            # Get per_dataset level derivatives and fields
            project_uri = '/data/archive/projects/{}'.format(project_id)
            project_json = self.login.get_json(project_uri)['items'][0]
            fields.extend(self.find_fields(
                project_json, dataset, tree_level='per_dataset'))
            fsets, recs = self.find_derivatives(
                project_json, project_uri, dataset, tree_level='per_dataset')
            file_groups.extend(fsets)
            provenances.extend(recs)
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
                    timepoint_id = session_label[len(subject_id) + 1:]
                else:
                    timepoint_id = session_label
                # Strip project ID from subject ID if required
                if subject_id.startswith(project_id + '_'):
                    subject_id = subject_id[len(project_id) + 1:]
                        # Extract part of JSON relating to files
                file_groups.extend(self.find_scans(
                    session_json, session_uri, subject_id, timepoint_id,
                    dataset, **kwargs))
                fields.extend(self.find_fields(
                    session_json, dataset, tree_level='per_session',
                    subject_id=subject_id, timepoint_id=timepoint_id, **kwargs))
                fsets, recs = self.find_derivatives(
                    session_json, session_uri, dataset, subject_id=subject_id,
                    timepoint_id=timepoint_id, tree_level='per_session')
                file_groups.extend(fsets)
                provenances.extend(recs)
            # Get subject level resources and fields
            for subject_xid in subject_xids:
                subject_id = subject_xids_to_labels[subject_xid]
                subject_uri = ('/data/archive/projects/{}/subjects/{}'
                               .format(project_id, subject_id))
                subject_json = self.login.get_json(subject_uri)['items'][0]
                fields.extend(self.find_fields(
                    subject_json, dataset, tree_level='per_subject',
                    subject_id=subject_id))
                fsets, recs = self.find_derivatives(
                    subject_json, subject_uri, dataset,
                    tree_level='per_subject', subject_id=subject_id)
                file_groups.extend(fsets)
                provenances.extend(recs)
        return file_groups, fields, provenances

    def find_derivatives(self, node_json, node_uri, dataset, tree_level,
                         subject_id=None, timepoint_id=None, **kwargs):
        try:
            resources_json = next(
                c['items'] for c in node_json['children']
                if c['field'] == 'resources/resource')
        except StopIteration:
            return [], []
        file_groups = []
        provenances = []
        for d in resources_json:
            label = d['data_fields']['label']
            resource_uri = '{}/resources/{}'.format(node_uri, label)
            (name, namespace,
             file_group_timepoint_id, file_group_freq) = self.split_derived_name(
                 label, timepoint_id=timepoint_id, tree_level=tree_level)
            if name != self.PROV_RESOURCE:
                # Use the timepoint from the derived name if present
                file_groups.append(FileGroup(
                    name, uri=resource_uri, dataset=dataset,
                    namespace=namespace, tree_level=file_group_freq,
                    subject_id=subject_id, timepoint_id=file_group_timepoint_id,
                    resource_name=d['data_fields']['format'], **kwargs))
            else:
                # Download provenance JSON files and parse into
                # provenances
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
                                json_name_path = op.join(base_dir, fname)
                                provenances.append(
                                    Provenance.load(
                                        pipeline_name,
                                        name_path=json_name_path,
                                        tree_level=tree_level,
                                        subject_id=subject_id,
                                        timepoint_id=timepoint_id,
                                        namespace=namespace))
                finally:
                    shutil.rmtree(temp_dir, ignore_errors=True)
        return file_groups, provenances

    def find_fields(self, node_json, dataset, tree_level, subject_id=None,
                    timepoint_id=None, **kwargs):
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
            # field_names = set([(name, None, timepoint_id, tree_level)])
            # # Potentially add the field twice, once
            # # as a field name in its own right (for externally created fields)
            # # and second as a field name prefixed by an analysis name. Would
            # # ideally have the generated fields (and file_groups) in a separate
            # # assessor so there was no chance of a conflict but there should
            # # be little harm in having the field referenced twice, the only
            # # issue being with pattern matching
            # field_names.add(self.split_derived_name(name, timepoint_id=timepoint_id,
            #                                         tree_level=tree_level))
            # for name, namespace, field_timepoint_id, field_freq in field_names:
            (name, namespace,
             field_timepoint_id, field_freq) = self.split_derived_name(
                 name, timepoint_id=timepoint_id, tree_level=tree_level)
            fields.append(Field(
                name=name,
                value=value,
                namespace=namespace,
                dataset=dataset,
                subject_id=subject_id,
                timepoint_id=field_timepoint_id,
                tree_level=field_freq,
                **kwargs))
        return fields

    def find_scans(self, session_json, session_uri, subject_id,
                   timepoint_id, dataset, **kwargs):
        try:
            scans_json = next(
                c['items'] for c in session_json['children']
                if c['field'] == 'scans/scan')
        except StopIteration:
            return []
        file_groups = []
        for scan_json in scans_json:
            order = scan_json['data_fields']['ID']
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
                    scan_type, id=order,
                    uri='{}/scans/{}/resources/{}'.format(session_uri, order,
                                                          resource),
                    dataset=dataset, subject_id=subject_id, timepoint_id=timepoint_id,
                    quality=scan_quality, resource_name=resource, **kwargs))
        logger.debug("Found node %s:%s", subject_id, timepoint_id)
        return file_groups

    def extract_subject_id(self, xsubject_label):
        """
        This assumes that the subject ID is prepended with
        the project ID.
        """
        return xsubject_label.split('_')[1]

    def extract_timepoint_id(self, xsession_label):
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

    def download_file_group(self, tmp_dir, xresource, file_group, cache_name_path):
        # Download resource to zip file
        zip_name_path = op.join(tmp_dir, 'download.zip')
        with open(zip_name_path, 'wb') as f:
            xresource.xnat_session.download_stream(
                xresource.uri + '/files', f, format='zip', verbose=True)
        checksums = self.get_checksums(file_group)
        # Extract downloaded zip file
        expanded_dir = op.join(tmp_dir, 'expanded')
        try:
            with ZipFile(zip_name_path) as zip_file:
                zip_file.extractall(expanded_dir)
        except BadZipfile as e:
            raise ArcanaError(
                "Could not unzip file '{}' ({})"
                .format(xresource.id, e))
        data_name_path = glob(expanded_dir + '/**/files', recursive=True)[0]
        # Remove existing cache if present
        try:
            shutil.rmtree(cache_name_path)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise e
        shutil.move(data_name_path, cache_name_path)
        with open(cache_name_path + XnatRepo.MD5_SUFFIX, 'w',
                  **JSON_ENCODING) as f:
            json.dump(checksums, f, indent=2)

    def _delayed_download(self, tmp_dir, xresource, file_group, cache_name_path,
                          delay):
        logger.info("Waiting %s seconds for incomplete download of '%s' "
                    "initiated another process to finish", delay, cache_name_path)
        initial_mod_time = dir_modtime(tmp_dir)
        time.sleep(delay)
        if op.exists(cache_name_path):
            logger.info("The download of '%s' has completed "
                        "successfully in the other process, continuing",
                        cache_name_path)
            return
        elif initial_mod_time != dir_modtime(tmp_dir):
            logger.info(
                "The download of '%s' hasn't completed yet, but it has"
                " been updated.  Waiting another %s seconds before "
                "checking again.", cache_name_path, delay)
            self._delayed_download(tmp_dir, xresource, file_group, cache_name_path,
                                   delay)
        else:
            logger.warning(
                "The download of '%s' hasn't updated in %s "
                "seconds, assuming that it was interrupted and "
                "restarting download", cache_name_path, delay)
            shutil.rmtree(tmp_dir)
            os.mkdir(tmp_dir)
            self.download_file_group(tmp_dir, xresource, file_group, cache_name_path)

    def get_xnode(self, item, dataset=None):
        """
        Returns the XNAT session and cache dir corresponding to the
        item.
        """
        if dataset is None:
            dataset = item.dataset
        subj_label = dataset.subject_label(item.subject_id)
        sess_label = dataset.session_label(item.subject_id, item.timepoint_id)
        with self:
            xproject = self.login.projects[dataset.name]
            if item.tree_level not in ('per_subject', 'per_session'):
                return xproject
            try:
                xsubject = xproject.subjects[subj_label]
            except KeyError:
                xsubject = self.login.classes.SubjectData(
                    label=subj_label, parent=xproject)
            if item.tree_level == 'per_subject':
                return xsubject
            elif item.tree_level != 'per_session':
                raise ArcanaUsageError(
                    "Unrecognised item tree_level '{}'".format(item.tree_level))
            try:
                xsession = xsubject.experiments[sess_label]
            except KeyError:
                xsession = self.login.classes.MrSessionData(
                    label=sess_label, parent=xsubject)
            return xsession

    def cache_name_path(self, item):
        """Path to the directory where the item is/should be cached. Note that
        the URI of the item needs to be set beforehand

        Parameters
        ----------
        item : FileGroup | `str`
            The file_group provenance that has been, or will be, cached

        Returns
        -------
        `str`
            The name_path to the directory where the item will be cached
        """
        # Append the URI after /projects as a relative name_path from the base
        # cache directory
        if not isinstance(item, str):
            uri = item.uri
        else:
            uri = item
        if uri is None:
            raise ArcanaError("URI of item needs to be set before cache name_path")
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
        item : FileGroup | Provenance
            The item to generate a derived name for

        Returns
        -------
        `str`
            The derived name
        """
        if item.derived:
            name = cls.prepend_analysis(item.name, item.namespace)
        else:
            name = item.name
        if item.tree_level == 'per_timepoint':
            name = 'VISIT_{}--{}'.format(item.timepoint_id, name)
        return name

    @classmethod
    def prepend_analysis(cls, name, namespace):
        return namespace + '-' + name

    @classmethod
    def split_derived_name(cls, name, timepoint_id=None, tree_level='per_session'):
        """Reverses the escape of an item name by `derived_name`

        Parameters
        ----------
        name : `str`
            An name escaped by `derived_name`
        timepoint_id : `str`
            The timepoint ID of the node that name is found in. Will be overridden
             if 'vis_<timepoint_id>' is found in the name
        tree_level : `str`
            The tree_level of the node the derived name is found in.

        Returns
        -------
        name : `str`
            The unescaped name of an item
        namespace : `str` | `NoneType`
            The name of the analysis the item was generated by
        timepoint_id : `str` | `NoneType`
            The timepoint ID of the derived_name, overridden from the value passed
            to the method if 'vis_<timepoint_id>' is found in the name
        tree_level : `str`
            The tree_level of the derived name, overridden from the value passed
            to the method if 'vis_<timepoint_id>' is found in the name
        """
        namespace = None
        if '-' in name:
            match = re.match(
                (r'(?:VISIT_(?P<timepoint>\w+)--)?(?:(?P<analysis>\w+)-)?'
                 + r'(?P<name>.+)'),
                name)
            name = match.group('name')
            namespace = match.group('analysis')
            if match.group('timepoint') is not None:
                if tree_level != 'per_dataset':
                    raise ArcanaRepositoryError(
                        "Visit prefixed resource ({}) found in non-project"
                        " level node".format(name))
                tree_level = 'per_timepoint'
                timepoint_id = match.group('timepoint')
        return name, namespace, timepoint_id, tree_level

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
        