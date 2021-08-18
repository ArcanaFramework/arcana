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
from copy import copy
from zipfile import ZipFile, BadZipfile
import shutil
from tqdm import tqdm
import xnat
from arcana2.utils import JSON_ENCODING
from arcana2.utils import makedirs
from arcana2.data import FileGroup, Field
from .base import Repository
from arcana2.exceptions import (
    ArcanaError, ArcanaNameError, ArcanaUsageError, ArcanaFileFormatError,
    ArcanaRepositoryError, ArcanaWrongRepositoryError)
from ..item import Provenance
from arcana2.utils import dir_modtime, get_class_info, parse_value
from ..dataset import Dataset
from ..frequency import Clinical



logger = logging.getLogger('arcana2')

special_char_re = re.compile(r'[^a-zA-Z_0-9]')
tag_parse_re = re.compile(r'\((\d+),(\d+)\)')

RELEVANT_DICOM_TAG_TYPES = set(('UI', 'CS', 'DA', 'TM', 'SH', 'LO',
                                'PN', 'ST', 'AS'))

def default_id_map(self, ids):
    ids = copy(ids)
    try:
        ids[Clinical.member] = ids[Clinical.subject]
    except KeyError:
        pass
    try:
        ids[Clinical.timepoint] = ids[Clinical.session]
    except KeyError:
        pass
    return ids

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
        frequency) then the filter should match all sessions in the Analysis's
        subject_ids and timepoint_ids.
    id_inference : Dict[DataFrequency, Dict[DataFrequency, str]] or Callable
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
    PROV_SUFFIX = '.__prov__.json'
    FIELD_PROV_RESOURCE = '__provenance__'
    depth = 2

    def __init__(self, server, cache_dir, user=None, password=None,
                 check_md5=True, race_cond_delay=30,
                 session_filter=None, id_inference=default_id_map):
        super().__init__(id_inference)
        if not isinstance(server, str):
            raise ArcanaUsageError(
                "Invalid server url {}".format(server))
        self._server = server
        self.cache_dir = cache_dir
        makedirs(self.cache_dir, exist_ok=True)
        self._cached_datasets = {}        
        self.user = user
        self.password = password
        self.id_inference = id_inference
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
        Caches a single file_group (if the 'cache_path' attribute is
        accessed and it has not been previously cached for example)

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
            The name_path of the primary file once it has been cached
        aux_paths : dict[str, str]
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
            if not file_group.uri:
                base_uri = self.standard_uri(xnode)
                if file_group.derived:
                    xresource = xnode.resources[self.escape_name(file_group)]
                else:
                    # If file_group is a primary 'scan' (rather than a
                    # derivative) we need to get the resource of the scan
                    # instead of the scan
                    xscan = xnode.scans[file_group.name]
                    file_group.id = xscan.id
                    base_uri += '/scans/' + xscan.id
                    xresource = xscan.resources[file_group.format_name]
                # Set URI so we can retrieve checksums if required. We ensure we
                # use the resource name instead of its ID in the URI for
                # consistency with other locations where it is set and to keep the
                # cache name_path consistent
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
                # The name_path to the directory which the files will be
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
                        # TODO: This should really take into account the
                        # size of the file being downloaded, and then the
                        # user can estimate the download speed for their
                        # repository
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
            primary_path, aux_paths = file_group.format.assort_files(
                op.join(cache_path, f) for f in os.listdir(cache_path))
        else:
            primary_path = cache_path
            aux_paths = None
        return primary_path, aux_paths

    def get_field(self, field):
        self._check_repository(field)
        with self:
            xsession = self.get_xnode(field)
            val = xsession.fields[self.escape_name(field)]
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
            if not file_group.uri:
                name = self.escape_name(file_group)
                # Set the uri of the file_group
                file_group.uri = '{}/resources/{}'.format(
                    self.standard_uri(xnode), name)
            # Copy file_group to cache
            cache_path = self.cache_path(file_group)
            if os.path.exists(cache_path):
                shutil.rmtree(cache_path)
            os.makedirs(cache_path, stat.S_IRWXU | stat.S_IRWXG)
            if file_group.format.directory:
                shutil.copytree(file_group.file_path, cache_path)
            else:
                # Copy primary file
                shutil.copyfile(file_group.file_path,
                                op.join(cache_path, file_group.fname))
                # Copy auxiliaries
                for sc_fname, sc_path in file_group.aux_file_fnames_and_paths:
                    shutil.copyfile(sc_path, op.join(cache_path, sc_fname))
            with open(cache_path + self.MD5_SUFFIX, 'w',
                      **JSON_ENCODING) as f:
                json.dump(file_group.calculate_checksums(), f, indent=2)
            if file_group.provenance:
                self.put_provenance(file_group)
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
                for dpath, _, fnames  in os.walk(file_group.file_path):
                    for fname in fnames:
                        fpath = op.join(dpath, fname)
                        frelpath = op.relpath(fpath, file_group.file_path)
                        xresource.upload(fpath, frelpath)
            else:
                xresource.upload(file_group.name_path, file_group.fname)
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
            xsession.fields[self.escape_name(field)] = val
        if field.provenance:
            self.put_provenance(field)

    def put_provenance(self, item):
        xnode = self.get_xnode(item)
        uri = '{}/resources/{}'.format(self.standard_uri(xnode),
                                       self.PROV_RESOURCE)
        cache_dir = self.cache_path(uri)
        os.makedirs(cache_dir, exist_ok=True)
        fname = self.escape_name(item) + '.json'
        if item.is_field:
            fname = self.FIELD_PROV_PREFIX + fname
        cache_path = op.join(cache_dir, fname)
        item.provenance.save(cache_path)
        # TODO: Should also save digest of prov.json to check to see if it
        #       has been altered remotely. This could be put in a field
        #       to save having to download a file
        try:
            xresource = xnode.resources[self.PROV_RESOURCE]
        except KeyError:
            xresource = self.login.classes.ResourceCatalog(
                parent=xnode, label=self.PROV_RESOURCE,
                format='PROVENANCE')
            # Until XnatPy adds a create_resource to projects, subjects &
            # sessions
            # xresource = xnode.create_resource(format_name)
        xresource.upload(cache_path, fname)

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

    def populate_tree(self, dataset: Dataset, **kwargs):
        """
        Find all file_groups, fields and provenance provenances within an XNAT
        project and create data tree within dataset

        Parameters
        ----------
        dataset : Dataset
            The dataset to construct
        """
        # Add derived timepoint IDs to list of timepoint ids to filter
        project_id = dataset.name
        # Note we prefer the use of raw REST API calls here for performance
        # reasons over using XnatPy's data structures.
        with self:
            # Get per_dataset level derivatives and fields
            project_uri = '/data/archive/projects/{}'.format(project_id)
            project_json = self.login.get_json(project_uri)['items'][0]
            # Add project and summary nodes to dataset
            self.add_fields_to_node(dataset.root_node, project_json)
            self.add_resources_to_node(dataset.root_node, project_json,
                                       project_uri)
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
                ids = self.infer_ids({Clinical.subject: subject_id,
                                    Clinical.session: session_label})
                # Add node for session
                data_node = dataset.add_node(Clinical.session, ids)
                session_uri = (
                    '/data/archive/projects/{}/subjects/{}/experiments/{}'
                    .format(project_id, subject_id, session_label))
                # Add scans, fields and resources to data node
                self.add_scans_to_node(data_node, session_json, session_uri,
                                       ids, **kwargs)
                self.add_fields_to_node(data_node, session_json, **kwargs)
                self.add_resources_to_node(data_node, session_json,
                                           session_uri, **kwargs)
            # Get subject level resources and fields
            for subject_xid in subject_xids:
                subject_id = subject_xids_to_labels[subject_xid]
                ids = self.infer_ids({Clinical.subject: subject_id})
                data_node = dataset.add_node(Clinical.subject, ids)
                subject_uri = ('/data/archive/projects/{}/subjects/{}'
                               .format(project_id, subject_id))
                subject_json = self.login.get_json(subject_uri)['items'][0]
                # Add subject level resources and fields to subject node
                self.add_fields_to_node(data_node, subject_json, **kwargs)
                self.add_resources_to_node(data_node, subject_json,
                                           subject_uri, dataset, **kwargs)

    def add_resources_to_node(self, data_node, node_json, node_uri, **kwargs):
        try:
            resources_json = next(
                c['items'] for c in node_json['children']
                if c['field'] == 'resources/resource')
        except StopIteration:
            resources_json = []
        provenance_resources = []
        for d in resources_json:
            label = d['data_fields']['label']
            resource_uri = '{}/resources/{}'.format(node_uri, label)
            name, dn = self._unescape_name_and_get_node(name, data_node)
            format_name = d['data_fields']['format']
            if name != self.PROV_RESOURCE:
                # Use the timepoint from the derived name if present
                dn.add_file_group(
                    name, resource_uris={format_name: resource_uri}, **kwargs)
            else:
                provenance_resources.append((dn, resource_uri))
        for dn, uri in provenance_resources:
            self.set_provenance(dn, uri)


    def set_provenance(self, data_node, resource_uri):
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
                        name_path = fname[:-len('.json')]
                        prov = Provenance.load(op.join(base_dir,
                                                        fname))
                        if fname.starts_with(self.FIELD_PROV_PREFIX):
                            name_path = name_path[len(self.FIELD_PROV_PREFIX):]
                            data_node.field(name_path).provenance = prov
                        else:
                            data_node.file_group(name_path).provenance = prov
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def add_fields_to_node(self, data_node, node_json, **kwargs):
        try:
            fields_json = next(
                c['items'] for c in node_json['children']
                if c['field'] == 'fields/field')
        except StopIteration:
            return []
        for js in fields_json:
            try:
                value = js['data_fields']['field']
            except KeyError:
                continue
            value = value.replace('&quot;', '"')
            name = js['data_fields']['name']
            # field_names = set([(name, None, timepoint_id, frequency)])
            # # Potentially add the field twice, once
            # # as a field name in its own right (for externally created fields)
            # # and second as a field name prefixed by an analysis name. Would
            # # ideally have the generated fields (and file_groups) in a separate
            # # assessor so there was no chance of a conflict but there should
            # # be little harm in having the field referenced twice, the only
            # # issue being with pattern matching
            # field_names.add(self.unescape_name(name, timepoint_id=timepoint_id,
            #                                         frequency=frequency))
            # for name, namespace, field_timepoint_id, field_freq in field_names:
            name, dn = self._unescape_name_and_get_node(name, data_node)
            dn.add_field(name=name, value=value **kwargs)

    def _unescape_name_and_get_node(self, name, data_node):
        name, frequency, ids = self.unescape_name(name)
        if frequency != data_node.frequency:
            try:
                data_node = data_node.dataset.node(frequency, ids)
            except ArcanaNameError:
                data_node = data_node.dataset.add_node(frequency, ids)
        return name, data_node

    def add_scans_to_node(self, data_node: Dataset, session_json: dict,
                          session_uri: str, **kwargs):
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
            data_node.add_file_group(
                name=scan_type, order=order, quality=scan_quality,
                resource_uris={
                    r: f"{session_uri}/scans/{order}/resources/{r}"
                    for r in resources}, **kwargs)
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
        with open(cache_path + self.MD5_SUFFIX, 'w', **JSON_ENCODING) as f:
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

    def get_xnode(self, data_node):
        """
        Returns the XNAT session and cache dir corresponding to the provided
        data_node

        Parameters
        ----------
        data_node : DataNode
            The data_node to get the corresponding XNAT node for
        """
        with self:
            xproject = self.login.projects[data_node.dataset.name]
            if data_node.frequency not in (Clinical.subject, Clinical.session):
                return xproject
            subj_label = data_node.ids[Clinical.subject]
            try:
                xsubject = xproject.subjects[subj_label]
            except KeyError:
                xsubject = self.login.classes.SubjectData(
                    label=subj_label, parent=xproject)
            if data_node.frequency == Clinical.subject:
                return xsubject
            sess_label = data_node.ids[Clinical.session]
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
            raise ArcanaError("URI of item needs to be set before cache path")
        return op.join(self.cache_dir, *uri.split('/')[3:])

    def _check_repository(self, item):
        if item.data_node.dataset.repository is not self:
            raise ArcanaWrongRepositoryError(
                "{} is from {} instead of {}".format(
                    item, item.dataset.repository, self))

    @classmethod
    def escape_name(cls, item):
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
        name = '__'.join(item.name_path)
        if item.data_node.frequency not in (Clinical.subject,
                                            Clinical.session):
            name = ('___'
                    + '___'.join(f'{l}__{item.data_node.ids[l]}'
                                 for l in item.data_node.frequency.layers)
                    + '___')
        return name

    @classmethod
    def unescape_name(cls, xname: str):
        """Reverses the escape of an item name by `escape_name`

        Parameters
        ----------
        xname : `str`
            An escaped name of a data node stored in the project resources

        Returns
        -------
        name : `str`
            The unescaped name of an item
        frequency : Clinical
            The frequency of the node
        ids : Dict[Clinical, str]
            A dictionary of IDs for the node
        """
        ids = {}
        id_parts = xname.split('___')[1:-1]
        freq_value = 0b0
        for part in id_parts:
            layer_freq_str, id = part.split('__')
            layer_freq = Clinical[layer_freq_str]
            ids[layer_freq] = id
            freq_value != layer_freq
        frequency = Clinical(freq_value)
        name_path = '/'.join(id_parts[-1].split('__'))
        return name_path, frequency, ids

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
        