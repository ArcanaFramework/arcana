import os
import os.path as op
import stat
from pathlib import Path
import typing as ty
from glob import glob
import time
import logging
import errno
import json
import re
from zipfile import ZipFile, BadZipfile
import shutil
import attr
from tqdm import tqdm
import xnat
from arcana2.core.utils import JSON_ENCODING
from arcana2.core.repository import Repository
from arcana2.exceptions import (
    ArcanaError, ArcanaCacheError, ArcanaUsageError, ArcanaFileFormatError,
    ArcanaWrongRepositoryError)
from arcana2.core.data.provenance import DataProvenance
from arcana2.core.utils import dir_modtime, get_class_info, parse_value
from arcana2.core.data.set import Dataset
from arcana2.dataspaces.clinical import Clinical


logger = logging.getLogger('arcana2')

special_char_re = re.compile(r'[^a-zA-Z_0-9]')
tag_parse_re = re.compile(r'\((\d+),(\d+)\)')

RELEVANT_DICOM_TAG_TYPES = set(('UI', 'CS', 'DA', 'TM', 'SH', 'LO',
                                'PN', 'ST', 'AS'))

COMMAND_INPUT_TYPES = {
    bool: 'bool',
    str: 'string',
    int: 'number',
    float: 'number'}

@attr.s
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
    """

    server: str = attr.ib()
    cache_dir: str = attr.ib(converter=Path)
    user: str = attr.ib(default=None)
    password: str = attr.ib(default=None)
    check_md5: bool = attr.ib(default=True)
    race_condition_delay: int = attr.ib(default=30)
    _cached_datasets: ty.Dict[str, Dataset]= attr.ib(factory=dict, init=False)
    _login = attr.ib(default=None, init=False)

    type = 'xnat'
    MD5_SUFFIX = '.md5.json'
    PROV_SUFFIX = '.__prov__.json'
    FIELD_PROV_RESOURCE = '__provenance__'
    depth = 2
    DEFAULT_HIERARCHY = [Clinical.subject, Clinical.session]
    PATH_SEP = '_ll_'

    @property
    def prov(self):
        return {
            'type': get_class_info(type(self)),
            'server': self.server}

    @cache_dir.validator
    def cache_dir_validator(self, _, cache_dir):
        if not cache_dir.exists():
            raise ValueError(
                f"Cache dir, '{cache_dir}' does not exist")

    @property
    def login(self):
        if self._login is None:
            raise ArcanaError("XNAT repository has been disconnected before "
                              "exiting outer context")
        return self._login

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
        self._login = xnat.connect(server=self.server, **sess_kwargs)

    def disconnect(self):
        self.login.disconnect()
        self._login = None

    def get_file_group_paths(self, file_group):
        """
        Caches a file_group to the local file system and returns the path to
        the cached files

        Parameters
        ----------
        file_group : FileGroup
            The file_group to cache

        Returns
        -------
        primary_path : str
            The name_path of the primary file once it has been cached
        side_cars : dict[str, str]
            A dictionary containing a mapping of auxiliary file names to
            name_paths
        """
        if file_group.datatype is None:
            raise ArcanaUsageError(
                "Attempting to download {}, which has not been assigned a "
                "file format (see FileGroup.datatypeted)".format(file_group))
        self._check_repository(file_group)
        with self:  # Connect to the XNAT repository if haven't already
            xnode = self.get_xnode(file_group.data_node)
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
                    xresource = xscan.resources[file_group.datatype_name]
                # Set URI so we can retrieve checksums if required. We ensure we
                # use the resource name instead of its ID in the URI for
                # consistency with other locations where it is set and to keep the
                # cache name_path consistent
                file_group.uri = base_uri + '/resources/' + xresource.label
            cache_path = self.cache_path(file_group)
            need_to_download = True
            if op.exists(cache_path):
                if self.check_md5:
                    md5_path = append_suffix(cache_path, self.MD5_SUFFIX)
                    if md5_path.exists():
                        with open(md5_path, 'r') as f:
                            cached_checksums = json.load(f)
                    if cached_checksums == file_group.checksums:
                        need_to_download = False
                else:
                    need_to_download = False
            if need_to_download:
                # The name_path to the directory which the files will be
                # downloaded to.
                tmp_dir = append_suffix(cache_path, '.download')
                xresource = self.login.classes.Resource(uri=file_group.uri,
                                                        xnat_session=self.login)
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
        return self._file_group_paths(file_group)

    def get_field_value(self, field):
        """
        Retrieves a fields value

        Parameters
        ----------
        field : Field
            The field to retrieve

        Returns
        -------
        value : float or int or str of list[float] or list[int] or list[str]
            The value of the field
        """
        self._check_repository(field)
        with self:
            xsession = self.get_xnode(field.data_node)
            val = xsession.fields[self.escape_name(field)]
            val = val.replace('&quot;', '"')
            val = parse_value(val)
        return val

    def put_file_group(self, file_group, fs_path, side_cars):
        """
        Retrieves a fields value

        Parameters
        ----------
        field : Field
            The field to retrieve

        Returns
        -------
        value : float or int or str of list[float] or list[int] or list[str]
            The value of the field
        """
        if file_group.datatype is None:
            raise ArcanaFileFormatError(
                "Format of {} needs to be set before it is uploaded to {}"
                .format(file_group, self))
        self._check_repository(file_group)
        # Open XNAT session
        with self:
            # Add session for derived scans if not present
            xnode = self.get_xnode(file_group.data_node)
            if not file_group.uri:
                escaped_name = self.escape_name(file_group)
                # Set the uri of the file_group
                file_group.uri = '{}/resources/{}'.format(
                    self.standard_uri(xnode), escaped_name)
            # Delete existing resource (if present)
            try:
                xresource = xnode.resources[escaped_name]
            except KeyError:
                pass
            else:
                # Delete existing resource. We could possibly just use the
                # 'overwrite' option of upload but this would leave files in
                # the previous file_group that aren't in the current
                xresource.delete()
            # Create the new resource for the file_group
            xresource = self.login.classes.ResourceCatalog(
                parent=xnode, label=escaped_name,
                format=file_group.datatype.name)
            # Create cache path
            cache_path = self.cache_path(file_group)
            if cache_path.exists():
                shutil.rmtree(cache_path)
            # Upload data and add it to cache
            if file_group.datatype.directory:
                for dpath, _, fnames  in os.walk(fs_path):
                    dpath = Path(dpath)
                    for fname in fnames:
                        fpath = dpath / fname
                        frelpath = fpath.relative_to(fs_path)
                        xresource.upload(str(fpath), str(frelpath))
                shutil.copytree(fs_path, cache_path)
            else:
                # Upload primary file and add to cache
                fname = escaped_name + file_group.datatype.extension
                xresource.upload(str(fs_path), fname)
                os.makedirs(cache_path, stat.S_IRWXU | stat.S_IRWXG)
                shutil.copyfile(fs_path, cache_path / fname)
                # Upload side cars and add them to cache
                for sc_name, sc_src_path in side_cars.items():
                    sc_fname = escaped_name + file_group.datatype.side_cars[sc_name]
                    xresource.upload(str(sc_src_path), sc_fname)
                    shutil.copyfile(sc_src_path, cache_path / sc_fname)
            # need to manually set this here in order to calculate the
            # checksums (instead of waiting until after the 'put' is finished)
            file_group._set_fs_paths(*self._file_group_paths(file_group))
            with open(append_suffix(cache_path, self.MD5_SUFFIX), 'w',
                      **JSON_ENCODING) as f:
                json.dump(file_group.calculate_checksums(), f,
                          indent=2)
            # Save provenance
            if file_group.provenance:
                self.put_provenance(file_group)

    def put_field(self, field, value):
        self._check_repository(field)
        if field.array:
            if field.datatype is str:
                value = ['"{}"'.format(v) for v in value]
            value = '[' + ','.join(str(v) for v in value) + ']'
        if field.datatype is str:
            value = '"{}"'.format(value)
        with self:
            xsession = self.get_xnode(field.data_node)
            xsession.fields[self.escape_name(field)] = value
        if field.provenance:
            self.put_provenance(field)

    def put_provenance(self, item):
        xnode = self.get_xnode(item.data_node)
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
        if not file_group.datatype.directory:
            # Replace the fnames with the relative path to the primary file
            primary = file_group.datatype.assort_files(checksums.keys())[0]
            new_checksums = {}
            for fname, chksum in checksums.items():
                try:
                    new_fname = Path(fname).relative_to(primary)
                except ValueError:
                    new_fname = '.'.join(fname)
                if new_fname in new_checksums:
                    new_fname = fname
                new_checksums[new_fname] = chksum
        return checksums

    def find_nodes(self, dataset: Dataset, **kwargs):
        """
        Find all file_groups, fields and provenance provenances within an XNAT
        project and create data tree within dataset

        Parameters
        ----------
        dataset : Dataset
            The dataset to construct
        """
        with self:
            # Get per_dataset level derivatives and fields
            for exp in self.login.projects[dataset.name].experiments.values():
                dataset.add_leaf_node([exp.subject.label, exp.label])

    def find_items(self, data_node):
        with self:
            xnode = self.get_xnode(data_node)
            # Add scans, fields and resources to data node
            try:
                xscans = xnode.scans
            except AttributeError:
                pass  # A subject or project node
            else:
                for xscan in xscans.values():
                    data_node.add_file_group(
                        path=xscan.type,
                        order=xscan.id,
                        quality=xscan.quality,
                        # Ensure uri uses resource label instead of ID
                        uris={r.label: '/'.join(r.uri.split('/')[:-1]
                                                + [r.label])
                              for r in xscan.resources.values()})
            for name, value in xnode.fields.items():
                data_node.add_field(
                    path=self.unescape_name(name),
                    value=value)
            for xresource in xnode.resources.values():
                data_node.add_file_group(
                    path=self.unescape_name(xresource.label),
                    uris={xresource.format: xresource.uri})

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
        with open(str(cache_path) + self.MD5_SUFFIX, 'w',
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
            if data_node.frequency == Clinical.dataset:
                xnode = xproject
            elif data_node.frequency == Clinical.subject:
                xnode = xproject.subjects[data_node.ids[Clinical.subject]]
            elif data_node.frequency == Clinical.session:
                xnode = xproject.experiments[data_node.ids[Clinical.session]]
            else:
                # Create a "subject" to hold the non-standard node (i.e. not
                # a project, subject or session node)
                node_label = (
                    '__' + '__'.join(
                        f'{l}_' + '_'.join(data_node.ids[l])
                        for l in data_node.frequency.nonzero_basis()) + '__')
                xnode = self.login.classes.SubjectData(label=node_label,
                                                       parent=xproject)
            return xnode

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
        return self.cache_dir.joinpath(*uri.split('/')[3:])

    def _check_repository(self, item):
        if item.data_node.dataset.repository is not self:
            raise ArcanaWrongRepositoryError(
                "{} is from {} instead of {}".format(
                    item, item.dataset.repository, self))

    def _file_group_paths(self, file_group):
        cache_path = self.cache_path(file_group)
        if not file_group.datatype.directory:
            primary_path, side_cars = file_group.datatype.assort_files(
                op.join(cache_path, f) for f in os.listdir(cache_path))
        else:
            primary_path = cache_path
            side_cars = None
        return primary_path, side_cars

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
        return cls.PATH_SEP.join(item.path.split('/'))

    @classmethod
    def unescape_name(cls, name):
        return '/'.join(name.split(cls.PATH_SEP))


    @classmethod
    def node_name(cls, data_node):
        """"""
        if data_node.frequency not in (Clinical.subject, Clinical.session):
            node_name = (
                '__' + '__'.join(
                    f'{l}_' + '_'.join(data_node.ids[l])
                    for l in data_node.frequency.nonzero_basis()) + '__')
        else:
            node_name = data_node.id
        return node_name

    # @classmethod
    # def unescape_name(cls, xname: str):
    #     """Reverses the escape of an item name by `escape_name`

    #     Parameters
    #     ----------
    #     xname : `str`
    #         An escaped name of a data node stored in the project resources

    #     Returns
    #     -------
    #     name : `str`
    #         The unescaped name of an item
    #     frequency : Clinical
    #         The frequency of the node
    #     ids : Dict[Clinical, str]
    #         A dictionary of IDs for the node
    #     """
    #     ids = {}
    #     id_parts = xname.split('___')[1:-1]
    #     freq_value = 0b0
    #     for part in id_parts:
    #         layer_freq_str, id = part.split('__')
    #         layer_freq = Clinical[layer_freq_str]
    #         ids[layer_freq] = id
    #         freq_value != layer_freq
    #     frequency = Clinical(freq_value)
    #     name_path = '/'.join(id_parts[-1].split('__'))
    #     return name_path, frequency, ids

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


def append_suffix(path, suffix):
    "Appends a string suffix to a Path object"
    return Path(str(path) + suffix)
