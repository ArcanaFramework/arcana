import os
import os.path as op
import stat
from pathlib import Path
import typing as ty
from glob import glob
import time
import tempfile
import logging
import errno
import json
import re
from zipfile import ZipFile, BadZipfile
import shutil
import attrs
import xnat.session
from arcana.core.utils import JSON_ENCODING
from arcana.core.data.store import DataStore
from arcana.core.data.row import DataRow
from arcana.exceptions import ArcanaError, ArcanaUsageError, ArcanaWrongRepositoryError
from arcana.core.utils import dir_modtime, parse_value
from arcana.core.data.set import Dataset
from arcana.core.utils import path2varname, varname2path, asdict
from arcana.data.spaces.medimage import Clinical


logger = logging.getLogger("arcana")

special_char_re = re.compile(r"[^a-zA-Z_0-9]")
tag_parse_re = re.compile(r"\((\d+),(\d+)\)")

RELEVANT_DICOM_TAG_TYPES = set(("UI", "CS", "DA", "TM", "SH", "LO", "PN", "ST", "AS"))

COMMAND_INPUT_TYPES = {bool: "bool", str: "string", int: "number", float: "number"}


@attrs.define
class Xnat(DataStore):
    """
    Access class for XNAT data repositories

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
    race_condition_delay : int
        The amount of time to wait before checking that the required
        file_group has been downloaded to cache by another process has
        completed if they are attempting to download the same file_group
    """

    server: str = attrs.field()
    cache_dir: str = attrs.field(converter=Path)
    user: str = attrs.field(default=None, metadata={"asdict": False})
    password: str = attrs.field(default=None, metadata={"asdict": False})
    check_md5: bool = attrs.field(default=True)
    race_condition_delay: int = attrs.field(default=30)
    _cached_datasets: ty.Dict[str, Dataset] = attrs.field(factory=dict, init=False)
    _login: xnat.session.XNATSession = attrs.field(default=None, init=False)

    alias = "xnat"
    MD5_SUFFIX = ".md5.json"
    PROV_SUFFIX = ".__prov__.json"
    FIELD_PROV_RESOURCE = "__provenance__"
    depth = 2
    DEFAULT_SPACE = Clinical
    DEFAULT_HIERARCHY = ["subject", "session"]
    METADATA_RESOURCE = "__arcana__"

    def save_dataset_definition(
        self, dataset_id: str, definition: ty.Dict[str, ty.Any], name: str
    ):
        with self:
            xproject = self.login.projects[dataset_id]
            try:
                xresource = xproject.resources[self.METADATA_RESOURCE]
            except KeyError:
                # Create the new resource for the file_group
                xresource = self.login.classes.ResourceCatalog(
                    parent=xproject, label=self.METADATA_RESOURCE, format="json"
                )
            definition_file = Path(tempfile.mkdtemp()) / str(name + ".json")
            with open(definition_file, "w") as f:
                json.dump(definition, f, indent="    ")
            xresource.upload(str(definition_file), name + ".json", overwrite=True)

    def load_dataset_definition(
        self, dataset_id: str, name: str
    ) -> ty.Dict[str, ty.Any]:
        with self:
            xproject = self.login.projects[dataset_id]
            try:
                xresource = xproject.resources[self.METADATA_RESOURCE]
            except KeyError:
                definition = None
            else:
                download_dir = Path(tempfile.mkdtemp())
                xresource.download_dir(download_dir)
                fpath = (
                    download_dir
                    / dataset_id
                    / "resources"
                    / "__arcana__"
                    / "files"
                    / (name + ".json")
                )
                print(fpath)
                if fpath.exists():
                    with open(fpath) as f:
                        definition = json.load(f)
                else:
                    definition = None
        return definition

    @cache_dir.validator
    def cache_dir_validator(self, _, cache_dir):
        if not cache_dir.exists():
            raise ValueError(f"Cache dir, '{cache_dir}' does not exist")

    @property
    def login(self):
        if self._login is None:
            raise ArcanaError(
                "XNAT repository has been disconnected before " "exiting outer context"
            )
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
            sess_kwargs["user"] = self.user
        if self.password is not None:
            sess_kwargs["password"] = self.password
        self._login = xnat.connect(server=self.server, **sess_kwargs)

    def disconnect(self):
        self.login.disconnect()
        self._login = None

    def find_rows(self, dataset: Dataset, **kwargs):
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
            for exp in self.login.projects[dataset.id].experiments.values():
                dataset.add_leaf([exp.subject.label, exp.label])

    def find_items(self, row):
        with self:
            xrow = self.get_xrow(row)
            # Add scans, fields and resources to data row
            try:
                xscans = xrow.scans
            except AttributeError:
                pass  # A subject or project row
            else:
                for xscan in xscans.values():
                    row.add_file_group(
                        path=xscan.type,
                        order=xscan.id,
                        quality=xscan.quality,
                        # Ensure uri uses resource label instead of ID
                        uris={
                            r.label: "/".join(r.uri.split("/")[:-1] + [r.label])
                            for r in xscan.resources.values()
                        },
                    )
            for name, value in xrow.fields.items():
                row.add_field(path=varname2path(name), value=value)
            for xresource in xrow.resources.values():
                row.add_file_group(
                    path=varname2path(xresource.label),
                    uris={xresource.format: xresource.uri},
                )

    def get_file_group_paths(self, file_group):
        """
        Caches a file_group to the local file system and returns the path to
        the cached files

        Parameters
        ----------
        file_group : FileGroup
            The file_group to retrieve the files/directories for

        Returns
        -------
        list[Path]
            The paths to cached files/directories on the local file-system
        """
        logger.info(
            "Getting %s from %s:%s row via API access",
            file_group.path,
            file_group.row.frequency,
            file_group.row.id,
        )
        self._check_store(file_group)
        with self:  # Connect to the XNAT repository if haven't already
            xrow = self.get_xrow(file_group.row)
            if not file_group.uri:
                base_uri = self.standard_uri(xrow)
                # if file_group.derived:
                xresource = xrow.resources[path2varname(file_group.path)]
                # else:
                #     # If file_group is a primary 'scan' (rather than a
                #     # derivative) we need to get the resource of the scan
                #     # instead of the scan
                #     xscan = xrow.scans[file_group.name]
                #     file_group.id = xscan.id
                #     base_uri += '/scans/' + xscan.id
                #     xresource = xscan.resources[file_group.class_name]
                # Set URI so we can retrieve checksums if required. We ensure we
                # use the resource name instead of its ID in the URI for
                # consistency with other locations where it is set and to keep the
                # cache name_path consistent
                file_group.uri = base_uri + "/resources/" + xresource.label
            cache_path = self.cache_path(file_group)
            need_to_download = True
            if op.exists(cache_path):
                if self.check_md5:
                    md5_path = append_suffix(cache_path, self.MD5_SUFFIX)
                    if md5_path.exists():
                        with open(md5_path, "r") as f:
                            cached_checksums = json.load(f)
                    if cached_checksums == file_group.checksums:
                        need_to_download = False
                else:
                    need_to_download = False
            if need_to_download:
                # The name_path to the directory which the files will be
                # downloaded to.
                tmp_dir = append_suffix(cache_path, ".download")
                xresource = self.login.classes.Resource(
                    uri=file_group.uri, xnat_session=self.login
                )
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
                        # store
                        self._delayed_download(
                            tmp_dir,
                            xresource,
                            file_group,
                            cache_path,
                            delay=self._race_cond_delay,
                        )
                    else:
                        raise
                else:
                    self.download_file_group(tmp_dir, xresource, file_group, cache_path)
                    shutil.rmtree(tmp_dir)
        if file_group.is_dir:
            cache_paths = [cache_path]
        else:
            cache_paths = list(cache_path.iterdir())
        return cache_paths

    def put_file_group_paths(self, file_group, fs_paths):
        """
        Stores files for a file group into the XNAT repository

        Parameters
        ----------
        file_group : FileGroup
            The file-group to put the paths for
        fs_paths: list[Path or str  ]
            The paths of files/directories to put into the XNAT repository

        Returns
        -------
        list[Path]
            The locations of the locally cached paths
        """
        self._check_store(file_group)
        # Open XNAT session
        with self:
            # Add session for derived scans if not present
            xrow = self.get_xrow(file_group.row)
            escaped_name = path2varname(file_group.path)
            if not file_group.uri:
                # Set the uri of the file_group
                file_group.uri = "{}/resources/{}".format(
                    self.standard_uri(xrow), escaped_name
                )
            # Delete existing resource (if present)
            try:
                xresource = xrow.resources[escaped_name]
            except KeyError:
                pass
            else:
                # Delete existing resource. We could possibly just use the
                # 'overwrite' option of upload but this would leave files in
                # the previous file_group that aren't in the current
                xresource.delete()
            # Create the new resource for the file_group
            xresource = self.login.classes.ResourceCatalog(
                parent=xrow, label=escaped_name, format=file_group.class_name()
            )
            # Create cache path
            base_cache_path = self.cache_path(file_group)
            if base_cache_path.exists():
                shutil.rmtree(base_cache_path)
            # Upload data and add it to cache
            cache_paths = []
            for fs_path in fs_paths:
                if fs_path.is_dir():
                    # Upload directory to XNAT and add to cache
                    for dpath, _, fnames in os.walk(fs_path):
                        dpath = Path(dpath)
                        for fname in fnames:
                            fpath = dpath / fname
                            frelpath = fpath.relative_to(fs_path)
                            xresource.upload(str(fpath), str(frelpath))
                    shutil.copytree(fs_path, base_cache_path)
                    cache_path = base_cache_path
                else:
                    # Upload file path to XNAT and add to cache
                    fname = file_group.copy_ext(fs_path, escaped_name)
                    xresource.upload(str(fs_path), str(fname))
                    base_cache_path.mkdir(
                        exist_ok=True, parents=True, mode=stat.S_IRWXU | stat.S_IRWXG
                    )
                    cache_path = base_cache_path / fname
                    shutil.copyfile(fs_path, cache_path)
                cache_paths.append(cache_path)
            # need to manually set this here in order to calculate the
            # checksums (instead of waiting until after the 'put' is finished)
            file_group.set_fs_paths(cache_paths)
            with open(
                append_suffix(base_cache_path, self.MD5_SUFFIX), "w", **JSON_ENCODING
            ) as f:
                json.dump(file_group.calculate_checksums(), f, indent=2)
        logger.info(
            "Put %s into %s:%s row via API access",
            file_group.path,
            file_group.row.frequency,
            file_group.row.id,
        )
        return cache_paths

    def get_field_value(self, field):
        """
        Retrieves a fields value

        Parameters
        ----------
        field : Field
            The field to retrieve

        Returns
        -------
        value : ty.Union[float, int, str, ty.List[float], ty.List[int], ty.List[str]]
            The value of the field
        """
        self._check_store(field)
        with self:
            xsession = self.get_xrow(field.row)
            val = xsession.fields[path2varname(field)]
            val = val.replace("&quot;", '"')
            val = parse_value(val)
        return val

    def put_field_value(self, field, value):
        """Store the value for a field in the XNAT repository

        Parameters
        ----------
        field : Field
            the field to store the value for
        value : str or float or int or bool
            the value to store
        """
        self._check_store(field)
        if field.array:
            if field.format is str:
                value = ['"{}"'.format(v) for v in value]
            value = "[" + ",".join(str(v) for v in value) + "]"
        if field.format is str:
            value = '"{}"'.format(value)
        with self:
            xsession = self.get_xrow(field.row)
            xsession.fields[path2varname(field)] = value

    def get_checksums(self, file_group):
        """
        Downloads the MD5 digests associated with the files in the file-set.
        These are saved with the downloaded files in the cache and used to
        check if the files have been updated on the server

        Parameters
        ----------
        file_group: FileGroup
            the file_group to get the checksums for. Used to
            determine the primary file within the resource and change the
            corresponding key in the checksums dictionary to '.' to match
            the way it is generated locally by Arcana.
        """
        if file_group.uri is None:
            raise ArcanaUsageError(
                "Can't retrieve checksums as URI has not been set for {}".format(
                    file_group
                )
            )
        with self:
            checksums = {
                r["URI"]: r["digest"]
                for r in self.login.get_json(file_group.uri + "/files")["ResultSet"][
                    "Result"
                ]
            }
        # strip base URI to get relative paths of files within the resource
        checksums = {
            re.match(r".*/resources/\w+/files/(.*)$", u).group(1): c
            for u, c in sorted(checksums.items())
        }
        # if not self.is_dir:
        #     # Replace the paths of the primary file with primary file with '.'
        #     checksums['.'] = checksums.pop(primary)
        #     for path in set(checksums.keys()) - set(['.']):
        #         ext = '.'.join(Path(path).suffixes)
        #         if ext in checksums:
        #             logger.warning(
        #                 f"Multiple side-cars found in {file_group} XNAT "
        #                 f"resource with the same extension (this isn't "
        #                 f"allowed) and therefore cannot convert {path} to "
        #                 "{ext} in checksums")
        #         else:
        #             checksums[ext] = checksums.pop(path)
        if not file_group.is_dir:
            checksums = file_group.generalise_checksum_keys(
                checksums, base_path=file_group.matches_ext(*checksums.keys())
            )
        return checksums

    def dicom_header(self, file_group):
        def convert(val, code):
            if code == "TM":
                try:
                    val = float(val)
                except ValueError:
                    pass
            elif code == "CS":
                val = val.split("\\")
            return val

        with self:
            scan_uri = "/" + "/".join(file_group.uri.split("/")[2:-2])
            response = self.login.get(
                "/REST/services/dicomdump?src=" + scan_uri
            ).json()["ResultSet"]["Result"]
        hdr = {
            tag_parse_re.match(t["tag1"]).groups(): convert(t["value"], t["vr"])
            for t in response
            if (tag_parse_re.match(t["tag1"]) and t["vr"] in RELEVANT_DICOM_TAG_TYPES)
        }
        return hdr

    def download_file_group(self, tmp_dir, xresource, file_group, cache_path):
        # Download resource to zip file
        zip_path = op.join(tmp_dir, "download.zip")
        with open(zip_path, "wb") as f:
            xresource.xnat_session.download_stream(
                xresource.uri + "/files", f, format="zip", verbose=True
            )
        checksums = self.get_checksums(file_group)
        # Extract downloaded zip file
        expanded_dir = op.join(tmp_dir, "expanded")
        try:
            with ZipFile(zip_path) as zip_file:
                zip_file.extractall(expanded_dir)
        except BadZipfile as e:
            raise ArcanaError(
                "Could not unzip file '{}' ({})".format(xresource.id, e)
            ) from e
        data_path = glob(expanded_dir + "/**/files", recursive=True)[0]
        # Remove existing cache if present
        try:
            shutil.rmtree(cache_path)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise e
        shutil.move(data_path, cache_path)
        with open(str(cache_path) + self.MD5_SUFFIX, "w", **JSON_ENCODING) as f:
            json.dump(checksums, f, indent=2)

    def _delayed_download(self, tmp_dir, xresource, file_group, cache_path, delay):
        logger.info(
            "Waiting %s seconds for incomplete download of '%s' "
            "initiated another process to finish",
            delay,
            cache_path,
        )
        initial_mod_time = dir_modtime(tmp_dir)
        time.sleep(delay)
        if op.exists(cache_path):
            logger.info(
                "The download of '%s' has completed "
                "successfully in the other process, continuing",
                cache_path,
            )
            return
        elif initial_mod_time != dir_modtime(tmp_dir):
            logger.info(
                "The download of '%s' hasn't completed yet, but it has"
                " been updated.  Waiting another %s seconds before "
                "checking again.",
                cache_path,
                delay,
            )
            self._delayed_download(tmp_dir, xresource, file_group, cache_path, delay)
        else:
            logger.warning(
                "The download of '%s' hasn't updated in %s "
                "seconds, assuming that it was interrupted and "
                "restarting download",
                cache_path,
                delay,
            )
            shutil.rmtree(tmp_dir)
            os.mkdir(tmp_dir)
            self.download_file_group(tmp_dir, xresource, file_group, cache_path)

    def get_xrow(self, row):
        """
        Returns the XNAT session and cache dir corresponding to the provided
        row

        Parameters
        ----------
        row : DataRow
            The row to get the corresponding XNAT row for
        """
        with self:
            xproject = self.login.projects[row.dataset.id]
            if row.frequency == Clinical.dataset:
                xrow = xproject
            elif row.frequency == Clinical.subject:
                xrow = xproject.subjects[row.ids[Clinical.subject]]
            elif row.frequency == Clinical.session:
                xrow = xproject.experiments[row.ids[Clinical.session]]
            else:
                xrow = self.login.classes.SubjectData(
                    label=self._make_row_name(row), parent=xproject
                )
            return xrow

    def _make_row_name(self, row):
        # Create a "subject" to hold the non-standard row (i.e. not
        # a project, subject or session row)
        if row.id is None:
            id_str = ""
        elif isinstance(row.id, tuple):
            id_str = "_" + "_".join(row.id)
        else:
            id_str = "_" + str(row.id)
        return f"__{row.frequency}{id_str}__"

    def _make_uri(self, row: DataRow):
        uri = "/data/archive/projects/" + row.dataset.id
        if row.frequency == Clinical.session:
            uri += "/experiments/" + row.id
        elif row.frequency == Clinical.subject:
            uri += "/subjects/" + row.id
        elif row.frequency != Clinical.dataset:
            uri += "/subjects/" + self._make_row_name(row)
        return uri

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
        return self.cache_dir.joinpath(*uri.split("/")[3:])

    def _check_store(self, item):
        if item.row.dataset.store is not self:
            raise ArcanaWrongRepositoryError(
                "{} is from {} instead of {}".format(item, item.dataset.store, self)
            )

    @classmethod
    def standard_uri(cls, xrow):
        """Get the URI of the XNAT row (ImageSession | Subject | Project)
        using labels rather than IDs for subject and sessions, e.g

        >>> from arcana.data.stores.medimage import Xnat
        >>> store = Xnat.load('my-xnat')
        >>> xsession = store.login.experiments['MRH017_100_MR01']
        >>> store.standard_uri(xsession)

        '/data/archive/projects/MRH017/subjects/MRH017_100/experiments/MRH017_100_MR01'

        Parameters
        ----------
        xrow : xnat.ImageSession | xnat.Subject | xnat.Project
            A row of the XNAT data tree
        """
        uri = xrow.uri
        if "experiments" in uri:
            # Replace ImageSession ID with label in URI.
            uri = re.sub(r"(?<=/experiments/)[^/]+", xrow.label, uri)
        if "subjects" in uri:
            try:
                # If xrow is a ImageSession
                subject_id = xrow.subject.label
            except AttributeError:
                # If xrow is a Subject
                subject_id = xrow.label
            except KeyError:
                # There is a bug where the subject isn't appeared to be cached
                # so we use this as a workaround
                subject_json = xrow.xnat_session.get_json(
                    xrow.uri.split("/experiments")[0]
                )
                subject_id = subject_json["items"][0]["data_fields"]["label"]
            # Replace subject ID with subject label in URI
            uri = re.sub(r"(?<=/subjects/)[^/]+", subject_id, uri)
        return uri

    def put_provenance(self, item, provenance: ty.Dict[str, ty.Any]):
        xresource, _, cache_path = self._provenance_location(item, create_resource=True)
        with open(cache_path, "w") as f:
            json.dump(provenance, f, indent="  ")
        xresource.upload(cache_path, cache_path.name)

    def get_provenance(self, item) -> ty.Dict[str, ty.Any]:
        try:
            xresource, uri, cache_path = self._provenance_location(item)
        except KeyError:
            return {}  # Provenance doesn't exist on server
        with open(cache_path, "w") as f:
            xresource.xnat_session.download_stream(uri, f)
            provenance = json.load(f)
        return provenance

    def _provenance_location(self, item, create_resource=False):
        xrow = self.get_xrow(item.row)
        if item.is_field:
            fname = self.FIELD_PROV_PREFIX + path2varname(item)
        else:
            fname = path2varname(item) + ".json"
        uri = f"{self.standard_uri(xrow)}/resources/{self.PROV_RESOURCE}/files/{fname}"
        cache_path = self.cache_path(uri)
        cache_path.parent.mkdir(parent=True, exist_ok=True)
        try:
            xresource = xrow.resources[self.PROV_RESOURCE]
        except KeyError:
            if create_resource:
                xresource = self.login.classes.ResourceCatalog(
                    parent=xrow, label=self.PROV_RESOURCE, format="PROVENANCE"
                )
            else:
                raise
        return xresource, uri, cache_path

    def _encrypt_credentials(self, serialised):
        with self:
            (
                serialised["user"],
                serialised["password"],
            ) = self.login.services.issue_token()

    def asdict(self, **kwargs):
        # Call asdict utility method with 'ignore_instance_method' to avoid
        # infinite recursion
        dct = asdict(self, **kwargs)
        self._encrypt_credentials(dct)
        return dct


def append_suffix(path, suffix):
    "Appends a string suffix to a Path object"
    return Path(str(path) + suffix)
