from __future__ import absolute_import
from builtins import range
from builtins import object
import string
import random
import os
import os.path as op
import shutil
import re
from copy import copy
import pydicom
import xnat
from arcana2.utils.testing import BaseTestCase
from arcana2.repository.xnat import XnatRepo
from arcana2.repository import Dataset
from arcana2.exceptions import ArcanaError
from arcana2.data.file_format import text_format
import logging


logger = logging.getLogger('arcana')

try:
    SERVER = os.environ['ARCANA_TEST_XNAT']
except KeyError:
    SERVER = None

SKIP_ARGS = (SERVER is None, "Skipping as ARCANA_TEST_XNAT env var not set")


class CreateXnatProjectMixin(object):

    PROJECT_NAME_LEN = 12
    REF_FORMATS = [text_format]

    @property
    def project(self):
        """
        Creates a random string of letters and numbers to be the
        project ID
        """
        try:
            return self._project
        except AttributeError:
            self._project = ''.join(
                random.choice(string.ascii_uppercase + string.digits)
                for _ in range(self.PROJECT_NAME_LEN))
            return self._project

    def _create_project(self, project_name=None):
        if project_name is None:
            project_name = self.project
        if SERVER == 'https://mbi-xnat.erc.monash.edu.au':
            raise ArcanaError(
                "Shouldn't be creating projects on the production "
                "server")
        with xnat.connect(SERVER) as login:
            uri = '/data/archive/projects/{}'.format(project_name)
            query = {'xsiType': 'xnat:projectData', 'req_format': 'qa'}
            response = login.put(uri, query=query)
            if response.ok:
                logger.info("Created test project '{}'"
                            .format(project_name))

    def _delete_project(self, project_name=None):
        if project_name is None:
            project_name = self.project
        with xnat.connect(SERVER) as login:
            login.projects[project_name].delete()


class TestOnXnatMixin(CreateXnatProjectMixin):

    def session_label(self, subject=None, visit=None):
        if subject is None:
            subject = self.SUBJECT
        if visit is None:
            visit = self.VISIT
        label = '_'.join((subject, visit))
        return label

    def subject_label(self, subject=None):
        if subject is None:
            subject = self.SUBJECT
        return subject

    def session_uri(self, project=None, subject=None, visit=None):
        if project is None:
            project = self.project
        if subject is None:
            subject = self.SUBJECT
        return '/data/archive/projects/{}/subjects/{}/experiments/{}'.format(
            project, subject, self.session_label(subject, visit))

    def subject_uri(self, project=None, subject=None):
        if project is None:
            project = self.project
        if subject is None:
            subject = self.SUBJECT
        return '/data/archive/projects/{}/subjects/{}'.format(project, subject)

    def project_uri(self, project=None):
        if project is None:
            project = self.project
        return '/data/archive/projects/{}'.format(project)

    def session_cache_path(self, repository, project=None, subject=None,
                           visit=None):
        return repository.cache_path(self.session_uri(
            project=project, subject=subject, visit=visit))

    def subject_cache_path(self, repository, project=None, subject=None):
        return repository.cache_path(self.subject_uri(
            project=project, subject=subject))

    def project_cache_path(self, repository, project=None):
        return repository.cache_path(self.project_uri(project=project))


    def setUp(self):
        BaseTestCase.setUp(self)
        shutil.rmtree(self.cache_dir, ignore_errors=True)
        os.makedirs(self.cache_dir)
        self._create_project()
        with self._connect() as login:
            xproject = login.projects[self.project]
            xsubject = login.classes.SubjectData(
                label=self.SUBJECT,
                parent=xproject)
            xsession = login.classes.MrSessionData(
                label=self.session_label(),
                parent=xsubject)
            for file_group in self.session.file_groups:
                file_group.format = file_group.detect_format(self.REF_FORMATS)
                put_file_group(file_group, xsession)
            for field in self.session.fields:
                put_field(field, xsession)

    def tearDown(self):
        # Clean up working dirs
        shutil.rmtree(self.cache_dir, ignore_errors=True)
        # Clean up session created for unit-test
        self._delete_project()

    def _connect(self):
        return xnat.connect(SERVER)

def put_file_group(file_group, xsession):
    if file_group.format.name == 'dicom':
        dcm_files = [f for f in os.listdir(file_group.path)
                     if f.endswith('.dcm')]
        hdr = pydicom.dcmread(op.join(file_group.path, dcm_files[0]))
        id_ = int(hdr.SeriesNumber)
    else:
        id_ = file_group.basename
    xfile_group = xsession.xnat_session.classes.MrScanData(
        id=id_, type=file_group.basename, parent=xsession)
    resource = xfile_group.create_resource(
        file_group.format.resource_names(XnatRepo.type)[0])
    if file_group.format.directory:
        for fname in os.listdir(file_group.path):
            resource.upload(
                op.join(file_group.path, fname), fname)
    else:
        for path in file_group.paths:
            resource.upload(path, op.basename(path))


def put_field(field, xsession):
    if field.dtype is str:
        value = '"{}"'.format(field.value)
    else:
        value = field.value
    xsession.fields[field.name] = value



class TestMultiSubjectOnXnatMixin(CreateXnatProjectMixin):

    sanitize_id_re = re.compile(r'[^a-zA-Z_0-9]')

    dataset_depth = 2

    def setUp(self):
        self._clean_up()
        self._dataset = XnatRepo(
            server=SERVER, cache_dir=self.cache_dir).dataset(self.project)

        self._create_project()
        self.BASE_CLASS.setUp(self)
        # local_dataset = Dataset(self.project_dir, depth=self.dataset_depth)
        temp_dataset = XnatRepo(SERVER, '/tmp').dataset(self.project)
        with temp_dataset.repository:
            login = temp_dataset.repository.login
            xproject = login.projects[self.project]
            for node in self.input_tree:
                if node.subject_id is not None and node.visit_id is not None:
                    xsubject = login.classes.SubjectData(
                        label=node.subject_id,
                        parent=xproject)
                    xsession = login.classes.MrSessionData(
                        label='_'.join((node.subject_id, node.visit_id)),
                        parent=xsubject)
                else:
                    xsession = None
                for file_group in node.file_groups:
                    file_group._path = op.join(
                        self.local_dataset.repository.file_group_path(
                            file_group, dataset=self.local_dataset))
                    if not file_group.derived and xsession:
                        put_file_group(file_group, xsession)
                    else:
                        file_group = copy(file_group)
                        file_group._dataset = temp_dataset
                        file_group.put()
                for field in node.fields:
                    if not field.derived and xsession:
                        put_field(field, xsession)
                    else:
                        field = copy(field)
                        field._dataset = temp_dataset
                        field.put()
                for record in node.records:
                    temp_dataset.put_record(record)

    def tearDown(self):
        self._clean_up()
        self._delete_project()

    def _clean_up(self):
        # Clean up working dirs
        shutil.rmtree(self.cache_dir, ignore_errors=True)

    @property
    def dataset(self):
        return self._dataset

    @property
    def xnat_session_name(self):
        return '{}_{}'.format(self.project, self.base_name)

    @property
    def project_dir(self):
        return op.join(self.dataset_path, self.base_name)

    @property
    def output_cache_dir(self):
        return self._output_cache_dir

    @property
    def base_name(self):
        return self.BASE_CLASS._get_name()  # noqa pylint: disable=protected-access

    def _full_subject_id(self, subject):
        return self.project + '_' + subject

    def get_session_dir(self, subject=None, visit=None,
                        tree_level='per_session'):
        if subject is None and tree_level in ('per_session', 'per_subject'):
            subject = self.SUBJECT
        if visit is None and tree_level in ('per_session', 'per_visit'):
            visit = self.VISIT
        session_path = op.join(self.output_cache_dir, '{}_{}'.format(subject,
                                                                     visit))
        if not op.exists(session_path):
            raise ArcanaError(
                "Session path '{}' does not exist".format(session_path))
        return session_path

    def output_file_path(self, fname, from_analysis, subject=None, visit=None,
                         tree_level='per_session'):
        try:
            acq_path = self.BASE_CLASS.output_file_path(
                self, fname, from_analysis, subject=subject, visit=visit,
                tree_level=tree_level, derived=False)
        except KeyError:
            acq_path = None
        try:
            proc_path = self.BASE_CLASS.output_file_path(
                self, fname, from_analysis, subject=subject, visit=visit,
                tree_level=tree_level, derived=True)
        except KeyError:
            proc_path = None
        if acq_path is not None and op.exists(acq_path):
            if op.exists(proc_path):
                raise ArcanaError(
                    "Both acquired and derived paths were found for "
                    "'{}_{}' ({} and {})".format(from_analysis, fname,
                                                 acq_path, proc_path))
            path = acq_path
        else:
            path = proc_path
        return path


def filter_resources(names, visit=None, analysis=None):
    """Matchers out the names of resources to exclude provenance and
    md5"""
    filtered = []
    for name in names:
        match = re.match(
            r'(?:(?P<analysis>\w+)-)?(?:vis_(?P<visit>\w+)-)?(?P<deriv>\w+)',
            name)
        if ((analysis is None or match.analysis == analysis)
                and visit == match.group('visit')):
            filtered.append(name)
    return sorted(filtered)

def add_metadata_resources(names, md5=False):
    names = names + [XnatRepo.PROV_RESOURCE]
    if md5:
        names.extend(n + XnatRepo.MD5_SUFFIX for n in names)
    return sorted(names)
