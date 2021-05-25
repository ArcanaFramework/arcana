from __future__ import absolute_import
import os.path as op
import tempfile
import unittest
import sys
from unittest import TestCase
import xnat
from arcana.utils.testing import BaseTestCase
from arcana.data import FilesetFilter
from arcana.data.file_format import text_format
from arcana.repository import XnatRepo
from arcana.processor import SingleProc
from arcana.utils.testing.xnat import SKIP_ARGS, SERVER, TestOnXnatMixin
from arcana.exceptions import ArcanaError


# Import TestExistingPrereqs analysis to test it on XNAT
sys.path.insert(0, op.join(op.dirname(__file__), '..', '..'))
import test_data  # noqa pylint: disable=import-error disable=wrong-import-position
from test_data import dicom_format  # noqa pylint: disable=import-error disable=wrong-import-position
sys.path.pop(0)

# Import test_local to run TestProjectInfo on XNAT using TestOnXnat mixin
sys.path.insert(0, op.join(op.dirname(__file__), '..', '..', 'processor'))
import test_to_process  # noqa pylint: disable=import-error disable=wrong-import-position
sys.path.pop(0)


class TestConnectDisconnect(TestCase):

    @unittest.skipIf(*SKIP_ARGS)
    def test_connect_disconnect(self):
        repository = XnatRepo(server=SERVER,
                              cache_dir=tempfile.mkdtemp())
        with repository:
            self._test_open(repository)
        self._test_closed(repository)

        with repository:
            self._test_open(repository)
            with repository:
                self._test_open(repository)
            self._test_open(repository)
        self._test_closed(repository)

    def _test_open(self, repository):
        self.assertTrue(repository.login.classes)  # check connection

    def _test_closed(self, repository):
        self.assertRaises(
            ArcanaError,
            getattr,
            repository,
            'login')


class TestProvInputChangeOnXnat(TestOnXnatMixin,
                                test_to_process.TestProvInputChange):

    BASE_CLASS = test_to_process.TestProvInputChange

    @unittest.skipIf(*SKIP_ARGS)
    def test_input_change(self):
        super(TestProvInputChangeOnXnat, self).test_input_change()


class TestDicomTagMatchAndIDOnXnat(TestOnXnatMixin,
                                   test_data.TestDicomTagMatch):

    BASE_CLASS = test_data.TestDicomTagMatch
    REF_FORMATS = [dicom_format]

    @property
    def ref_dir(self):
        return op.join(
            self.ref_path, self._get_name(self.BASE_CLASS))

    def setUp(self):
        test_data.TestDicomTagMatch.setUp(self)
        TestOnXnatMixin.setUp(self)
        # Set up DICOM headers
        with xnat.connect(SERVER) as login:
            xsess = login.projects[self.project].experiments[
                '_'.join((self.SUBJECT, self.VISIT))]
            login.put('/data/experiments/{}?pullDataFromHeaders=true'
                      .format(xsess.id))

    def tearDown(self):
        TestOnXnatMixin.tearDown(self)
        test_data.TestDicomTagMatch.tearDown(self)

    @unittest.skipIf(*SKIP_ARGS)
    def test_dicom_match(self):
        analysis = test_data.TestMatchAnalysis(
            name='test_dicom',
            dataset=XnatRepo(
                server=SERVER,
                cache_dir=tempfile.mkdtemp()).dataset(self.project),
            processor=SingleProc(self.work_dir),
            inputs=test_data.TestDicomTagMatch.DICOM_MATCH)
        phase = list(analysis.data('gre_phase', derive=True))[0]
        mag = list(analysis.data('gre_mag', derive=True))[0]
        self.assertEqual(phase.name, 'gre_field_mapping_3mm_phase')
        self.assertEqual(mag.name, 'gre_field_mapping_3mm_mag')

    @unittest.skipIf(*SKIP_ARGS)
    def test_id_match(self):
        analysis = test_data.TestMatchAnalysis(
            name='test_dicom',
            dataset=XnatRepo(
                server=SERVER,
                cache_dir=tempfile.mkdtemp()).dataset(self.project),
            processor=SingleProc(self.work_dir),
            inputs=[
                FilesetFilter('gre_phase', valid_formats=dicom_format, id=7),
                FilesetFilter('gre_mag', valid_formats=dicom_format, id=6)])
        phase = list(analysis.data('gre_phase', derive=True))[0]
        mag = list(analysis.data('gre_mag', derive=True))[0]
        self.assertEqual(phase.name, 'gre_field_mapping_3mm_phase')
        self.assertEqual(mag.name, 'gre_field_mapping_3mm_mag')

    @unittest.skipIf(*SKIP_ARGS)
    def test_order_match(self):
        test_data.TestDicomTagMatch.test_order_match(self)


class TestFilesetCacheOnPathAccess(TestOnXnatMixin, BaseTestCase):

    INPUT_FILESETS = {'fileset': '1'}

    @unittest.skipIf(*SKIP_ARGS)
    def test_cache_on_path_access(self):
        tmp_dir = tempfile.mkdtemp()
        repository = XnatRepo(
            server=SERVER,
            cache_dir=tmp_dir)
        dataset = repository.dataset(self.project,
                                     subject_ids=[self.SUBJECT],
                                     visit_ids=[self.VISIT])
        tree = dataset.tree
        # Get a fileset
        fileset = list(list(list(tree.subjects)[0].sessions)[0].filesets)[0]
        fileset.format = text_format
        # Accessing the path attribute directly, shouldn't trigger the download
        self.assertIsNone(fileset._path)  # noqa pylint: disable=protected-access
        # This should implicitly download the fileset
        self.assertIsNotNone(fileset.path)
        with open(fileset.path) as f:
            self.assertEqual(f.read(),
                             self.INPUT_FILESETS[fileset.basename])


class TestScanQualityLabelMatching(TestOnXnatMixin, BaseTestCase):

    # The labels are prefixed with the order that they should be uploaded
    # to XNAT with
    INPUT_FILESETS = {'1unusable': '1',
                      '2unlabelled': '1',
                      '3questionable': '1',
                      '4usable': '1'}

    def setUp(self):
        super(TestScanQualityLabelMatching, self).setUp()
        with self._connect() as login:
            xproject = login.projects[self.project]
            xsubject = login.classes.SubjectData(
                label='{}_{}'.format(self.project, self.SUBJECT),
                parent=xproject)
            xsession = login.classes.MrSessionData(
                label=self.session_label(),
                parent=xsubject)
            for scan in xsession.scans.values():
                if not scan.type.endswith('unlabelled'):
                    scan.quality = scan.type[1:]

    @unittest.skipIf(*SKIP_ARGS)
    def test_scan_label_quality(self):
        tmp_dir = tempfile.mkdtemp()
        repository = XnatRepo(
            server=SERVER, cache_dir=tmp_dir)
        dataset = repository.dataset(self.project,
                                     subject_ids=[self.SUBJECT],
                                     visit_ids=[self.VISIT])
        tree = dataset.tree
        for accepted, expected in (
                (None, '1unusable'),
                ((None, 'questionable', 'usable'), '2unlabelled'),
                (('questionable', 'usable'), '3questionable'),
                ('usable', '4usable')):
            inpt = FilesetFilter('dummy', order=0, valid_formats=text_format,
                                 acceptable_quality=accepted)
            matched = inpt.match(tree).item(subject_id=self.SUBJECT,
                                            visit_id=self.VISIT)
            self.assertEqual(matched.name, expected)
