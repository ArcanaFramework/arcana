import os.path as op
import tempfile
import unittest
import sys
from arcana.utils.testing import BaseMultiSubjectTestCase
from arcana.repository.xnat import XnatRepo
from arcana.data import (
    FilesetFilter, InputFilesetSpec)
from arcana.analysis import Analysis, AnalysisMetaClass
from arcana.data.file_format import text_format
from arcana.repository.tree import Tree
from arcana.data import Fileset
from arcana.utils.testing.xnat import (
    TestMultiSubjectOnXnatMixin, SKIP_ARGS, SERVER)

# Import TestExistingPrereqs analysis to test it on XNAT
sys.path.insert(0, op.join(op.dirname(__file__), '..', '..', 'analysis'))
import test_analysis  # noqa pylint: disable=import-error disable=wrong-import-position
sys.path.pop(0)

# Import test_local to run TestProjectInfo on XNAT using TestOnXnat mixin
sys.path.insert(0, op.join(op.dirname(__file__), '..'))
import test_directory  # noqa pylint: disable=import-error disable=wrong-import-position
sys.path.pop(0)


class TestAnalysis(Analysis, metaclass=AnalysisMetaClass):

    add_data_specs = [
        InputFilesetSpec('fileset1', text_format),
        InputFilesetSpec('fileset2', text_format, optional=True),
        InputFilesetSpec('fileset3', text_format),
        InputFilesetSpec('fileset5', text_format, optional=True)]


class TestExistingPrereqsOnXnat(TestMultiSubjectOnXnatMixin,
                                test_analysis.TestExistingPrereqs):

    BASE_CLASS = test_analysis.TestExistingPrereqs

    @unittest.skipIf(*SKIP_ARGS)
    def test_per_session_prereqs(self):
        super().test_per_session_prereqs()


class TestProjectInfo(TestMultiSubjectOnXnatMixin,
                      test_directory.TestDirectoryProjectInfo):

    BASE_CLASS = test_directory.TestDirectoryProjectInfo

    @unittest.skipIf(*SKIP_ARGS)
    def test_project_info(self):
        tree = self.dataset.tree
        for node in tree.nodes():
            for fileset in node.filesets:
                fileset.format = text_format
                # Clear id and format name from regenerated tree
                # fileset._id = None  # pylint: disable=protected-access
#                 fileset.get()
        ref_tree = self.get_tree(self.dataset)  # , sync_with_repo=True)
        self.assertEqual(
            tree, ref_tree,
            "Generated project doesn't match reference:{}"
            .format(tree.find_mismatch(ref_tree)))


class TestXnatCache(TestMultiSubjectOnXnatMixin,
                    BaseMultiSubjectTestCase):

    BASE_CLASS = BaseMultiSubjectTestCase
    STRUCTURE = {
        'subject1': {
            'visit1': ['fileset1', 'fileset2', 'fileset3'],
            'visit2': ['fileset1', 'fileset2', 'fileset3']},
        'subject2': {
            'visit1': ['fileset1', 'fileset2', 'fileset3'],
            'visit2': ['fileset1', 'fileset2', 'fileset3']}}

    DATASET_CONTENTS = {'fileset1': 1,
                        'fileset2': 2,
                        'fileset3': 3}

    @property
    def input_tree(self):
        filesets = []
        for subj_id, visits in list(self.STRUCTURE.items()):
            for visit_id, fileset_names in list(visits.items()):
                filesets.extend(
                    Fileset(d, text_format, subject_id=subj_id,
                            visit_id=visit_id) for d in fileset_names)
        return Tree.construct(self.dataset.repository, filesets=filesets)

    # @unittest.skipIf(*SKIP_ARGS)
    @unittest.skip  # Will work out a better solution for caching in v2
    def test_cache_download(self):
        repository = XnatRepo(
            server=SERVER,
            cache_dir=tempfile.mkdtemp())
        dataset = repository.dataset(self.project)
        analysis = self.create_analysis(
            TestAnalysis, 'cache_download',
            inputs=[
                FilesetFilter('fileset1', 'fileset1', text_format),
                FilesetFilter('fileset3', 'fileset3', text_format)],
            dataset=dataset)
        analysis.cache_inputs()
        for subject_id, visits in list(self.STRUCTURE.items()):
            subj_dir = op.join(
                repository.cache_dir, self.project,
                '{}_{}'.format(self.project, subject_id))
            for visit_id in visits:
                sess_dir = op.join(
                    subj_dir,
                    '{}_{}_{}'.format(self.project, subject_id,
                                      visit_id))
                for inpt in analysis.inputs:
                    self.assertTrue(op.exists(
                        op.join(sess_dir, inpt.name + '-' + inpt.name)))

    @property
    def base_name(self):
        return self.name
