import os
import os.path as op
import shutil
import json
import time
from glob import glob
import unittest
from multiprocessing import Process
from arcana.utils.testing import BaseTestCase
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from arcana.processor import SingleProc
from arcana.repository.xnat import XnatRepo
from arcana.repository.interfaces import RepositorySource, RepositorySink
from arcana.data import FilesetFilter
from arcana.utils import PATH_SUFFIX, JSON_ENCODING
from arcana.data.file_format import text_format
from arcana.utils.testing.xnat import (
    TestOnXnatMixin, SERVER, SKIP_ARGS, filter_resources,
    add_metadata_resources, logger)
from arcana.analysis import Analysis, AnalysisMetaClass
from arcana.data import InputFilesetSpec, FilesetSpec, FieldSpec


class DummyAnalysis(Analysis, metaclass=AnalysisMetaClass):

    add_data_specs = [
        InputFilesetSpec('source1', text_format),
        InputFilesetSpec('source2', text_format, optional=True),
        InputFilesetSpec('source3', text_format, optional=True),
        InputFilesetSpec('source4', text_format, optional=True),
        FilesetSpec('sink1', text_format, 'dummy_pipeline'),
        FilesetSpec('sink3', text_format, 'dummy_pipeline'),
        FilesetSpec('sink4', text_format, 'dummy_pipeline'),
        FilesetSpec('subject_sink', text_format, 'dummy_pipeline',
                    frequency='per_subject'),
        FilesetSpec('visit_sink', text_format, 'dummy_pipeline',
                    frequency='per_visit'),
        FilesetSpec('dataset_sink', text_format, 'dummy_pipeline',
                    frequency='per_dataset'),
        FilesetSpec('resink1', text_format, 'dummy_pipeline'),
        FilesetSpec('resink2', text_format, 'dummy_pipeline'),
        FilesetSpec('resink3', text_format, 'dummy_pipeline'),
        FieldSpec('field1', int, 'dummy_pipeline'),
        FieldSpec('field2', float, 'dummy_pipeline'),
        FieldSpec('field3', str, 'dummy_pipeline')]

    def dummy_pipeline(self, **name_maps):
        return self.new_pipeline('dummy_pipeline', name_maps=name_maps)


class TestXnatSourceAndSinkBase(TestOnXnatMixin, BaseTestCase):

    SUBJECT = 'SUBJECT'
    VISIT = 'VISIT'
    ANALYSIS_NAME = 'aanalysis'
    SUMMARY_ANALYSIS = 'asummary'

    INPUT_FILESETS = {'source1': 'foo', 'source2': 'bar',
                      'source3': 'wee', 'source4': 'wa'}
    INPUT_FIELDS = {'field1': 1, 'field2': 0.5, 'field3': 'boo'}

    def setUp(self):
        BaseTestCase.setUp(self)
        TestOnXnatMixin.setUp(self)

    def tearDown(self):
        TestOnXnatMixin.tearDown(self)
        BaseTestCase.tearDown(self)


class TestXnatSourceAndSink(TestXnatSourceAndSinkBase):

    @property
    def checksum_sink_project(self):
        return self.project + 'SINK'

    @unittest.skipIf(*SKIP_ARGS)
    def setUp(self):
        TestXnatSourceAndSinkBase.setUp(self)
        self._create_project(self.checksum_sink_project)

    @unittest.skipIf(*SKIP_ARGS)
    def tearDown(self):
        TestXnatSourceAndSinkBase.tearDown(self)
        self._delete_project(self.checksum_sink_project)

    @unittest.skipIf(*SKIP_ARGS)
    def test_repository_roundtrip(self):

        # Create working dirs
        # Create DarisSource node
        repository = XnatRepo(
            server=SERVER, cache_dir=self.cache_dir)
        dataset = repository.dataset(self.project)
        analysis = DummyAnalysis(
            self.ANALYSIS_NAME, dataset=dataset, processor=SingleProc('a_dir'),
            inputs=[FilesetFilter('source1', 'source1', text_format),
                    FilesetFilter('source2', 'source2', text_format),
                    FilesetFilter('source3', 'source3', text_format),
                    FilesetFilter('source4', 'source4', text_format)])
        # TODO: Should test out other file formats as well.
        source_files = ['source1', 'source2', 'source3', 'source4']
        sink_files = ['sink1', 'sink3', 'sink4']
        inputnode = pe.Node(IdentityInterface(['subject_id',
                                               'visit_id']),
                            'inputnode')
        inputnode.inputs.subject_id = str(self.SUBJECT)
        inputnode.inputs.visit_id = str(self.VISIT)
        source = pe.Node(
            RepositorySource(
                analysis.bound_spec(f).slice for f in source_files),
            name='source')
        dummy_pipeline = analysis.dummy_pipeline()
        dummy_pipeline.cap()
        sink = pe.Node(
            RepositorySink(
                (analysis.bound_spec(f).slice for f in sink_files),
                dummy_pipeline),
            name='sink')
        sink.inputs.name = 'repository-roundtrip-unittest'
        sink.inputs.desc = (
            "A test session created by repository roundtrip unittest")
        # Create workflow connecting them together
        workflow = pe.Workflow('source-sink-unit-test',
                               base_dir=self.work_dir)
        workflow.add_nodes((source, sink))
        workflow.connect(inputnode, 'subject_id', source, 'subject_id')
        workflow.connect(inputnode, 'visit_id', source, 'visit_id')
        workflow.connect(inputnode, 'subject_id', sink, 'subject_id')
        workflow.connect(inputnode, 'visit_id', sink, 'visit_id')
        for source_name in source_files:
            if source_name != 'source2':
                sink_name = source_name.replace('source', 'sink')
                workflow.connect(
                    source, source_name + PATH_SUFFIX,
                    sink, sink_name + PATH_SUFFIX)
        workflow.run()
        # Check cache was created properly
        scans_cache_dir = glob('{}/**/scans'.format(repository.cache_dir),
                               recursive=True)[0]
        self.assertEqual(sorted(os.listdir(scans_cache_dir)), source_files)
        resources_cache_dir = scans_cache_dir[:-len('scans')] + 'resources'
        expected_resources = sorted(
            repository.prepend_analysis(f, self.ANALYSIS_NAME)
            for f in sink_files + [repository.PROV_RESOURCE])
        self.assertEqual(
            sorted(os.listdir(resources_cache_dir)),
            sorted(
                expected_resources + [
                    (repository.prepend_analysis(f, self.ANALYSIS_NAME)
                     + repository.MD5_SUFFIX)
                    for f in sink_files]))
        with repository:
            xproject = repository.login.projects[self.project]
            xsession = xproject.experiments[self.session_label()]
            fileset_names = sorted(r.label for r in xsession.resources.values())
        self.assertEqual(fileset_names, expected_resources)

    @unittest.skipIf(*SKIP_ARGS)
    def test_fields_roundtrip(self):
        repository = XnatRepo(
            server=SERVER, cache_dir=self.cache_dir)
        dataset = repository.dataset(self.project)
        analysis = DummyAnalysis(
            self.ANALYSIS_NAME, dataset=dataset, processor=SingleProc('a_dir'),
            inputs=[FilesetFilter('source1', 'source1', text_format)])
        fields = ['field{}'.format(i) for i in range(1, 4)]
        dummy_pipeline = analysis.dummy_pipeline()
        dummy_pipeline.cap()
        sink = pe.Node(
            RepositorySink(
                (analysis.bound_spec(f).slice for f in fields),
                dummy_pipeline),
            name='fields_sink')
        sink.inputs.field1_field = field1 = 1
        sink.inputs.field2_field = field2 = 2.0
        sink.inputs.field3_field = field3 = str('3')
        sink.inputs.subject_id = self.SUBJECT
        sink.inputs.visit_id = self.VISIT
        sink.inputs.desc = "Test sink of fields"
        sink.inputs.name = 'test_sink'
        sink.run()
        source = pe.Node(
            RepositorySource(
                analysis.bound_spec(f).slice for f in fields),
            name='fields_source')
        source.inputs.visit_id = self.VISIT
        source.inputs.subject_id = self.SUBJECT
        source.inputs.desc = "Test source of fields"
        source.inputs.name = 'test_source'
        results = source.run()
        self.assertEqual(results.outputs.field1_field, field1)
        self.assertEqual(results.outputs.field2_field, field2)
        self.assertEqual(results.outputs.field3_field, field3)

    # @unittest.skip('Skipping delayed download test as it is is proving '
    #                'problematic')
    # def test_delayed_download(self):
    #     """
    #     Tests handling of race conditions where separate processes attempt to
    #     cache the same fileset
    #     """
    #     cache_dir = op.join(self.work_dir,
    #                         'cache-delayed-download')
    #     DATASET_NAME = 'source1'
    #     target_path = op.join(self.session_cache(cache_dir),
    #                           DATASET_NAME,
    #                           DATASET_NAME + text_format.extension)
    #     tmp_dir = target_path + '.download'
    #     shutil.rmtree(cache_dir, ignore_errors=True)
    #     os.makedirs(cache_dir)
    #     repository = XnatRepo(server=SERVER, cache_dir=cache_dir)
    #     dataset = repository.dataset(self.project)
    #     analysis = DummyAnalysis(
    #         self.ANALYSIS_NAME, dataset, SingleProc('ad'),
    #         inputs=[FilesetFilter(DATASET_NAME, DATASET_NAME, text_format)])
    #     source = pe.Node(
    #         RepositorySource(
    #             [analysis.bound_spec(DATASET_NAME).slice]),
    #         name='delayed_source')
    #     source.inputs.subject_id = self.SUBJECT
    #     source.inputs.visit_id = self.VISIT
    #     result1 = source.run()
    #     source1_path = result1.outputs.source1_path
    #     self.assertTrue(op.exists(source1_path))
    #     self.assertEqual(source1_path, target_path,
    #                      "Output file path '{}' not equal to target path '{}'"
    #                      .format(source1_path, target_path))
    #     # Clear cache to start again
    #     shutil.rmtree(cache_dir, ignore_errors=True)
    #     # Create tmp_dir before running interface, this time should wait for 1
    #     # second, check to see that the session hasn't been created and then
    #     # clear it and redownload the fileset.
    #     os.makedirs(tmp_dir)
    #     source.inputs.race_cond_delay = 1
    #     result2 = source.run()
    #     source1_path = result2.outputs.source1_path
    #     # Clear cache to start again
    #     shutil.rmtree(cache_dir, ignore_errors=True)
    #     # Create tmp_dir before running interface, this time should wait for 1
    #     # second, check to see that the session hasn't been created and then
    #     # clear it and redownload the fileset.
    #     internal_dir = op.join(tmp_dir, 'internal')
    #     deleted_tmp_dir = tmp_dir + '.deleted'

    #     def simulate_download():
    #         "Simulates a download in a separate process"
    #         os.makedirs(internal_dir)
    #         time.sleep(5)
    #         # Modify a file in the temp dir to make the source download keep
    #         # waiting
    #         logger.info('Updating simulated download directory')
    #         with open(op.join(internal_dir, 'download'), 'a') as f:
    #             f.write('downloading')
    #         time.sleep(10)
    #         # Simulate the finalising of the download by copying the previously
    #         # downloaded file into place and deleting the temp dir.
    #         logger.info('Finalising simulated download')
    #         with open(target_path, 'a') as f:
    #             f.write('simulated')
    #         shutil.move(tmp_dir, deleted_tmp_dir)

    #     source.inputs.race_cond_delay = 10
    #     p = Process(target=simulate_download)
    #     p.start()  # Start the simulated download in separate process
    #     time.sleep(1)
    #     source.run()  # Run the local download
    #     p.join()
    #     with open(op.join(deleted_tmp_dir, 'internal', 'download')) as f:
    #         d = f.read()
    #     self.assertEqual(d, 'downloading')
    #     with open(target_path) as f:
    #         d = f.read()
    #     self.assertEqual(d, 'simulated')

    @unittest.skipIf(*SKIP_ARGS)
    def test_checksums(self):
        """
        Tests check of downloaded checksums to see if file needs to be
        redownloaded
        """
        cache_dir = op.join(self.work_dir, 'cache-checksum-check')
        DATASET_NAME = 'source1'
        ANALYSIS_NAME = 'checksum_check_analysis'
        fileset_fname = DATASET_NAME + text_format.extension
        shutil.rmtree(cache_dir, ignore_errors=True)
        os.makedirs(cache_dir)
        repository = XnatRepo(
            server=SERVER, cache_dir=cache_dir)
        source_dataset = repository.dataset(self.project)
        source_cache_dir = self.session_cache_path(repository)
        source_target_path = op.join(source_cache_dir, 'scans', DATASET_NAME,
                                     'resources', text_format.name)
        md5_path = source_target_path + XnatRepo.MD5_SUFFIX
        source_target_fpath = op.join(source_target_path, fileset_fname)
        sink_dataset = repository.dataset(
            self.checksum_sink_project, subject_ids=['SUBJECT'],
            visit_ids=['VISIT'], fill_tree=True)
        analysis = DummyAnalysis(
            ANALYSIS_NAME,
            dataset=sink_dataset,
            processor=SingleProc('ad'),
            inputs=[FilesetFilter(DATASET_NAME, DATASET_NAME, text_format,
                                  dataset=source_dataset)])
        source = pe.Node(
            RepositorySource(
                [analysis.bound_spec(DATASET_NAME).slice]),
            name='checksum_check_source')
        source.inputs.subject_id = self.SUBJECT
        source.inputs.visit_id = self.VISIT
        source.run()
        self.assertTrue(op.exists(md5_path))
        self.assertTrue(op.exists(source_target_fpath))
        with open(md5_path) as f:
            checksums = json.load(f)
        # Stash the downloaded file in a new location and create a dummy
        # file instead
        stash_path = source_target_path + '.stash'
        shutil.move(source_target_path, stash_path)
        os.mkdir(source_target_path)
        with open(source_target_fpath, 'w') as f:
            f.write('dummy')
        # Run the download, which shouldn't download as the checksums are the
        # same
        source.run()
        with open(source_target_fpath) as f:
            d = f.read()
        self.assertEqual(d, 'dummy')
        # Replace the checksum with a dummy
        os.remove(md5_path)
        checksums['.'] = 'dummy_checksum'
        with open(md5_path, 'w', **JSON_ENCODING) as f:
            json.dump(checksums, f, indent=2)
        # Retry the download, which should now download since the checksums
        # differ
        source.run()
        with open(source_target_fpath) as f:
            d = f.read()
        with open(op.join(stash_path, fileset_fname)) as f:
            e = f.read()
        self.assertEqual(d, e)
        # Resink the source file and check that the generated MD5 checksum is
        # stored in identical format
        DATASET_NAME = 'sink1'
        dummy_pipeline = analysis.dummy_pipeline()
        dummy_pipeline.cap()
        sink = pe.Node(
            RepositorySink(
                [analysis.bound_spec(DATASET_NAME).slice],
                dummy_pipeline),
            name='checksum_check_sink')
        sink.inputs.name = 'checksum_check_sink'
        sink.inputs.desc = "Tests the generation of MD5 checksums"
        sink.inputs.subject_id = self.SUBJECT
        sink.inputs.visit_id = self.VISIT
        sink.inputs.sink1_path = source_target_fpath
        sink_cache_dir = self.session_cache_path(
            repository, project=self.checksum_sink_project)
        sink_target_path = '{}/resources/{}'.format(
            sink_cache_dir,
            repository.prepend_analysis(DATASET_NAME, ANALYSIS_NAME))
        sink_md5_path = sink_target_path + XnatRepo.MD5_SUFFIX
        sink.run()
        with open(md5_path) as f:
            source_checksums = json.load(f)
        with open(sink_md5_path) as f:
            sink_checksums = json.load(f)
        self.assertEqual(
            source_checksums, sink_checksums,
            ("Source checksum ({}) did not equal sink checksum ({})"
             .format(source_checksums, sink_checksums)))


class TestXnatSummarySourceAndSink(TestXnatSourceAndSinkBase):


    SUBJECT = 'SUBJECT'
    VISIT = 'VISIT'

    @unittest.skipIf(*SKIP_ARGS)
    def test_summary(self):
        # Create working dirs
        # Create XnatSource node
        repository = XnatRepo(
            server=SERVER, cache_dir=self.cache_dir)
        analysis = DummyAnalysis(
            self.SUMMARY_ANALYSIS,
            repository.dataset(self.project),
            SingleProc('ad'),
            inputs=[
                FilesetFilter('source1', 'source1', text_format),
                FilesetFilter('source2', 'source2', text_format),
                FilesetFilter('source3', 'source3', text_format)])
        # TODO: Should test out other file formats as well.
        source_files = ['source1', 'source2', 'source3']
        inputnode = pe.Node(IdentityInterface(['subject_id', 'visit_id']),
                            'inputnode')
        inputnode.inputs.subject_id = self.SUBJECT
        inputnode.inputs.visit_id = self.VISIT
        source = pe.Node(
            RepositorySource(
                [analysis.bound_spec(f).slice for f in source_files]),
            name='source')
        subject_sink_files = ['subject_sink']
        dummy_pipeline = analysis.dummy_pipeline()
        dummy_pipeline.cap()
        subject_sink = pe.Node(
            RepositorySink(
                [analysis.bound_spec(f).slice for f in subject_sink_files],
                dummy_pipeline),
            name='subject_sink')
        subject_sink.inputs.name = 'subject_summary'
        subject_sink.inputs.desc = (
            "Tests the sinking of subject-wide filesets")
        # Test visit sink
        visit_sink_files = ['visit_sink']
        visit_sink = pe.Node(
            RepositorySink(
                [analysis.bound_spec(f).slice for f in visit_sink_files],
                dummy_pipeline),
            name='visit_sink')
        visit_sink.inputs.name = 'visit_summary'
        visit_sink.inputs.desc = (
            "Tests the sinking of visit-wide filesets")
        # Test project sink
        dataset_sink_files = ['dataset_sink']
        dataset_sink = pe.Node(
            RepositorySink(
                [analysis.bound_spec(f).slice for f in dataset_sink_files],
                dummy_pipeline),
            name='dataset_sink')
        dataset_sink.inputs.name = 'project_summary'
        dataset_sink.inputs.desc = (
            "Tests the sinking of project-wide filesets")
        # Create workflow connecting them together
        workflow = pe.Workflow('summary_unittest',
                               base_dir=self.work_dir)
        workflow.add_nodes((source, subject_sink, visit_sink,
                            dataset_sink))
        workflow.connect(inputnode, 'subject_id', source, 'subject_id')
        workflow.connect(inputnode, 'visit_id', source, 'visit_id')
        workflow.connect(inputnode, 'subject_id', subject_sink, 'subject_id')
        workflow.connect(inputnode, 'visit_id', visit_sink, 'visit_id')
        workflow.connect(
            source, 'source1' + PATH_SUFFIX,
            subject_sink, 'subject_sink' + PATH_SUFFIX)
        workflow.connect(
            source, 'source2' + PATH_SUFFIX,
            visit_sink, 'visit_sink' + PATH_SUFFIX)
        workflow.connect(
            source, 'source3' + PATH_SUFFIX,
            dataset_sink, 'dataset_sink' + PATH_SUFFIX)
        workflow.run()
        analysis.clear_caches()  # Refreshed cached repository tree object
        with self._connect() as login:
            # Check subject summary directories were created properly in cache
            subject_dir = op.join(
                self.subject_cache_path(repository), 'resources')
            project_dir = op.join(
                self.project_cache_path(repository), 'resources')
            xproject = login.projects[self.project]
            xsubject = xproject.subjects[self.subject_label()]
            # Check subject resources in cache
            self.assertEqual(filter_resources(os.listdir(subject_dir)),
                             add_metadata_resources(subject_sink_files,
                                                    md5=True))
            # and on XNAT
            self.assertEqual(filter_resources(xsubject.resources),
                             add_metadata_resources(subject_sink_files))
            # Check visit summary directories were created properly in
            # cache
            self.assertEqual(filter_resources(os.listdir(project_dir),
                                              visit=self.VISIT),
                             add_metadata_resources(visit_sink_files,
                                                    md5=True))
            # and on XNAT
            self.assertEqual(filter_resources(xproject.resources,
                                              visit=self.VISIT),
                             add_metadata_resources(visit_sink_files))
            # Check dataset summary directories were created properly in cache
            self.assertEqual(filter_resources(os.listdir(project_dir)),
                             add_metadata_resources(dataset_sink_files,
                                                    md5=True))
            # and on XNAT
            self.assertEqual(filter_resources(xproject.resources),
                             add_metadata_resources(dataset_sink_files))
        # Reload the data from the summary directories
        reloadinputnode = pe.Node(IdentityInterface(['subject_id',
                                                     'visit_id']),
                                  'reload_inputnode')
        reloadinputnode.inputs.subject_id = self.SUBJECT
        reloadinputnode.inputs.visit_id = self.VISIT
        reloadsource_per_subject = pe.Node(
            RepositorySource(
                analysis.bound_spec(f).slice for f in subject_sink_files),
            name='reload_source_per_subject')
        reloadsource_per_visit = pe.Node(
            RepositorySource(
                analysis.bound_spec(f).slice for f in visit_sink_files),
            name='reload_source_per_visit')
        reloadsource_per_dataset = pe.Node(
            RepositorySource(
                analysis.bound_spec(f).slice for f in dataset_sink_files),
            name='reload_source_per_dataset')
        resink_filesets = ['resink1', 'resink2', 'resink3']
        reloadsink = pe.Node(
            RepositorySink(
                (analysis.bound_spec(f).slice
                 for f in resink_filesets),
                dummy_pipeline),
            name='reload_sink')
        reloadsink.inputs.name = 'reload_summary'
        reloadsink.inputs.desc = (
            "Tests the reloading of subject and project summary filesets")
        reloadworkflow = pe.Workflow('reload_summary_unittest',
                                     base_dir=self.work_dir)
        for node in (reloadsource_per_subject, reloadsource_per_visit,
                     reloadsource_per_dataset, reloadsink):
            for iterator in ('subject_id', 'visit_id'):
                reloadworkflow.connect(reloadinputnode, iterator,
                                       node, iterator)
        reloadworkflow.connect(reloadsource_per_subject,
                               'subject_sink' + PATH_SUFFIX,
                               reloadsink,
                               'resink1' + PATH_SUFFIX)
        reloadworkflow.connect(reloadsource_per_visit,
                               'visit_sink' + PATH_SUFFIX,
                               reloadsink,
                               'resink2' + PATH_SUFFIX)
        reloadworkflow.connect(reloadsource_per_dataset,
                               'dataset_sink' + PATH_SUFFIX,
                               reloadsink,
                               'resink3' + PATH_SUFFIX)
        reloadworkflow.run()
        # Check that the filesets
        session_dir = op.join(
            self.session_cache_path(repository), 'resources')
        self.assertEqual(
            filter_resources(os.listdir(session_dir)),
            add_metadata_resources(resink_filesets, md5=True))
        # and on XNAT
        with self._connect() as login:
            xsession = login.create_object(self.session_uri())
            self.assertEqual(filter_resources(xsession.resources.keys()),
                             add_metadata_resources(resink_filesets))
