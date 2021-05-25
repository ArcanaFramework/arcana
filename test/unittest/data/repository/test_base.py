import os
from nipype.pipeline import engine as pe
from nipype.interfaces.utility import IdentityInterface
from arcana.data.file_format import text_format
from arcana.processor import SingleProc
from arcana.data import (
    FilesetFilter, FieldSpec)
from arcana.utils import PATH_SUFFIX
from future.utils import with_metaclass
from arcana.utils.testing import BaseTestCase
from arcana.data import InputFilesetSpec, FilesetSpec
from arcana.analysis import Analysis, AnalysisMetaClass
from arcana.repository.interfaces import RepositorySource, RepositorySink
from arcana.repository import LocalFileSystemRepo


class DummyAnalysis(with_metaclass(AnalysisMetaClass, Analysis)):

    add_data_specs = [
        InputFilesetSpec('source1', text_format, optional=True),
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
        FilesetSpec('analysis_sink', text_format, 'dummy_pipeline',
                    frequency='per_dataset'),
        FilesetSpec('resink1', text_format, 'dummy_pipeline'),
        FilesetSpec('resink2', text_format, 'dummy_pipeline'),
        FilesetSpec('resink3', text_format, 'dummy_pipeline'),
        FieldSpec('field1', int, 'dummy_pipeline'),
        FieldSpec('field2', float, 'dummy_pipeline'),
        FieldSpec('field3', str, 'dummy_pipeline')]

    def dummy_pipeline(self, **name_maps):
        return self.new_pipeline('dummy', name_maps=name_maps)


class TestSinkAndSource(BaseTestCase):

    ANALYSIS_NAME = 'aanalysis'
    SUMMARY_ANALYSIS = 'asummary'
    INPUT_FILESETS = {'source1': '1',
                      'source2': '2',
                      'source3': '3',
                      'source4': '4'}

    def test_repository_roundtrip(self):
        analysis = DummyAnalysis(
            self.ANALYSIS_NAME, self.dataset, processor=SingleProc('a_dir'),
            inputs=[FilesetFilter('source1', 'source1', text_format),
                    FilesetFilter('source2', 'source2', text_format),
                    FilesetFilter('source3', 'source3', text_format),
                    FilesetFilter('source4', 'source4', text_format)])
        # TODO: Should test out other file formats as well.
        source_files = ('source1', 'source2', 'source3', 'source4')
        sink_files = ('sink1', 'sink3', 'sink4')
        inputnode = pe.Node(IdentityInterface(['subject_id', 'visit_id']),
                            'inputnode')
        inputnode.inputs.subject_id = self.SUBJECT
        inputnode.inputs.visit_id = self.VISIT
        source = pe.Node(
            RepositorySource(
                analysis.bound_spec(f).slice
                for f in source_files),
            name='source')
        dummy_pipeline = analysis.dummy_pipeline()
        dummy_pipeline.cap()
        sink = pe.Node(
            RepositorySink(
                (analysis.bound_spec(f).slice for f in sink_files),
                dummy_pipeline),
            name='sink')
        sink.inputs.name = 'repository_sink'
        sink.inputs.desc = (
            "A test session created by repository roundtrip unittest")
        # Create workflow connecting them together
        workflow = pe.Workflow('source_sink_unit_test', base_dir=self.work_dir)
        workflow.add_nodes((source, sink))
        workflow.connect(inputnode, 'subject_id', source, 'subject_id')
        workflow.connect(inputnode, 'visit_id', source, 'visit_id')
        workflow.connect(inputnode, 'subject_id', sink, 'subject_id')
        workflow.connect(inputnode, 'visit_id', sink, 'visit_id')
        for source_name in source_files:
            if not source_name.endswith('2'):
                sink_name = source_name.replace('source', 'sink')
                workflow.connect(
                    source, source_name + PATH_SUFFIX,
                    sink, sink_name + PATH_SUFFIX)
        workflow.run()
        # Check local directory was created properly
        outputs = [
            f for f in sorted(os.listdir(
                self.get_session_dir(from_analysis=self.ANALYSIS_NAME)))
            if f not in (LocalFileSystemRepo.FIELDS_FNAME,
                         LocalFileSystemRepo.PROV_DIR)]
        self.assertEqual(outputs, ['sink1.txt', 'sink3.txt', 'sink4.txt'])

    def test_fields_roundtrip(self):
        ANALYSIS_NAME = 'fields_roundtrip'
        analysis = DummyAnalysis(
            ANALYSIS_NAME, self.dataset,
            processor=SingleProc('a_dir'),
            inputs=[])
        dummy_pipeline = analysis.dummy_pipeline()
        dummy_pipeline.cap()
        sink = pe.Node(
            RepositorySink(
                (analysis.bound_spec(f).slice
                 for f in ['field1', 'field2', 'field3']), dummy_pipeline),
            name='fields_sink')
        sink.inputs.field1_field = field1 = 1
        sink.inputs.field2_field = field2 = 2.0
        sink.inputs.field3_field = field3 = '3'
        sink.inputs.subject_id = self.SUBJECT
        sink.inputs.visit_id = self.VISIT
        sink.inputs.desc = "Test sink of fields"
        sink.inputs.name = 'test_sink'
        sink.run()
        source = pe.Node(
            RepositorySource(
                analysis.bound_spec(f).slice
                for f in ['field1', 'field2', 'field3']),
            name='fields_source')
        source.inputs.visit_id = self.VISIT
        source.inputs.subject_id = self.SUBJECT
        source.inputs.desc = "Test source of fields"
        source.inputs.name = 'test_source'
        results = source.run()
        self.assertEqual(results.outputs.field1_field, field1)
        self.assertEqual(results.outputs.field2_field, field2)
        self.assertEqual(results.outputs.field3_field, field3)

    def test_summary(self):
        analysis = DummyAnalysis(
            self.SUMMARY_ANALYSIS, self.dataset, SingleProc('ad'),
            inputs=[FilesetFilter('source1', 'source1', text_format),
                    FilesetFilter('source2', 'source2', text_format),
                    FilesetFilter('source3', 'source3', text_format)])
        # TODO: Should test out other file formats as well.
        source_files = ['source1', 'source2', 'source3']
        inputnode = pe.Node(
            IdentityInterface(['subject_id', 'visit_id']), 'inputnode')
        inputnode.inputs.subject_id = self.SUBJECT
        inputnode.inputs.visit_id = self.VISIT
        source = pe.Node(
            RepositorySource(analysis.bound_spec(f).slice
                             for f in source_files), name='source')
        # Test subject sink
        subject_sink_files = ['subject_sink']
        dummy_pipeline = analysis.dummy_pipeline()
        dummy_pipeline.cap()
        subject_sink = pe.Node(
            RepositorySink(
                (analysis.bound_spec(f).slice for f in subject_sink_files),
                dummy_pipeline),
            name='subject_sink')
        subject_sink.inputs.name = 'subject_summary'
        subject_sink.inputs.desc = (
            "Tests the sinking of subject-wide filesets")
        # Test visit sink
        visit_sink_files = ['visit_sink']
        visit_sink = pe.Node(
            RepositorySink(
                (analysis.bound_spec(f).slice
                 for f in visit_sink_files),
                dummy_pipeline),
            name='visit_sink')
        visit_sink.inputs.name = 'visit_summary'
        visit_sink.inputs.desc = (
            "Tests the sinking of visit-wide filesets")
        # Test project sink
        analysis_sink_files = ['analysis_sink']
        analysis_sink = pe.Node(
            RepositorySink(
                (analysis.bound_spec(f).slice for f in analysis_sink_files),
                dummy_pipeline),
            name='analysis_sink')

        analysis_sink.inputs.name = 'project_summary'
        analysis_sink.inputs.desc = (
            "Tests the sinking of project-wide filesets")
        # Create workflow connecting them together
        workflow = pe.Workflow('summary_unittest', base_dir=self.work_dir)
        workflow.add_nodes((source, subject_sink, visit_sink,
                            analysis_sink))
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
            analysis_sink, 'analysis_sink' + PATH_SUFFIX)
        workflow.run()
        # Check local summary directories were created properly
        subject_dir = self.get_session_dir(
            frequency='per_subject',
            from_analysis=self.SUMMARY_ANALYSIS)
        self.assertEqual(sorted(os.listdir(subject_dir)),
                         [LocalFileSystemRepo.PROV_DIR, 'subject_sink.txt'])
        visit_dir = self.get_session_dir(
            frequency='per_visit',
            from_analysis=self.SUMMARY_ANALYSIS)
        self.assertEqual(sorted(os.listdir(visit_dir)),
                         [LocalFileSystemRepo.PROV_DIR, 'visit_sink.txt'])
        project_dir = self.get_session_dir(
            frequency='per_dataset',
            from_analysis=self.SUMMARY_ANALYSIS)
        self.assertEqual(sorted(os.listdir(project_dir)),
                         [LocalFileSystemRepo.PROV_DIR, 'analysis_sink.txt'])
        # Reload the data from the summary directories
        reloadinputnode = pe.Node(
            IdentityInterface(['subject_id', 'visit_id']),
            name='reload_inputnode')
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
                analysis.bound_spec(f).slice for f in analysis_sink_files),
            name='reload_source_per_dataset')
        reloadsink = pe.Node(
            RepositorySink(
                (analysis.bound_spec(f).slice
                 for f in ['resink1', 'resink2', 'resink3']),
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
                               'analysis_sink' + PATH_SUFFIX,
                               reloadsink,
                               'resink3' + PATH_SUFFIX)
        reloadworkflow.run()
        outputs = [
            f for f in sorted(os.listdir(
                self.get_session_dir(from_analysis=self.SUMMARY_ANALYSIS)))
            if f not in (LocalFileSystemRepo.FIELDS_FNAME,
                         LocalFileSystemRepo.PROV_DIR)]
        self.assertEqual(outputs,
                         ['resink1.txt', 'resink2.txt', 'resink3.txt'])
