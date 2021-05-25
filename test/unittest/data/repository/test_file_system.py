import os
import os.path as op
from arcana2.data import (
    FileGroup, FileGroupSpec, Field)
from arcana2.utils.testing import BaseMultiSubjectTestCase
from arcana2.data import Tree
from future.utils import with_metaclass
from arcana2.utils.testing import BaseTestCase
from arcana2.data.file_format import FileFormat, text_format


# A dummy format that contains a header
with_header_format = FileFormat(name='with_header', extension='.whf',
                                aux_files={'header': '.hdr'})


class TestLocalFileSystemRepo(BaseTestCase):

    ANALYSIS_NAME = 'local_repo'
    INPUT_FILESETS = {'source1': '1',
                      'source2': '2',
                      'source3': '3',
                      'source4': '4'}

    def test_get_fileset(self):
        pass


class TestDirectoryProjectInfo(BaseMultiSubjectTestCase):
    """
    This unittest tests out that extracting the existing scans and
    fields in a project returned in a Tree object.
    """

    DATASET_CONTENTS = {'ones': 1, 'tens': 10, 'hundreds': 100,
                        'thousands': 1000, 'with_header': {'.': 'main',
                                                           'header': 'header'}}
    ANALYSIS_NAME = 'derived'

    def get_tree(self, dataset, sync_with_repo=False):
        fileset_kwargs = {'resource_name': 'text',
                          'dataset': dataset}
        derived_fileset_kwargs = {'resource_name': 'text',
                                  'dataset': dataset,
                                  'from_analysis': self.ANALYSIS_NAME}
        field_kwargs = {'dataset': dataset}
        derived_field_kwargs = {'dataset': dataset,
                                'from_analysis': self.ANALYSIS_NAME}
        filesets = [
            # Subject1
            FileGroup('ones', text_format,
                    frequency='per_subject',
                    subject_id='subject1',
                    **fileset_kwargs),
            FileGroup('tens', text_format,
                    frequency='per_subject',
                    subject_id='subject1',
                    **derived_fileset_kwargs),
            # subject1/visit1
            FileGroup('hundreds', text_format,
                    subject_id='subject1', visit_id='visit1',
                    **derived_fileset_kwargs),
            FileGroup('ones', text_format,
                    subject_id='subject1', visit_id='visit1',
                    **fileset_kwargs),
            FileGroup('tens', text_format,
                    subject_id='subject1', visit_id='visit1',
                    **derived_fileset_kwargs),
            FileGroup('with_header', text_format,
                    frequency='per_session',
                    subject_id='subject1', visit_id='visit1',
                    **derived_fileset_kwargs),
            # subject1/visit2
            FileGroup('ones', text_format,
                    subject_id='subject1', visit_id='visit2',
                    **fileset_kwargs),
            FileGroup('tens', text_format,
                    subject_id='subject1', visit_id='visit2',
                    **derived_fileset_kwargs),
            # Subject 2
            FileGroup('ones', text_format,
                    frequency='per_subject',
                    subject_id='subject2',
                    **fileset_kwargs),
            FileGroup('tens', text_format,
                    frequency='per_subject',
                    subject_id='subject2',
                    **derived_fileset_kwargs),
            # subject2/visit1
            FileGroup('ones', text_format,
                    subject_id='subject2', visit_id='visit1',
                    **fileset_kwargs),
            FileGroup('tens', text_format,
                    subject_id='subject2', visit_id='visit1',
                    **derived_fileset_kwargs),
            # subject2/visit2
            FileGroup('ones', text_format,
                    subject_id='subject2', visit_id='visit2',
                    **fileset_kwargs),
            FileGroup('tens', text_format,
                    subject_id='subject2', visit_id='visit2',
                    **derived_fileset_kwargs),
            # Visit 1
            FileGroup('ones', text_format,
                    frequency='per_visit',
                    visit_id='visit1',
                    **fileset_kwargs),
            # Analysis
            FileGroup('ones', text_format,
                    frequency='per_dataset',
                    **fileset_kwargs)]
        fields = [
            # Subject 2
            Field('e', value=3.33333,
                  frequency='per_subject',
                  subject_id='subject2',
                  **field_kwargs),
            # subject2/visit2
            Field('a', value=22,
                  subject_id='subject2', visit_id='visit2',
                  **derived_field_kwargs),
            Field('b', value=220,
                  subject_id='subject2', visit_id='visit2',
                  **derived_field_kwargs),
            Field('c', value='buggy',
                  subject_id='subject2', visit_id='visit2',
                  **derived_field_kwargs),
            # Subject1
            Field('e', value=4.44444,
                  frequency='per_subject',
                  subject_id='subject1',
                  **derived_field_kwargs),
            # subject1/visit1
            Field('a', value=1,
                  subject_id='subject1', visit_id='visit1',
                  **field_kwargs),
            Field('b', value=10,
                  subject_id='subject1', visit_id='visit1',
                  **derived_field_kwargs),
            Field('d', value=42.42,
                  subject_id='subject1', visit_id='visit1',
                  **derived_field_kwargs),
            # subject1/visit2
            Field('a', value=2,
                  subject_id='subject1', visit_id='visit2',
                  **derived_field_kwargs),
            Field('c', value='van',
                  subject_id='subject1', visit_id='visit2',
                  **derived_field_kwargs),
            # Visit 1
            Field('f', value='dog',
                  frequency='per_visit',
                  visit_id='visit1',
                  **derived_field_kwargs),
            # Visit 2
            Field('f', value='cat',
                  frequency='per_visit',
                  visit_id='visit2',
                  **derived_field_kwargs),
            # Analysis
            Field('g', value=100,
                  frequency='per_dataset',
                  **derived_field_kwargs)]
        # Set URI and IDs if necessary for repository type
        if sync_with_repo:
            for fileset in filesets:
                fileset.get()
            for field in fields:
                field.get()
        tree = Tree.construct(self.dataset, filesets, fields)
        return tree

    @property
    def input_tree(self):
        return self.get_tree(self.local_dataset)

    def test_project_info(self):
        # Add hidden file (i.e. starting with '.') to local dataset at
        # project and subject levels to test ignore functionality
        a_subj_dir = os.listdir(self.project_dir)[0]
        open(op.join(op.join(self.project_dir, '.DS_Store')),
             'w').close()
        open(op.join(self.project_dir, a_subj_dir, '.DS_Store'), 'w').close()
        tree = self.dataset.tree
        for node in tree.nodes():
            for fileset in node.filesets:
                fileset.format = text_format
                fileset._resource_name = 'text'
        self.assertEqual(
            tree, self.local_tree,
            "Generated project doesn't match reference:{}"
            .format(tree.find_mismatch(self.local_tree)))
