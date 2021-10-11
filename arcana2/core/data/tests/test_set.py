import os.path
from itertools import repeat
import tempfile
import pytest
import cloudpickle as cp
from arcana2.core.data.spec import DataSource, DataSink
from arcana2.repositories.file_system import FileSystem
from arcana2.dimensions.clinical import Clinical as cl
from arcana2.datatypes.neuroimaging import dicom, niftix_gz

    
# def test_nodes(dataset, test_dicom_dataset_dir):
#     session_ids = [d for d in os.listdir(test_dicom_dataset_dir)
#                    if not d.startswith('.')]
#     singular_id = [None]
#     assert list(dataset.node_ids(cl.session)) == session_ids
#     assert list(dataset.node_ids(Clinical.member)) == session_ids
#     assert list(dataset.node_ids(Clinical.subject)) == session_ids
#     assert list(dataset.node_ids(Clinical.matchedpoint)) == session_ids    
#     assert list(dataset.node_ids(Clinical.timepoint)) == singular_id
#     assert list(dataset.node_ids(Clinical.group)) == singular_id
#     assert list(dataset.node_ids(Clinical.batch)) == singular_id
#     assert list(dataset.node_ids(Clinical.dataset)) == singular_id


# def test_dicom_dataset_pickle(dataset):
#     with tempfile.TemporaryFile(mode='w+b') as f:
#         cp.dump(dataset, f)
#     assert True


# def test_workflow_pickle(dataset, inputs, outputs):
#     workflow = dataset.workflow('test', inputs, outputs)
#     workflow.pickle_task()
#     assert True