import os.path
from itertools import repeat
import tempfile
import pytest
import cloudpickle as cp
from arcana2.core.data.spec import DataSource, DataSink
from arcana2.repositories.file_system import FileSystem
from arcana2.dataspaces.clinical import Clinical as cl
from arcana2.datatypes.neuroimaging import dicom, niftix_gz


def test_dataset_pickle(dataset):
   pass 