import os
import pytest
from pathlib import Path
from tempfile import mkdtemp
import shutil
from arcana2.core.data.spec import DataSource, DataSink
from arcana2.repositories import FileSystem, Xnat
from arcana2.dataspaces.clinical import Clinical
from arcana2.datatypes import dicom, niftix_gz



@pytest.fixture
def work_dir():
    work_dir = mkdtemp()
    yield Path(work_dir)
    shutil.rmtree(work_dir)