import os
from unittest.mock import patch

import pytest

from arcana.core.data.store import DataStore
from arcana.exceptions import ArcanaNameError



def test_singletons():
    assert sorted(DataStore.singletons()) == ['bids', 'file']    

def test_store_save(xnat_repository, work_dir):
    nickname1='store1'
    nickname2='store2'
    test_home_dir = work_dir / 'test-arcana-home'
    # Create a new home directory so it doesn't conflict with user settings
    with patch.dict(os.environ, {'ARCANA_HOME': str(test_home_dir)}):
        store = xnat_repository
        store.save(nickname1)
        store.save(nickname2)
        stores  = DataStore.load_saved_entries()
        assert nickname1 in stores
        assert nickname2 in stores


def test_store_save_with_no_name(xnat_repository, work_dir):
    test_home_dir = work_dir / 'test-arcana-home'
    # Create a new home directory so it doesn't conflict with user settings
    with patch.dict(os.environ, {'ARCANA_HOME': str(test_home_dir)}):
        with pytest.raises(ArcanaNameError) as excinfo:
            store = xnat_repository
            store.save("")
        assert str(excinfo.value) == "Store Name can not be empty"
