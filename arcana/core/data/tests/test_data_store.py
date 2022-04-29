import os
import pytest

from unittest.mock import patch
from arcana.core.data.store import DataStore
from arcana.data.stores.bids.structure import Bids
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


def test_store_save_fails_with_no_name(xnat_repository, work_dir):
    test_home_dir = work_dir / 'test-arcana-home'
    # Create a new home directory so it doesn't conflict with user settings
    with patch.dict(os.environ, {'ARCANA_HOME': str(test_home_dir)}):
        with pytest.raises(ArcanaNameError) as excinfo:
            store = xnat_repository
            store.save("")
            assert str(excinfo.value) == "Store Name can not be empty"

def test_store_load_new_stores(xnat_repository, work_dir):
    test_home_dir = work_dir / 'test-arcana-home'
    # Create a new home directory so it doesn't conflict with user settings
    with patch.dict(os.environ, {'ARCANA_HOME': str(test_home_dir)}):
        #XNAT
        xnat_store_name='XNATstore'

        xnat_repository.save(xnat_store_name)
        loaded_xnat_store = DataStore.load(xnat_store_name)
        entries = DataStore.load_saved_entries()

        assert loaded_xnat_store.alias == xnat_repository.alias
        assert loaded_xnat_store.cache_dir.name == xnat_repository.cache_dir.name
        assert loaded_xnat_store.server == xnat_repository.server
        assert len(entries) == 1

        #BIDS
        bids = Bids()
        bids_store_name='BIDSstore'
        bids.save(bids_store_name)
        loaded_bids_store = DataStore.load(bids_store_name)
        entries = DataStore.load_saved_entries()

        #TODO add more checks
        assert loaded_bids_store.alias == bids.alias
        assert len(entries) == 2
        
        #TODO test other types of stores

def test_store_load_fails_with_incorrect_name():
    non_existing_store_name = "notIn"
    with pytest.raises(ArcanaNameError) as excinfo:
            DataStore.load(non_existing_store_name)
            assert str(excinfo.value) == "No saved data store or built-in type matches '"+non_existing_store_name+"'"