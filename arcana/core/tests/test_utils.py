from arcana.core.utils import pkg_from_module

def test_pkg_from_module():
    assert pkg_from_module('arcana.data.stores').key == 'arcana'
    assert pkg_from_module('pydra.tasks.dcm2niix').key == 'pydra-dcm2niix'
    assert pkg_from_module('pydra.engine').key == 'pydra'
