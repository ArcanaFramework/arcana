from arcana.core.utils import resolve_pkg_of_module

def test_resolve_pkg_of_module():
    assert resolve_pkg_of_module('arcana.data.stores').key == 'arcana'
    assert resolve_pkg_of_module('pydra.tasks.dcm2niix').key == 'pydra-dcm2niix'
    assert resolve_pkg_of_module('pydra.engine').key == 'pydra'
