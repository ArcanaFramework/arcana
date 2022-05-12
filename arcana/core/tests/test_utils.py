from arcana.core.utils import package_from_module

def test_package_from_module():
    assert package_from_module('arcana.data.stores').key == 'arcana'
    assert package_from_module('pydra.tasks.dcm2niix').key == 'pydra-dcm2niix'
    assert package_from_module('pydra.engine').key == 'pydra'
