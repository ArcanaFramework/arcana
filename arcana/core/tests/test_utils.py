from arcana.core.utils import package_from_module, path2varname, varname2path

def test_package_from_module():
    assert package_from_module('arcana.data.stores').key == 'arcana'
    assert package_from_module('pydra.tasks.dcm2niix').key == 'pydra-dcm2niix'
    assert package_from_module('pydra.engine').key == 'pydra'


def test_path2varname():
    escape_pairs = [('func/task-rest_bold', 'func__l__task__H__rest_bold'),
                    ('__a.very$illy*ath~', 'XXX_dunder_a__o__very__dollar__illy__star__ath__tilde__'),
                    ('anat/T1w', 'anat__l__T1w'),
                    ('anat__l__T1w', 'anat_dunder_l_dunder_T1w')]

    for path, varname in escape_pairs:
        assert path2varname(path) == varname
        assert varname2path(varname) == path
