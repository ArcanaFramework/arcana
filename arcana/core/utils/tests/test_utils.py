from arcana.core.utils.packaging import package_from_module
from arcana.core.utils.misc import path2varname, varname2path


def test_package_from_module():
    assert package_from_module("arcana.dirtree").key == "arcana-dirtree"
    assert package_from_module("pydra.tasks.dcm2niix").key == "pydra-dcm2niix"
    assert package_from_module("pydra.engine").key == "pydra"


def test_path2varname():
    escape_pairs = [
        ("dwi/dir-LR_dwi", "dwi__l__dir__H__LR_u_dwi"),
        ("func/task-rest_bold", "func__l__task__H__rest_u_bold"),
        (
            "with spaces and__ underscores",
            "with__s__spaces__s__and_u__u___s__underscores",
        ),
        ("__a.very$illy*ath~", "XXX_u__u_a__o__very__dollar__illy__star__ath__tilde__"),
        ("anat/T1w", "anat__l__T1w"),
        ("anat__l__T1w", "anat_u__u_l_u__u_T1w"),
        ("_u__u_", "XXX_u_u_u__u_u_u_"),
    ]
    for path, varname in escape_pairs:
        assert path2varname(path) == varname
        assert varname2path(varname) == path
        assert varname2path(varname2path(path2varname(path2varname(path)))) == path
