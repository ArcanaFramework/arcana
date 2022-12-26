from operator import mul
from functools import reduce
from fileformats.core.base import FileGroup


def test_column_api_access(dataset):

    bp = dataset.__annotations__["blueprint"]

    for col_name, exp_datatypes in bp.expected_datatypes.items():
        exp = exp_datatypes[0]

        dataset.add_source(col_name, exp.datatype)

        col = dataset[col_name]

        # Check length of column
        assert len(col) == reduce(mul, bp.dim_lengths)

        # Access file-group via leaf IDs
        for id_ in col.ids:
            item = col[id_]
            assert isinstance(item, exp.datatype)
            if issubclass(exp.datatype, FileGroup):
                assert sorted(p.name for p in item.fs_paths) == sorted(exp.filenames)
