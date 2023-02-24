from operator import mul
from functools import reduce
from fileformats.core.base import FileSet
from arcana.core.data.set.base import Dataset


def test_column_api_access(dataset: Dataset):

    bp = dataset.__annotations__["blueprint"]

    for col_name, exp_datatypes in bp.expected_datatypes.items():
        exp = exp_datatypes[0]

        dataset.add_source(col_name, exp.datatype)

        col = dataset[col_name]

        # Check length of column
        assert len(col) == reduce(mul, bp.dim_lengths)

        # Access file-set via leaf IDs
        with dataset.tree:
            for id_ in col.ids:
                item = col[id_]
                assert isinstance(item, exp.datatype)
                if issubclass(exp.datatype, FileSet):
                    assert sorted(p.name for p in item.fspaths) == sorted(exp.filenames)
