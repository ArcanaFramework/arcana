from operator import mul
from functools import reduce
from fileformats.core import FileSet
from arcana.core.data.set.base import Dataset


def test_column_api_access(dataset: Dataset):

    bp = dataset.__annotations__["blueprint"]

    for fileset_bp in bp.entries:

        dataset.add_source(fileset_bp.path, fileset_bp.datatype)

        col = dataset[fileset_bp.path]

        # Check length of column
        assert len(col) == reduce(mul, bp.dim_lengths)

        # Access file-set via leaf IDs
        with dataset.tree:
            for id_ in col.ids:
                item = col[id_]
                assert isinstance(item, fileset_bp.datatype)
                if issubclass(fileset_bp.datatype, FileSet):
                    assert sorted(p.name for p in item.fspaths) == sorted(
                        fileset_bp.filenames
                    )
