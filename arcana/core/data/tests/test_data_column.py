from operator import mul
from functools import reduce
from arcana.core.data.type.base import FileGroup


def test_column_api_access(dataset):

    bp = dataset.__annotations__["blueprint"]

    for col_name, formats in bp.expected_formats.items():
        data_format, files = formats[0]

        dataset.add_source(col_name, data_format)

        col = dataset[col_name]

        # Check length of column
        assert len(col) == reduce(mul, bp.dim_lengths)

        # Access file-group via leaf IDs
        for id_ in col.ids:
            item = col[id_]
            assert isinstance(item, data_format)
            if issubclass(data_format, FileGroup):
                assert sorted(p.name for p in item.fs_paths) == sorted(files)
