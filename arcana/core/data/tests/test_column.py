import pytest
from operator import mul
from functools import reduce
from arcana.core.data.format import FileGroup

@pytest.mark.skip("needs to wait until further refactoring")
def test_column_api_access(dataset):

    bp = dataset.blueprint

    for col_name, (data_format, files) in bp.expected_formats.items():

        col = dataset[col_name]

        # Check length of column
        assert len(col) == reduce(mul, bp.dim_lengths)

        # Access file-group via leaf IDs
        for id_ in col.ids:
            item = col[id_]
            assert isinstance(item, data_format)
            if issubclass(data_format, FileGroup):
                assert item.paths == files

        # Access file-groups via basis IDs
        for id_tuple in col.id_tuples:
            item = col[id_tuple]
            assert isinstance(item, data_format)
            if issubclass(data_format, FileGroup):
                assert item.paths == files