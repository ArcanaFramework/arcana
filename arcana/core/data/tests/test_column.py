from operator import mul
from functools import reduce

def test_column_access(dataset):

    bp = dataset.blueprint

    for col_name, (file_format, files) in bp.expected_formats.items():

        col = dataset[col_name]

        assert len(col) == reduce(mul, bp.dim_lengths)

        for id_ in col.ids:
            file_group = col[id_]
            assert isinstance(file_group, file_format)
            assert file_group.paths == files
