import os.path
import tempfile
import shutil
import filecmp
import pytest
from arcana.tasks.common.archive import create_tar, extract_tar, create_zip, extract_zip


TEST_DIR = "__test__"


@pytest.fixture(scope="module")
def base_dir():
    base_dir = tempfile.mkdtemp()
    yield base_dir
    shutil.rmtree(base_dir)


@pytest.fixture(scope="module")
def test_dir(base_dir):
    test_dir = os.path.join(base_dir, TEST_DIR)
    os.mkdir(test_dir)
    for i in range(1, 3):
        with open(os.path.join(test_dir, f"file{i}.txt"), "w") as f:
            f.write(f"test file {i}")
    sub_dir = os.path.join(test_dir, "sub-dir")
    os.mkdir(sub_dir)
    for i in range(4, 6):
        with open(os.path.join(sub_dir, f"file{i}.txt"), "w") as f:
            f.write(f"test file {i}")
    return test_dir


def test_tar_roundtrip(base_dir, test_dir):
    extract_dir = os.path.join(base_dir, "tar-extract")
    tar_file = f"{base_dir}/out.tar.gz"
    os.mkdir(extract_dir)
    create_tar_task = create_tar(
        in_file=test_dir, out_file=tar_file, base_dir=base_dir, compression="gz"
    )

    extract_tar_task = extract_tar(in_file=tar_file, extract_dir=extract_dir)

    create_tar_task()
    extract_tar_task()

    _assert_extracted_dir_matches(extract_dir, test_dir)


def test_zip_roundtrip(base_dir, test_dir):
    extract_dir = os.path.join(base_dir, "zip-extract")
    zip_file = f"{base_dir}/out.zip"
    os.mkdir(extract_dir)
    create_zip_task = create_zip(in_file=test_dir, out_file=zip_file, base_dir=base_dir)

    extract_zip_task = extract_zip(in_file=zip_file, extract_dir=extract_dir)

    create_zip_task()
    extract_zip_task()

    _assert_extracted_dir_matches(extract_dir, test_dir)


def _assert_extracted_dir_matches(extract_dir, test_dir):
    extract_test_dir = os.path.join(extract_dir, TEST_DIR)

    def assert_exact_match(cmp):
        assert (
            not cmp.left_only
        ), f"{cmp.left_only} missing from unarchved dir {cmp.right}"
        assert not cmp.right_only, (
            f"Additional {cmp.right_only} found in unarchived dir " + cmp.right
        )
        for subdir in cmp.subdirs.values():
            assert_exact_match(subdir)

    assert_exact_match(filecmp.dircmp(test_dir, extract_test_dir))
