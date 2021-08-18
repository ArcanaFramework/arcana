import os.path
import tempfile
import shutil
import filecmp
from unittest import TestCase
from arcana2.tasks.archive import (
    create_tar, extract_tar, create_zip, extract_zip)


class TestArchive(TestCase):

    TEST_DIR = 'test'

    def setUp(self) -> None:
        self.base_dir = tempfile.mkdtemp()
        self.test_dir = os.path.join(self.base_dir, self.TEST_DIR)
        os.mkdir(self.test_dir)
        for i in range(1, 3):
            with open(os.path.join(self.test_dir, f"file{i}.txt"), 'w') as f:
                f.write(f'test file {i}')
        sub_dir = os.path.join(self.test_dir, 'sub-dir')
        os.mkdir(sub_dir)
        for i in range(4, 6):
            with open(os.path.join(sub_dir, f"file{i}.txt"), 'w') as f:
                f.write(f'test file {i}')
        return super().setUp()

    def tearDown(self) -> None:
        shutil.rmtree(self.base_dir)
        return super().tearDown()

    def test_tar_roundtrip(self):
        extract_dir = os.path.join(self.base_dir, 'tar-extract')
        tar_file = f'{self.base_dir}/out.tar.gz'
        os.mkdir(extract_dir)
        create_tar_task = create_tar(
            in_file=self.test_dir,
            out_file=tar_file,
            base_dir=self.base_dir,
            compression='gz')

        extract_tar_task = extract_tar(
            in_file=tar_file,
            extract_dir=extract_dir)

        create_tar_task()
        extract_tar_task()

        self._assert_extracted_dir_matches(extract_dir)

    def test_zip_roundtrip(self):
        extract_dir = os.path.join(self.base_dir, 'zip-extract')
        zip_file = f'{self.base_dir}/out.zip'
        os.mkdir(extract_dir)
        create_zip_task = create_zip(
            in_file=self.test_dir,
            out_file=zip_file,
            base_dir=self.base_dir)

        extract_zip_task = extract_zip(
            in_file=zip_file,
            extract_dir=extract_dir)

        create_zip_task()
        extract_zip_task()

        self._assert_extracted_dir_matches(extract_dir)


    def _assert_extracted_dir_matches(self, extract_dir):
        extract_test_dir = os.path.join(extract_dir, self.TEST_DIR)
        def assert_exact_match(cmp):
            self.assertFalse(
                cmp.left_only,
                f"{cmp.left_only} missing from unarchved dir {cmp.right}")
            self.assertFalse(
                cmp.right_only,
                f"Additional {cmp.right_only} found in unarchived dir "
                + cmp.right)
            for subdir in cmp.subdirs.values():
                assert_exact_match(subdir)
        assert_exact_match(filecmp.dircmp(self.test_dir, extract_test_dir))
