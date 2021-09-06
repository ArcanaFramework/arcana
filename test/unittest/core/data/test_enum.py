from unittest import TestCase
from arcana2.core.data.enum import Clinical

class TestDataHierarchy(TestCase):

    TEST_DIR = 'test'

    def test_infer_ids(self):
        pass

    def test_basis(self):
        pass

    def test_diff_layers(self):
        pass

    def test_operators(self):
        pass

    def test_is_child(self):
        self.assertTrue(Clinical.session.is_child(Clinical.session))
        self.assertTrue(Clinical.session.is_child(Clinical.subject))
        self.assertTrue(Clinical.session.is_child(Clinical.member))
        self.assertTrue(Clinical.session.is_child(Clinical.group))
        self.assertTrue(Clinical.session.is_child(Clinical.timepoint))
        self.assertTrue(Clinical.session.is_child(Clinical.batch))
        self.assertTrue(Clinical.session.is_child(Clinical.matched_datapoint))
        self.assertTrue(Clinical.session.is_child(Clinical.dataset))

        self.assertFalse(Clinical.subject.is_child(Clinical.session))
        self.assertFalse(Clinical.subject.is_child(Clinical.subject))
        self.assertTrue(Clinical.subject.is_child(Clinical.member))
        self.assertTrue(Clinical.subject.is_child(Clinical.group))
        self.assertFalse(Clinical.subject.is_child(Clinical.timepoint))
        self.assertFalse(Clinical.subject.is_child(Clinical.batch))
        self.assertFalse(Clinical.subject.is_child(Clinical.matched_datapoint))
        self.assertTrue(Clinical.subject.is_child(Clinical.dataset))

        self.assertFalse(Clinical.group.is_child(Clinical.session))
        self.assertFalse(Clinical.group.is_child(Clinical.subject))
        self.assertFalse(Clinical.group.is_child(Clinical.member))
        self.assertFalse(Clinical.group.is_child(Clinical.group))
        self.assertFalse(Clinical.group.is_child(Clinical.timepoint))
        self.assertFalse(Clinical.group.is_child(Clinical.batch))
        self.assertFalse(Clinical.group.is_child(Clinical.matched_datapoint))
        self.assertTrue(Clinical.group.is_child(Clinical.dataset))

        self.assertFalse(Clinical.timepoint.is_child(Clinical.session))
        self.assertFalse(Clinical.timepoint.is_child(Clinical.subject))
        self.assertFalse(Clinical.timepoint.is_child(Clinical.member))
        self.assertFalse(Clinical.timepoint.is_child(Clinical.group))
        self.assertFalse(Clinical.timepoint.is_child(Clinical.timepoint))
        self.assertFalse(Clinical.timepoint.is_child(Clinical.batch))
        self.assertFalse(Clinical.timepoint.is_child(Clinical.matched_datapoint))
        self.assertTrue(Clinical.timepoint.is_child(Clinical.dataset))

        self.assertFalse(Clinical.batch.is_child(Clinical.session))
        self.assertFalse(Clinical.batch.is_child(Clinical.subject))
        self.assertFalse(Clinical.batch.is_child(Clinical.member))
        self.assertTrue(Clinical.batch.is_child(Clinical.group))
        self.assertTrue(Clinical.batch.is_child(Clinical.timepoint))
        self.assertFalse(Clinical.batch.is_child(Clinical.batch))
        self.assertFalse(Clinical.batch.is_child(Clinical.matched_datapoint))
        self.assertTrue(Clinical.batch.is_child(Clinical.dataset))

        self.assertFalse(Clinical.matched_datapoint.is_child(Clinical.session))
        self.assertFalse(Clinical.matched_datapoint.is_child(Clinical.subject))
        self.assertFalse(Clinical.matched_datapoint.is_child(Clinical.member))
        self.assertTrue(Clinical.matched_datapoint.is_child(Clinical.group))
        self.assertTrue(Clinical.matched_datapoint.is_child(Clinical.timepoint))
        self.assertFalse(Clinical.matched_datapoint.is_child(Clinical.batch))
        self.assertFalse(Clinical.matched_datapoint.is_child(Clinical.matched_datapoint))
        self.assertTrue(Clinical.matched_datapoint.is_child(Clinical.dataset))

        self.assertFalse(Clinical.member.is_child(Clinical.session))
        self.assertFalse(Clinical.member.is_child(Clinical.subject))
        self.assertFalse(Clinical.member.is_child(Clinical.member))
        self.assertFalse(Clinical.member.is_child(Clinical.group))
        self.assertFalse(Clinical.member.is_child(Clinical.timepoint))
        self.assertFalse(Clinical.member.is_child(Clinical.batch))
        self.assertFalse(Clinical.member.is_child(Clinical.matched_datapoint))
        self.assertTrue(Clinical.member.is_child(Clinical.dataset))

        self.assertFalse(Clinical.dataset.is_child(Clinical.session))
        self.assertFalse(Clinical.dataset.is_child(Clinical.subject))
        self.assertFalse(Clinical.dataset.is_child(Clinical.member))
        self.assertFalse(Clinical.dataset.is_child(Clinical.group))
        self.assertFalse(Clinical.dataset.is_child(Clinical.timepoint))
        self.assertFalse(Clinical.dataset.is_child(Clinical.batch))
        self.assertFalse(Clinical.dataset.is_child(Clinical.matched_datapoint))
        self.assertFalse(Clinical.dataset.is_child(Clinical.dataset))




