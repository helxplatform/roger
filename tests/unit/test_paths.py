"Unit tests for read and write paths based on environmnet variables"

from unittest import TestCase, mock
import os
from pathlib import Path
from dug_helpers.dug_utils import DugUtil
from roger.config import config

class ESMock(mock.AsyncMock):
    "Mock class to spoof async es object"
    def __init__(self, *args, **kwargs):
        "just debugging"
        super().__init__(*args, **kwargs)
        print("ESMock init")

class SearchMock(mock.MagicMock):
    "Mock class to spoof dug Search object"
    def __init__(self, *args, **kwargs):
        "Add attrs"
        super().__init__(*args, **kwargs)
        self.es = mock.AsyncMock()

class DugFactoryMock(mock.MagicMock):
    "Mock the DugFactory object"

    def build_http_session(self, *args, **kwargs):
        "Spit back an async mock"
        return mock.AsyncMock()

    def build_search_obj(self, *args, **kwargs):
        "Spit back a fake search object"
        return SearchMock()

class AnnotateTestCase(TestCase):
    "Path tests for methods associated with the annotate dag"

    def setUp(self):
        "Patch the read and write_object methods"
        self.storage_patcher = mock.patch.multiple(
            'dug_helpers.dug_utils.storage',
            # dug_dd_xml_objects=mock.DEFAULT,
            get_files_recursive=mock.DEFAULT,
            read_object=mock.DEFAULT,
            write_object=mock.DEFAULT
        )
        self.storage_mock = self.storage_patcher.start()

        self.factory_patcher = mock.patch(
            'dug_helpers.dug_utils.DugFactory', new_callable=DugFactoryMock)
        self.factory_mock = self.factory_patcher.start()

        self.parser_mock = mock.MagicMock(name="parser_mock")
        get_parser_mock = mock.MagicMock(return_value=self.parser_mock)
        self.parser_patcher = mock.patch.multiple(
            'dug_helpers.dug_utils',
            get_parser=get_parser_mock)
        self.parser_patcher.start()

        self.addCleanup(self.storage_patcher.stop)
        self.addCleanup(self.factory_patcher.stop)
        self.addCleanup(self.parser_patcher.stop)

    def tearDown(self):
        "De-patch the read and write_object methods"
        self.storage_patcher.stop()
        self.factory_patcher.stop()
        self.parser_patcher.stop()

    def test_annotate_db_gap_files_no_environment(self):
        "Test DugUtil.annotate_db_gap_files without env vars"
        self.storage_mock['get_files_recursive'].return_value = [
            Path('/some/path/input_files/db_gap/file_1.xml'),
            Path('/some/path/input_files/db_gap/file_2.xml')]

        DugUtil.annotate_db_gap_files(config=config)
        self.storage_mock['get_files_recursive'].assert_called()
        self.assertIn(
            'input_files/db_gap',
            str(self.storage_mock['get_files_recursive'].call_args.args[1]))
        self.parser_mock.assert_called()
        call_list = self.parser_mock.call_args_list
        self.assertEqual(len(call_list), 2)
        self.assertIn('input_files/db_gap/file_1.xml',
                      str(call_list[0].args[0]))
        self.storage_mock['write_object'].assert_called()
        self.fail(self.storage_mock['write_object'].call_args.args)
