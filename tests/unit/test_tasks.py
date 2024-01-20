"Tests for roger.tasks module"

from unittest import TestCase, mock
import os

from roger import tasks
from roger.pipelines.exceptions import PipelineException

class GetEnvDatasetsTestCase(TestCase):
    "Unit test case for roger.tasks.get_env_datasets"

    DATASETS_ENV_NAME = "ROGER_DUG__INPUTS_DATA__SETS"

    def setUp(self):
        "Grab the existing environment before doing anything else"
        super().setUp()
        self._oldval = os.getenv(self.DATASETS_ENV_NAME, None)

    def tearDown(self):
        "Put the environment back like it was"
        super().tearDown()
        if self._oldval:
            os.environ[self.DATASETS_ENV_NAME] = self._oldval
        else:
            os.unsetenv(self.DATASETS_ENV_NAME)

    def test_get_env_datasets__defaults(self):
        "test get_env_datasets with no env value set"
        os.unsetenv(self.DATASETS_ENV_NAME)
        val = tasks.get_env_datasets()
        self.assertIsInstance(val, dict)
        self.assertEqual(val, {'topmed': 'v1.0'})

    def test_get_env_datasets__value(self):
        "test get_env_datasets with env value set"
        os.environ[self.DATASETS_ENV_NAME] = "dataset1:vers1,dataset2:vers2"
        val = tasks.get_env_datasets()
        self.assertIsInstance(val, dict)
        self.assertEqual(val, {'dataset1': 'vers1', 'dataset2': 'vers2'})

    def test_get_env_datasets__helpful_error(self):
        "get_env_datasets should raise a helpful error if it fails"
        os.environ[self.DATASETS_ENV_NAME] = "ds1:v1,arglebargle"
        with self.assertRaises(PipelineException) as cm:
            val = tasks.get_env_datasets()

        self.assertIn('arglebargle', str(cm.exception),
                      "get_env_datasets parsing fail should return error with "
                      "bad string")
