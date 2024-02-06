"Pipeline for Topmed data"

from roger.pipelines import DugPipeline
from roger.core import storage
from roger.logger import logger
class TopmedPipeline(DugPipeline):
    "Pipeline for Topmed data set"
    pipeline_name = "topmed"
    parser_name = "TOPMedTag"

    def get_objects(self, input_data_path=None):
        if not input_data_path:
            input_data_path = str(storage.dug_input_files_path('topmed'))
        files =storage.get_files_recursive(
                lambda file_name: file_name.endswith('.csv'),
                input_data_path)
        return sorted([str(x) for x in files])
