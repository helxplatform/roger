"Pipeline for Topmed data"

from roger.pipelines import DugPipeline
from roger.core import storage

class TopmedPipeline(DugPipeline):
    "Pipeline for Topmed data set"
    pipeline_name = "topmed"
    parser_name = "TOPMedTag"

    def get_objects(self, input_data_path=None):
        if not input_data_path:
            input_data_path = str(storage.dug_input_files_path('topmed'))
        topmed_file_pattern = storage.os.path.join(input_data_path,
                                                   "topmed_*.csv")
        return sorted(storage.glob.glob(topmed_file_pattern))
