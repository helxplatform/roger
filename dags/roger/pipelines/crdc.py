"Pipeline for BACPAC data"

from roger.pipelines import DugPipeline
from roger.core import storage

class CRDCPipeline(DugPipeline):
    "Pipeline for BACPAC data set"
    pipeline_name = "crdc"
    parser_name = "crdc"

    def get_objects(self, input_data_path=None):
        if not input_data_path:
            input_data_path = storage.dug_crdc_path()
        files = storage.get_files_recursive(
            lambda file_name: (
                not file_name.startswith('GapExchange_')
                and file_name.endswith('.xml')),
            input_data_path)
        return sorted([str(f) for f in files])
