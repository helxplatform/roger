"NIDA data set pipeline definition"

import glob
from .core import DugPipeline

class NIDAPipeline(DugPipeline):
    "NIDA data pipeline"

    pipeline_name = 'nida'
    parser_name = 'NIDA'

    def get_objects(self, input_data_path=None):
        "Return list of NIDA source files"
        if not input_data_path:
            input_data_path = storage.dug_input_files_path(
                self.get_files_dir())
        nida_file_pattern = os.path.join(input_data_path, 'NIDA-*.xml')
        return sorted(glob.glob(nida_file_pattern))
