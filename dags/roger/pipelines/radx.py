"Pipeline for BACPAC data"

from roger.pipelines import DugPipeline

class RadxPipeline(DugPipeline):
    "Pipeline for BACPAC data set"
    pipeline_name = "radx"
    parser_name = "radx"
