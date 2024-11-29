from roger.pipelines import DugPipeline

class PicSure(DugPipeline):
    "Pipeline for BACPAC data set"
    pipeline_name = "topmedtest"  #lakefs 
    parser_name = "dbgap_parser"