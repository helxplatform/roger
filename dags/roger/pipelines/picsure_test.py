from roger.pipelines import DugPipeline

class PicSure(DugPipeline):
    "Pipeline for BACPAC data set"
    pipeline_name = "bdc-test5"  #lakefs 
    parser_name = "dbgap"