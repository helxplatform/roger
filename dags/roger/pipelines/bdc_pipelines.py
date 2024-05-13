"Dug pipeline for dbGaP data set"

from roger.pipelines import DugPipeline

class BIOLINCCdbGaPPipeline(DugPipeline):
    "Pipeline for the dbGaP data set"
    pipeline_name = 'biolincc'
    parser_name = 'biolincc'


class covid19dbGaPPipeline(DugPipeline):
    "Pipeline for the dbGaP data set"
    pipeline_name = 'covid19-dbgap'
    parser_name = 'covid19'

class dirDbGaPPipeline(DugPipeline):
    pipeline_name = "dir-dbgap"
    parser_name = "dir"

class LungMapDbGaPPipeline(DugPipeline):
    pipeline_name = "lungmap-dbgap"
    parser_name = "lungmap"

class nsrrDbGaPPipeline(DugPipeline):
    pipeline_name = "nsrr-dbgap"
    parser_name = "nsrr"

class ParentDbGaPPipeline(DugPipeline):
    pipeline_name = "parent-dbgap"
    parser_name = "parent"

class PCGCDbGaPPipeline(DugPipeline):
    pipeline_name = "pcgc-dbgap"
    parser_name = "pcgc"

class RecoverDbGaPPipeline(DugPipeline):
    pipeline_name = "recover-dbgap"
    parser_name = "recover"

class TopmedDBGaPPipeline(DugPipeline):
    pipeline_name = "topmed-gen3-dbgap"
    parser_name = "topmeddbgap"