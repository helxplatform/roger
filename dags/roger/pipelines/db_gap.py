"Dug pipeline for dbGaP data set"

from .base import DugPipeline

class dbGaPPipeline(DugPipeline):
    "Pipeline for the dbGaP data set"

    pipeline_name = 'dbGaP'
    parser_name = 'DbGaP'
    files_dir = 'db_gap'
