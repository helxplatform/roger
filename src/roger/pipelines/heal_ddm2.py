"Pipeline to ingest HEAL in new data model"

from roger.pipelines import DugPipeline
from roger.core import storage

class HealStudiesDDM2Pipeline(DugPipeline):
    "Pipeline for HEAL data using dug data model v2"
    pipeline_name = "heal_studies_ddm2"
    parser_name = "heal-ddm2"
