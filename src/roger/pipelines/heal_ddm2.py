"Pipeline to ingest HEAL in new data model"

from roger.pipelines import DDM2Pipeline
from roger.core import storage

class HealStudiesDDM2Pipeline(DDM2Pipeline):
    "Pipeline for HEAL data using dug data model v2"
    pipeline_name = "heal-mds-studies-ddm2"
    parser_name = "heal-ddm2"
