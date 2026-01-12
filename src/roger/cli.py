import roger.core.base as RogerUtil
from roger.config import config
from roger.logger import get_logger
from roger.pipelines import get_pipeline_classes
# from roger.dug_helpers.dug_utils import DugUtil, get_topmed_files, get_dbgap_files, get_sparc_files, get_anvil_files, get_nida_files
import sys
import argparse
import os
import time


log = get_logger()

def main():
    start = time.time()
    log.info(f"Start TIME:{start}")
    parser = argparse.ArgumentParser(description='Roger common cli tool.')
    """ Common CLI. """
    parser.add_argument('-d', '--data-root', default=None,
                        help="Root of data hierarchy")

    """ Roger CLI. """
    parser.add_argument('-v', '--dataset-version', help="Dataset version.",
                        default="v1.0")
    parser.add_argument('-g', '--get-kgx', help="Get KGX objects",
                        action='store_true')
    parser.add_argument('-s', '--create-schema', help="Infer schema",
                        action='store_true')
    parser.add_argument('-m', '--merge-kgx', help="Merge KGX nodes",
                        action='store_true')
    parser.add_argument('-b', '--create-bulk', help="Create bulk load",
                        action='store_true')
    parser.add_argument('-i', '--insert', help="Do the bulk insert",
                        action='store_true')
    parser.add_argument('-a', '--validate', help="Validate the insert",
                        action='store_true')

    dataset_envspec = os.getenv("ROGER_DUG__INPUTS_DATA__SETS",
                        "topmed:v2.0,dbGaP:v1.0,anvil:v1.0")
    data_sets = dataset_envspec.split(",")
    parser.add_argument('-D', '--datasets', action="append",
                        default= data_sets,
                        help="Dataset pipelines name:vers to run. "
                        "[-D topmed:v2.0 -D bdc:v1.0]")

    """ Dug Annotation CLI. """
    parser.add_argument('-gd', '--get_dug_input_files', action="store_true",
                        help="Gets input files for annotation")
    parser.add_argument('-l', '--load-and-annotate', action="store_true",
                        help="Annotates and normalizes datasets of varaibles.")
    parser.add_argument('-t', '--make-tagged-kg', action="store_true",
                        help="Creates KGX files from annotated variable "
                        "datasets.")

    """ Dug indexing CLI . """
    parser.add_argument('-iv', '--index-variables', action="store_true",
                        help="Index annotated variables to elastic search.")
    parser.add_argument('-C', '--crawl-concepts', action="store_true",
                        help="Crawl tranql and index concepts")
    parser.add_argument('-ic', '--index-concepts', action="store_true",
                        help="Index expanded concepts to elastic search.")

    parser.add_argument('-vc', '--validate-concepts', action="store_true",
                        help="Validates indexing of concepts")

    parser.add_argument('-vv', '--validate-variables', action="store_true",
                        help="Validates indexing of variables")

    args = parser.parse_args ()

    if args.data_root is not None:
        data_root = args.data_root
        config.data_root = data_root
        log.info (f"data root:{data_root}")

    # When all lights are on...

    # Instantiate the pipeline classes
    pipeline_names = {x.split(':')[0]: x.split(':')[1] for x in args.datasets}
    pipeline_classes = get_pipeline_classes(pipeline_names)
    pipelines = [pipeclass(config) for pipeclass in pipeline_classes]

    # Annotation comes first
    if args.get_dug_input_files:
        for pipe in pipelines:
            pipe.get_versioned_files()

    if args.load_and_annotate:
        DugUtil.clear_annotation_cached(config=config)
        DugUtil.annotate_db_gap_files(config=config)
        DugUtil.annotate_topmed_files(config=config)
        DugUtil.annotate_anvil_files(config=config)
    if args.make_tagged_kg:
        DugUtil.make_kg_tagged(config=config)

    # Roger things
    if args.get_kgx:
        RogerUtil.get_kgx(config=config)
    if args.merge_kgx:
        RogerUtil.merge_nodes(config=config)
    if args.create_schema:
        RogerUtil.create_schema(config=config)
    if args.create_bulk:
        RogerUtil.create_bulk_load(config=config)
    if args.insert:
        RogerUtil.bulk_load(config=config)
    if args.validate:
        RogerUtil.validate(config=config)
        RogerUtil.check_tranql(config=config)

    # Back to dug indexing
    if args.index_variables:
        DugUtil.index_variables(config=config)

    if args.validate_variables:
        DugUtil.validate_indexed_variables(config=config)

    if args.crawl_concepts:
        DugUtil.crawl_tranql(config=config)

    if args.index_concepts:
        DugUtil.index_concepts(config=config)

    if args.validate_concepts:
        DugUtil.validate_indexed_concepts(config=config)

    end = time.time()
    time_elapsed = end - start
    log.info(f"Completion TIME:{time_elapsed}")

    sys.exit (0)

if __name__ == "__main__":
    main()
