"Base class for implementing a dataset annotate, crawl, and index pipeline"

import os
import asyncio
from io import StringIO
import logging
import re
import hashlib
import traceback
from functools import reduce
from pathlib import Path

from airflow.models import DAG

from dug.core import get_parser, get_plugin_manager, DugConcept
from dug.core.annotate import DugAnnotator, ConceptExpander
from dug.core.crawler import Crawler
from dug.core.factory import DugFactory
from dug.core.parsers import Parser, DugElement
from dug.core.async_search import Search
from dug.core.index import Index

from roger.config import RogerConfig
from roger.core import storage
from roger.models.biolink import BiolinkModel
from roger.logger import get_logger
from roger.tasks import default_args, create_python_task

log = get_logger()

class PipelineException(Exception):
    "Exception raised from DugPipeline and related classes"

def make_edge(subj,
              obj,
              predicate='biolink:related_to',
              predicate_label='related to',
              relation='biolink:related_to',
              relation_label='related to'
              ):
    """Create an edge between two nodes.

    :param subj: The identifier of the subject.
    :param pred: The predicate linking the subject and object.
    :param obj: The object of the relation.
    :param predicate: Biolink compatible edge type.
    :param predicate_label: Edge label.
    :param relation: Ontological edge type.
    :param relation_label: Ontological edge type label.
    :returns: Returns and edge.
    """
    edge_id = hashlib.md5(
        f'{subj}{predicate}{obj}'.encode('utf-8')).hexdigest()
    return {
        "subject": subj,
        "predicate": predicate,
        "predicate_label": predicate_label,
        "id": edge_id,
        "relation": relation,
        "relation_label": relation_label,
        "object": obj,
        "provided_by": "renci.bdc.semanticsearch.annotator"
    }


class DugPipeline():
    "Base class for dataset pipelines"

    pipeline_name = None
    unzip_source = True

    def __init__(self, config: RogerConfig, to_string=True):
        "Set instance variables and check to make sure we're overriden"
        if not (self.pipeline_name):
            raise PipelineException(
                "Subclass must at least define pipeline_name as class var")
        self.parser: Parser = get_parser(dug_plugin_manager.hook,
                                         self.get_parser_name())

        self.config = config
        self.bl_toolkit = BiolinkModel()
        dug_conf = config.to_dug_conf()
        self.element_mapping = config.indexing.element_mapping
        self.factory = DugFactory(dug_conf)
        self.cached_session = self.factory.build_http_session()
        self.event_loop = asyncio.new_event_loop()
        if to_string:
            self.log_stream = StringIO()
            self.string_handler = logging.StreamHandler(self.log_stream)
            log.addHandler(self.string_handler)

        self.annotator: DugAnnotator = self.factory.build_annotator()

        self.tranqlizer: ConceptExpander = self.factory.build_tranqlizer()

        graph_name = self.config["redisgraph"]["graph"]
        source = f"redis:{graph_name}"
        self.tranql_queries: dict = self.factory.build_tranql_queries(source)
        self.node_to_element_queries: list = (
            self.factory.build_element_extraction_parameters(source))

        indexing_config = config.indexing
        self.variables_index = indexing_config.get('variables_index')
        self.concepts_index = indexing_config.get('concepts_index')
        self.kg_index = indexing_config.get('kg_index')

        self.search_obj: Search = self.factory.build_search_obj([
            self.variables_index,
            self.concepts_index,
            self.kg_index,
        ])
        self.index_obj: Index = self.factory.build_indexer_obj([
                self.variables_index,
                self.concepts_index,
                self.kg_index,

        ])

    def __enter__(self):
        self.event_loop = asyncio.new_event_loop()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # close elastic search connection
        self.event_loop.run_until_complete(self.search_obj.es.close())
        # close async loop
        if self.event_loop.is_running() and not self.event_loop.is_closed():
            self.event_loop.close()
        if exc_type or exc_val or exc_tb:
            traceback.print_exc()
            log.error("%s %s %s", exc_val, exc_val, exc_tb)
            log.exception("Got an exception")

    def get_data_format(self):
        """Access method for data_format parameter

        Defaults to pipeline_name unless self.data_format is set. This method
        can also be overriden
        """
        return getattr(self, 'data_format', self.pipeline_name)

    def get_files_dir(self):
        """Access method for files_dir parameter

        Defaults to pipeline_name unless self.files_dir is set. This method can
        also be overriden.
        """
        return getattr(self, 'files_dir', self.pipeline_name)

    def get_parser_name(self):
        """Access method for parser_name

        Defaults to pipeline_name unless self.parser_name is set. This method
        can also be overriden.
        """
        return getattr(self, 'parser_name', self.pipeline_name)

    def annotate_files(self, parsable_files, output_data_path=None):
        """
        Annotates a Data element file using a Dug parser.
        :param parser_name: Name of Dug parser to use.
        :param parsable_files: Files to parse.
        :return: None.
        """
        dug_plugin_manager = get_plugin_manager()
        if not output_data_path:
            output_data_path = storage.dug_annotation_path('')
        log.info("Parsing files")
        for parse_file in parsable_files:
            log.debug("Creating Dug Crawler object on parse_file %s",
                      parse_file)
            crawler = Crawler(
                crawl_file=parse_file,
                parser=self.parser,
                annotator=self.annotator,
                tranqlizer='',
                tranql_queries=[],
                http_session=self.cached_session
            )

            # configure output space.
            current_file_name = '.'.join(
                os.path.basename(parse_file).split('.')[:-1])
            elements_file_path = os.path.join(
                output_data_path, current_file_name)
            elements_file = os.path.join(elements_file_path, 'elements.pickle')
            concepts_file = os.path.join(elements_file_path, 'concepts.pickle')

            # This is a file that the crawler will later populate. We start here
            # by creating an empty elements file.
            # This also creates output dir if it doesn't exist.
            elements_json = os.path.join(elements_file_path,
                                         'element_file.json')
            log.debug("Creating empty file: %s", elements_json)
            storage.write_object({}, elements_json)

            # Use the specified parser to parse the parse_file into elements.
            log.debug("Parser is %s", str(self.parser))
            elements = self.parser(parse_file)
            log.debug("Parsed elements: %s", str(elements))

            # This inserts the list of elements into the crawler where
            # annotate_elements expects to find it. Maybe in some future version
            # of Dug this could be a parameter instead of an attribute?
            crawler.elements = elements

            # @TODO propose for Dug to make this a crawler class init param(??)
            crawler.crawlspace = elements_file_path
            log.debug("Crawler annotator: %s", str(crawler.annotator))
            crawler.annotate_elements()

            # Extract out the concepts gotten out of annotation
            # Extract out the elements
            non_expanded_concepts = crawler.concepts
            # The elements object will have been modified by annotate_elements,
            # so we want to make sure to catch those modifications.
            elements = crawler.elements

            # Write pickles of objects to file
            log.info("Parsed and annotated: %s", parse_file)

            storage.write_object(elements, elements_file)
            log.info("Pickled annotated elements to : %s", elements_file)
            storage.write_object(non_expanded_concepts, concepts_file)
            log.info("Pickled annotated concepts to : %s", concepts_file)

    def convert_to_kgx_json(self, elements, written_nodes=None):
        """
        Given an annotated and normalized set of study variables,
        generate a KGX compliant graph given the normalized annotations.
        Write that grpah to a graph database.
        See BioLink Model for category descriptions.
        https://biolink.github.io/biolink-model/notes.html
        """
        if written_nodes is None:
            written_nodes = set()
        graph = {
            "nodes": [],
            "edges": []
        }
        edges = graph['edges']
        nodes = graph['nodes']

        for _, element in enumerate(elements):
            # DugElement means a variable (Study variable...)
            if not isinstance(element, DugElement):
                continue
            study_id = element.collection_id
            if study_id not in written_nodes:
                nodes.append({
                    "id": study_id,
                    "category": ["biolink:Study"],
                    "name": study_id
                })
                written_nodes.add(study_id)

            # connect the study and the variable.
            edges.append(make_edge(
                subj=element.id,
                relation_label='part of',
                relation='BFO:0000050',
                obj=study_id,
                predicate='biolink:part_of',
                predicate_label='part of'))
            edges.append(make_edge(
                subj=study_id,
                relation_label='has part',
                relation="BFO:0000051",
                obj=element.id,
                predicate='biolink:has_part',
                predicate_label='has part'))

            # a node for the variable. Should be BL compatible
            variable_node = {
                "id": element.id,
                "name": element.name,
                "category": ["biolink:StudyVariable"],
                # bulk loader parsing issue
                "description": (
                    element.description.replace("'", '`').replace('\n', ' '))
            }
            if element.id not in written_nodes:
                nodes.append(variable_node)
                written_nodes.add(element.id)

            for identifier, metadata in element.concepts.items():
                identifier_object = metadata.identifiers.get(identifier)
                # This logic is treating DBGap files.
                # First item in current DBGap xml files is a topmed tag,
                # This is treated as a DugConcept Object. But since its not
                # a concept we get from annotation (?) its never added to
                # variable.concepts.items (Where variable is a DugElement obj)
                # The following logic is trying to extract types, and for the
                # aformentioned topmed tag it adds
                # `biolink:InfomrmationContentEntity`
                # Maybe a better solution could be adding types on
                # DugConcept objects
                # More specifically Biolink compatible types (?)
                #
                if identifier_object:
                    category = identifier_object.types
                elif identifier.startswith("TOPMED.TAG:"):
                    category = ["biolink:InformationContentEntity"]
                else:
                    continue
                if identifier not in written_nodes:
                    if isinstance(category, str):
                        bl_element = self.bl_toolkit.toolkit.get_element(
                            category)
                        category = [bl_element.class_uri or bl_element.slot_uri]
                    nodes.append({
                        "id": identifier,
                        "category": category,
                        "name": metadata.name
                    })
                    written_nodes.add(identifier)
                # related to edge
                edges.append(make_edge(
                    subj=element.id,
                    obj=identifier
                    ))
                # related to edge
                edges.append(make_edge(
                    subj=identifier,
                    obj=element.id))
        return graph

    def make_tagged_kg(self, elements):
        """ Make a Translator standard knowledge graph representing
        tagged study variables.
        :param variables: The variables to model.
        :param tags: The tags characterizing the variables.
        :returns: dict with nodes and edges modeling a Translator/Biolink KG.
        """
        graph = {
            "nodes": [],
            "edges": []
        }
        edges = graph['edges']
        nodes = graph['nodes']

        # Create graph elements to model tags and their
        # links to identifiers gathered by semantic tagging
        tag_map = {}
        # @TODO extract this into config or maybe dug ??
        topmed_tag_concept_type = "TOPMed Phenotype Concept"
        nodes_written = set()
        for tag in elements:
            if not (isinstance(tag, DugConcept)
                    and tag.type == topmed_tag_concept_type):
                continue
            tag_id = tag.id
            tag_map[tag_id] = tag
            nodes.append({
                "id": tag_id,
                "name": tag.name,
                "description": tag.description.replace("'", "`"),
                "category": ["biolink:InformationContentEntity"]
            })

            # Link ontology identifiers we've found for this tag via nlp.
            for identifier, metadata in tag.identifiers.items():
                if isinstance(metadata.types, str):
                    bl_element = self.bl_toolkit.toolkit.get_element(
                        metadata.types)
                    category = [bl_element.class_uri or bl_element.slot_uri]
                else:
                    category = metadata.types
                synonyms = metadata.synonyms if metadata.synonyms else []
                nodes.append({
                    "id": identifier,
                    "name": metadata.label,
                    "category": category,
                    "synonyms": synonyms
                })
                nodes_written.add(identifier)
                edges.append(make_edge(
                    subj=tag_id,
                    obj=identifier))
                edges.append(make_edge(
                    subj=identifier,
                    obj=tag_id))

        concepts_graph = self.convert_to_kgx_json(elements,
                                                  written_nodes=nodes_written)
        graph['nodes'] += concepts_graph['nodes']
        graph['edges'] += concepts_graph['edges']

        return graph

    def index_elements(self, elements_file):
        log.info(f"Indexing {elements_file}...")
        elements = storage.read_object(elements_file)
        count = 0
        total = len(elements)
        # Index Annotated Elements
        log.info(f"found {len(elements)} from elements files.")
        for element in elements:
            count += 1
            # Only index DugElements as concepts will be
            # indexed differently in next step
            if not isinstance(element, DugConcept):
                # override data-type with mapping values
                if element.type.lower() in self.element_mapping:
                    element.type = self.element_mapping[element.type.lower()]
                self.index_obj.index_element(
                    element, index=self.variables_index)
            percent_complete = (count / total) * 100
            if percent_complete % 10 == 0:
                log.info(f"{percent_complete} %")
        log.info(f"Done indexing {elements_file}.")

    def validate_indexed_elements(self, elements_file):
        elements = [x for x in storage.read_object(elements_file)
                    if not isinstance(x, DugConcept)]
        # Pick ~ 10 %
        sample_size = int(len(elements) * 0.1)

        # random.choices(elements, k=sample_size)
        test_elements = elements[:sample_size]
        log.info(f"Picked {len(test_elements)} "
                 f"from {elements_file} for validation.")
        for element in test_elements:
            # Pick a concept
            concepts = [element.concepts[curie] for curie in element.concepts
                        if element.concepts[curie].name]

            if len(concepts):
                # Pick the first concept
                concept = concepts[0]
                curie = concept.id
                search_term = re.sub(r'[^a-zA-Z0-9_\ ]+', '', concept.name)
                log.debug(f"Searching for Concept: {curie} "
                          f"and Search term: {search_term}")
                all_elements_ids = self._search_elements(curie, search_term)
                present = element.id in all_elements_ids
                if not present:
                    log.error(f"Did not find expected variable "
                              f"{element.id} in search result.")
                    log.error(f"Concept id : {concept.id}, "
                              f"Search term: {search_term}")
                    raise PipelineException(
                        f"Validation exception - did not find variable "
                        f"{element.id} from {str(elements_file)}"
                        f"when searching variable index with Concept ID : "
                        f"{concept.id} using Search Term : {search_term} ")
            else:
                log.info(
                    f"{element.id} has no concepts annotated. "
                    f"Skipping validation for it."
                )

    def _search_elements(self, curie, search_term):
        response = self.event_loop.run_until_complete(
            self.search_obj.search_vars_unscored(
                concept=curie,
                query=search_term))
        ids_dict = []
        if 'total_items' in response:
            if response['total_items'] == 0:
                log.error(f"No search elements returned for variable search:"
                          f" {self.variables_index}.")
                log.error(f"Concept id : {curie}, Search term: {search_term}")
                # raise Exception(f"Validation error - Did not find {curie} for"
                #                 f"Search term: {search_term}")
        else:
            for element_type in response:
                all_elements_ids = [e['id'] for e in
                                    reduce(lambda x, y: x + y['elements'],
                                           response[element_type], [])]
                ids_dict += all_elements_ids
        return ids_dict

    def crawl_concepts(self, concepts, data_set_name):
        """Adds tranql KG to Concepts

        Terms grabbed from KG are also added as search terms
        :param concepts:
        :param data_set_name:
        :return:
        """
        crawl_dir = storage.dug_crawl_path('crawl_output')
        output_file_name = os.path.join(data_set_name,
                                        'expanded_concepts.pickle')
        extracted_dug_elements_file_name = os.path.join(data_set_name,
                                                        'extracted_graph_elements.pickle')
        output_file = storage.dug_expanded_concepts_path(output_file_name)
        extracted_output_file = storage.dug_expanded_concepts_path(
            extracted_dug_elements_file_name)
        Path(crawl_dir).mkdir(parents=True, exist_ok=True)
        extracted_dug_elements = []
        log.debug("Creating Dug Crawler object")
        crawler = Crawler(
            crawl_file="",
            parser=None,
            annotator=None,
            tranqlizer=self.tranqlizer,
            tranql_queries=self.tranql_queries,
            http_session=self.cached_session,
        )
        crawler.crawlspace = crawl_dir
        counter = 0
        total = len(concepts)
        for concept_id, concept in concepts.items():
            counter += 1
            try:
                crawler.expand_concept(concept)
                concept.set_search_terms()
                concept.set_optional_terms()
            except Exception as e:
                log.error(concept)
                raise e
            for query in self.node_to_element_queries:
                log.info(query)
                casting_config = query['casting_config']
                tranql_source = query['tranql_source']
                dug_element_type = query['output_dug_type']
                extracted_dug_elements += crawler.expand_to_dug_element(
                    concept=concept,
                    casting_config=casting_config,
                    dug_element_type=dug_element_type,
                    tranql_source=tranql_source
                )
            concept.clean()
            percent_complete = int((counter / total) * 100)
            if percent_complete % 10 == 0:
                log.info(f"{percent_complete}%")
        storage.write_object(obj=concepts, path=output_file)
        storage.write_object(obj=extracted_dug_elements,
                             path=extracted_output_file)

    def index_concepts(self, concepts):
        log.info("Indexing Concepts")
        total = len(concepts)
        count = 0
        for concept_id, concept in concepts.items():
            count += 1
            self.index_obj.index_concept(concept, index=self.concepts_index)
            # Index knowledge graph answers for each concept
            for kg_answer_id, kg_answer in concept.kg_answers.items():
                self.index_obj.index_kg_answer(
                    concept_id=concept_id,
                    kg_answer=kg_answer,
                    index=self.kg_index,
                    id_suffix=kg_answer_id
                )
            percent_complete = int((count / total) * 100)
            if percent_complete % 10 == 0:
                log.info(f"{percent_complete} %")
        log.info("Done Indexing concepts")

    def validate_indexed_concepts(self, elements, concepts):
        """
        Validates linked concepts are searchable
        :param elements: Annotated dug elements
        :param concepts: Crawled (expanded) concepts
        :return:
        """
        # 1 . Find concepts with KG <= 10% of all concepts,
        # <= because we might have no results for some concepts from tranql
        sample_concepts = {key: value for key, value
                           in concepts.items() if value.kg_answers}
        if len(concepts) == 0:
            log.info("No Concepts found.")
            return
        log.info(
            f"Found only {len(sample_concepts)} Concepts with "
            f"Knowledge graph out of {len(concepts)}. "
            f"{(len(sample_concepts) / len(concepts)) * 100} %")
        # 2. pick elements that have concepts in the sample concepts set
        sample_elements = {}
        for element in elements:
            if isinstance(element, DugConcept):
                continue
            for concept in element.concepts:
                # add elements that have kg
                if concept in sample_concepts:
                    sample_elements[concept] = sample_elements.get(
                        concept, set())
                    sample_elements[concept].add(element.id)

        # Time for some validation
        for curie in concepts:
            concept = concepts[curie]
            if not concept.kg_answers:
                continue
            search_terms = []
            for key in concept.kg_answers:
                kg_object = concept.kg_answers[key]
                search_terms += kg_object.get_node_names()
                search_terms += kg_object.get_node_synonyms()
                # reduce(lambda x,y: x + y, [[node.get("name")]
                #                            + node.get("synonyms", [])
                #             for node in concept.kg_answers[
                #                 "knowledge_graph"]["nodes"]], [])
            # validation here is that for any of these nodes we should get back
            # the variable.
            # make unique
            search_terms_cap = 10
            search_terms = list(set(search_terms))[:search_terms_cap]
            log.debug(f"Using {len(search_terms)} "
                      f"Search terms for concept {curie}")
            for search_term in search_terms:
                # avoids elastic failure due to some reserved characters
                # 'search_phase_execution_exception',
                # 'token_mgr_error: Lexical error ...
                search_term = re.sub(r'[^a-zA-Z0-9_\ ]+', '', search_term)

                searched_element_ids = self._search_elements(curie, search_term)

                if curie not in sample_elements:
                    log.error(f"Did not find Curie id {curie} in Elements.")
                    log.error(f"Concept id : {concept.id}, "
                              f"Search term: {search_term}")
                    raise PipelineException(
                        f"Validation error - Did not find {element.id} for"
                        f" Concept id : {concept.id}, "
                        f"Search term: {search_term}")
                else:
                    present = bool([x for x in sample_elements[curie]
                                    if x in searched_element_ids])
                    if not present:
                        log.error(f"Did not find expected variable "
                                  f"{element.id} in search result.")
                        log.error(f"Concept id : {concept.id}, "
                                  f"Search term: {search_term}")
                        raise PipelineException(
                            f"Validation error - Did not find {element.id} for"
                            f" Concept id : {concept.id}, "
                            f"Search term: {search_term}")

    def clear_index(self, index_id):
        exists = self.search_obj.es.indices.exists(index=index_id)
        if exists:
            log.info(f"Deleting index {index_id}")
            response = self.event_loop.run_until_complete(
                self.search_obj.es.indices.delete(index=index_id))
            log.info(f"Cleared Elastic : {response}")
        log.info("Re-initializing the indicies")
        self.index_obj.init_indices()

    def clear_variables_index(self):
        self.clear_index(self.variables_index)

    def clear_kg_index(self):
        self.clear_index(self.kg_index)

    def clear_concepts_index(self):
        self.clear_index(self.concepts_index)

    ####
    # Methods above this are directly from what used to be
    # dug_helpers.dug_utils.Dug. Methods below are consolidated from what used
    # to be dug_helpers.dug_utils.DugUtil. These are intented to be the "top
    # level" interface to Roger, which Airflow DAGs or other orchestrators can
    # call directly.

    def get_versioned_files(self):
        """ Fetches a dug input data files to input file directory
        """
        meta_data = storage.read_relative_object("../../metadata.yaml")
        output_dir: Path = storage.dug_input_files_path(
            self.get_files_dir())
        data_store = self.config.dug_inputs.data_source

        # clear dir
        storage.clear_dir(output_dir)
        data_sets = self.config.dug_inputs.data_sets
        log.info(f"dataset: {data_sets}")
        pulled_files = []
        s3_utils = S3Utils(self.config.s3_config)
        for data_set in data_sets:
            data_set_name, current_version = data_set.split(':')
            for item in meta_data["dug_inputs"]["versions"]:
                if (item["version"] == current_version and
                    item["name"] == data_set_name and
                    item["format"] == self.get_data_format()):
                    if data_store == "s3":
                        for filename in item["files"]["s3"]:
                            log.info(f"Fetching {filename}")
                            output_name = filename.split('/')[-1]
                            output_path = output_dir / output_name
                            s3_utils.get(
                                str(filename),
                                str(output_path),
                            )
                            if self.unzip_source:
                                log.info(f"Unzipping {output_path}")
                                tar = tarfile.open(str(output_path))
                                tar.extractall(path=output_dir)
                            pulled_files.append(output_path)
                    else:
                        for filename in item["files"]["stars"]:
                            log.info(f"Fetching {filename}")
                            # fetch from stars
                            remote_host = config.annotation_base_data_uri
                            fetch = FileFetcher(
                                remote_host=remote_host,
                                remote_dir=current_version,
                                local_dir=output_dir)
                            output_path = fetch(filename)
                            if self.unzip_source:
                                log.info(f"Unzipping {output_path}")
                                tar = tarfile.open(str(output_path))
                                tar.extractall(path=output_dir)
                            pulled_files.append(output_path)
        return [str(filename) for filename in pulled_files]

    def get_objects(input_data_path=None):
        """Retrieve initial source objects for parsing

        This is a default method that will be overridden by subclasses
        frequently, it is expected.
        """
        if not input_data_path:
            input_data_path = storage.dug_input_files_path(
                self.get_files_dir())
        files = get_files_recursive(
            lambda file_name: file_name.endswith('.xml'),
            input_data_path)
        return sorted([str(f) for f in files])

    def annotate(to_string=False, files=None, input_data_path=None,
                 output_data_path=None):
        "Annotate files with the appropriate parsers and crawlers"
        if files is None:
            files = self.get_objects(input_data_path=input_data_path)
        self.annotate_files(parsable_files=files,
                            output_data_path=output_data_path)
        output_log = self.log_stream.get_value() if to_string else ''
        return output_log

    def generate_pipeline_subdag(self):
        "emit a dag pipeline for dataset that can be used as a subdag"
