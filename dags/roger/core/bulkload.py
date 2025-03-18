"Bulk loader for Roger"

import os
import glob
import shutil
from collections import defaultdict
from functools import reduce
from string import Template
import time

import requests
import redis
from falkordb_bulk_loader.bulk_insert import bulk_insert

from roger.config import get_default_config as get_config
from roger.logger import get_logger
from roger.core.redis_graph import RedisGraph
from roger.core.enums import SchemaType
from roger.models.biolink import BiolinkModel
from roger.components.data_conversion import cast
from roger.core import storage

log = get_logger()

class BulkLoad:
    """ Tools for creating a Redisgraph bulk load dataset. """
    def __init__(self, biolink, config=None):
        self.biolink = biolink
        if not config:
            config = get_config()
        self.config = config
        separator = self.config.get('bulk_loader',{}).get('separator', '|')
        self.separator =(chr(separator) if isinstance(separator, int)
                         else separator)

    def create (self):
        """Used in the CLI on args.create_bulk"""
        self.create_nodes_csv_file()
        self.create_edges_csv_file()

    def create_nodes_csv_file(self, input_data_path=None, output_data_path=None):
        # clear out previous data
        bulk_path = storage.bulk_path("nodes", output_data_path)
        if os.path.exists(bulk_path):
            shutil.rmtree(bulk_path)
        categories_schema = storage.read_schema (SchemaType.CATEGORY, input_data_path)
        state = defaultdict(lambda: None)
        log.info(f"processing nodes")
        """ Write node data for bulk load. """

        categories = defaultdict(lambda: [])
        category_error_nodes = set()
        merged_nodes_file = storage.merged_objects('nodes', input_data_path)
        counter = 1
        for node in storage.json_line_iter(merged_nodes_file):
            if not node.get('category'):
                category_error_nodes.add(node['id'])
                node['category'] = [BiolinkModel.root_type]
            index = self.biolink.get_leaf_class(node['category'])
            categories[index].append(node)
            if category_error_nodes:
                log.error(
                    f"some nodes didn't have category assigned. "
                    f"KGX file has errors. "
                    f"Nodes {len(category_error_nodes)}. "
                    f"They will be typed {BiolinkModel.root_type}. "
                    f"Showing first 10: {list(category_error_nodes)[:10]}.")
            # flush every 100K
            if counter % 100_000 == 0:
                self.write_bulk(storage.bulk_path("nodes", output_data_path),
                                categories, categories_schema,
                                state=state, is_relation=False)
                # reset variables.
                category_error_nodes = set()
                categories = defaultdict(lambda: [])
            counter += 1
        # write back if any thing left.
        if len(categories):
            self.write_bulk(storage.bulk_path("nodes", output_data_path),
                            categories, categories_schema,
                            state=state, is_relation=False)

    def create_edges_csv_file(self, input_data_path=None, output_data_path=None):
        """ Write predicate data for bulk load. """
        # Clear out previous data
        bulk_path = storage.bulk_path("edges", output_data_path)
        if os.path.exists(bulk_path):
            shutil.rmtree(bulk_path)
        predicates_schema = storage.read_schema(SchemaType.PREDICATE, input_data_path)
        predicates = defaultdict(lambda: [])
        edges_file = storage.merged_objects('edges', input_data_path)
        counter = 1
        state = {}
        for edge in storage.json_line_iter(edges_file):
            predicates[edge['predicate']].append(edge)
            # write out every 100K , to avoid large predicate dict.
            if counter % 100_000 == 0:
                self.write_bulk(
                    storage.bulk_path("edges", output_data_path),predicates, predicates_schema,
                    state=state, is_relation=True)
                predicates = defaultdict(lambda : [])
            counter += 1
        # if there are some items left (if loop ended before counter reached the
        # specified value)
        if len(predicates):
            self.write_bulk(storage.bulk_path("edges", output_data_path), predicates,
                            predicates_schema,state=state, is_relation=True)

    @staticmethod
    def create_redis_schema_header(attributes: dict, is_relation=False):
        """Creates col headers for csv to be used by redis bulk loader

        Column headers are generated by assigning redis types
        :param attributes: dict of data labels with values as python type strs
        :param separator: CSV separator
        :return: list of attrs, each item is attributeLabel:redisGraphDataType
        """
        redis_type_conversion_map = {
            'str': 'STRING',
            'float': 'FLOAT',  # Do we need to handle double
            'int': 'INT',
            'bool': 'BOOL',
            'list': 'ARRAY'
        }
        col_headers = []
        def format_for_redis(label, typ):
            return f'{label}:{typ}'
        for attribute, attribute_type in attributes.items():
            col_headers.append(format_for_redis(
                attribute, redis_type_conversion_map[attribute_type]))
        # Note this two fields are only important to bulk loader
        # they will not be members of the graph
        # https://github.com/RedisGraph/redisgraph-bulk-loader/tree/master#input-schemas
        if is_relation:
            col_headers.append('internal_start_id:START_ID')
            col_headers.append('internal_end_id:END_ID')
        # replace id:STRING with id:ID
        col_headers.append('id:ID')
        col_headers = list(filter(lambda x: x != 'id:STRING', col_headers))
        return col_headers

    @staticmethod
    def group_items_by_attributes_set(objects: list, processed_object_ids: set):
        """ Groups items into a dictionary

        The keys the output dictionary are sets of attributes set for all
        items accessed in that key.

        Eg.:
        { set(id,name,category): [{id:'xx0',name:'bbb', 'category':['type']}....
        {id:'xx1', name:'bb2', category: ['type1']}] }
        :param objects: list of nodes or edges
        :param processed_object_ids: ids to skip since they are processed.
        :return: dictionary grouping based on set attributes
        """
        clustered_by_set_values = {}
        improper_keys = set()
        def value_set_test(val):
            "Converted from lambda function, is this just 'if x:'?"
            if (val is not None and val != [] and val != ''):
                return True
            return False
        for obj in objects:
            # redis bulk loader needs columns not to include ':'
            # till backticks are implemented we should avoid these.
            def key_filter(key):
                # Make sure no colons in key names
                return ':' not in key
            keys_with_values = frozenset(
                [k for k in obj.keys()
                 if value_set_test(obj[k]) and key_filter(k)])
            for key in [k for k in obj.keys() if obj[k] and not key_filter(k)]:
                improper_keys.add(key)
            # group by attributes that have values. # Why?
            # Redis bulk loader has one issue
            # imagine we have:
            #
            #{'name': 'x'} , {'name': 'y', 'is_metabolite': true}
            #
            # we have a common schema name:STRING,is_metabolite:
            #
            # BOOL values `x,` and `y,true`
            #
            # but x not having value for is_metabolite is not handled well,
            # redis bulk loader says we should give it default if we were to
            # enforce schema but due to the nature of the data assigning
            # defaults is very not an option.  hence grouping data into several
            # csv's might be the right way (?)
            if obj['id'] not in processed_object_ids:
                val_list = clustered_by_set_values.get(keys_with_values, [])
                val_list.append(obj)
                clustered_by_set_values[keys_with_values] = val_list
        return clustered_by_set_values, improper_keys

    def write_bulk(self, bulk_path, obj_map, schema, state={},
                   is_relation=False):
        """ Write a bulk load group of objects.
        :param bulk_path: Path to the bulk loader object to write.
        :param obj_map: A map of biolink type to list of objects.
        :param schema: The schema (nodes or predicates) containing identifiers.
        :param state: Track state of already written objs to avoid duplicates.
        """

        os.makedirs (bulk_path, exist_ok=True)
        processed_objects_id = state.get('processed_id', set())
        called_x_times = state.get('called_times', 0)
        called_x_times += 1
        for key, objects in obj_map.items ():
            if len(objects) == 0:
                continue
            try:
                all_keys = schema[key]
            except Exception as e:
                log.error(f"{key} not in {schema.keys()} " )
                raise Exception("error") from e
            """ Make all objects conform to the schema. """
            clustered_by_set_values, improper_redis_keys = (
                self.group_items_by_attributes_set(objects,
                                                   processed_objects_id))

            if improper_redis_keys:
                log.warning(
                    "The following keys were skipped since they include "
                    "conflicting `:` that would cause errors while bulk "
                    "loading to redis. [%s]", str(improper_redis_keys))
            for index, set_attributes in enumerate(
                    clustered_by_set_values.keys()):
                items = clustered_by_set_values[set_attributes]
                # When parted files are saved let the file names be collected
                # here
                state['file_paths'] = state.get('file_paths', {})
                state['file_paths'][key] = state['file_paths'].get(key, {})
                out_file = state['file_paths'][key][set_attributes] = (
                    state['file_paths'].get(key, {}).get(set_attributes, ''))

                # When calling write bulk , lets say we have processed some
                # chemicals from file 1 and we start processing file 2 if we are
                # using just index then we might (rather will) end up adding
                # records to the wrong file so we need this to be unique as
                # possible by adding called_x_times , if we already found
                # out-file from state obj we are sure that the schemas match.

                # biolink:<TYPE> is not valid name so we need to remove :
                file_key = key.replace(':', '~')

                out_file = (
                    f"{bulk_path}/{file_key}.csv-{index}-{called_x_times}"
                    if not out_file
                    else out_file)
                # store back file name
                state['file_paths'][key][set_attributes] = out_file
                new_file = not os.path.exists(out_file)
                keys_for_header = {x: all_keys[x] for x in all_keys
                                   if x in set_attributes}
                redis_schema_header = self.create_redis_schema_header(
                    keys_for_header, is_relation)
                with open(out_file, "a", encoding='utf-8') as stream:
                    if new_file:
                        state['file_paths'][key][set_attributes] = out_file
                        log.info(f"  --creating {out_file}")
                        stream.write(self.separator.join(redis_schema_header))
                        stream.write("\n")
                    else:
                        log.info(f"  --appending to {out_file}")

                    # Write fields, skipping duplicate objects.
                    for obj in items:
                        oid = str(obj['id'])
                        if oid in processed_objects_id:
                            continue
                        processed_objects_id.add(oid)

                        # Add ID / START_ID / END_ID depending
                        internal_id_fields = {
                            'internal_id': obj['id']
                        }
                        if is_relation:
                            internal_id_fields.update({
                                'internal_start_id': obj['subject'],
                                'internal_end_id': obj['object']
                            })
                        obj.update(internal_id_fields)
                        values = []

                        # uses redis schema header to preserve order when
                        # writing lines out.
                        for column_name in redis_schema_header:
                            # last key is the type
                            obj_key = ':'.join(column_name.split(':')[:-1])
                            value = obj[obj_key]

                            if obj_key not in internal_id_fields:
                                current_type = type(value).__name__
                                expected_type = all_keys[obj_key]
                                # cast it if it doesn't match type in schema
                                # keys i.e all_keys
                                value = (
                                    cast(obj[obj_key], all_keys[obj_key])
                                    if expected_type != current_type
                                    else value)
                            # escape quotes .
                            values.append(str(value).replace("\"", "\\\""))
                        s = self.separator.join(values)
                        stream.write(s)
                        stream.write("\n")
        state['processed_id'] = processed_objects_id
        state['called_times'] = called_x_times

    def insert (self, input_data_path=None):
        redisgraph = self.config.redisgraph
        nodes = sorted(glob.glob (storage.bulk_path ("**/nodes/**.csv*", input_data_path), recursive=True))
        edges = sorted(glob.glob (storage.bulk_path ("**/edges/**.csv*", input_data_path), recursive=True))
        graph = redisgraph['graph']
        log.info(f"bulk loading \n  nodes: {nodes} \n  edges: {edges}")

        try:
            log.info (f"deleting graph {graph} in preparation for bulk load.")
            db = self.get_redisgraph()
            db.redis_graph.delete ()
        except redis.exceptions.ResponseError:
            log.info("no graph to delete")

        log.info ("bulk loading graph: %s", str(graph))
        args = []
        if len(nodes) > 0:
            bulk_path_root = glob.glob(storage.bulk_path('**/nodes', path=input_data_path), recursive=True)[0] + os.path.sep
            nodes_with_type = []
            collect_labels = set()
            for x in nodes:
                """ 
                    These lines prep nodes bulk load by:
                    1) appending to labels 'biolink.'
                    2) combine labels to create a multilabel redis node i.e. "biolink.OrganismalEntity:biolink.SubjectOfInvestigation" 
                """
                file_name_type_part = x.replace(bulk_path_root, '').split('.')[0].split('~')[1]
                all_labels = "biolink." + file_name_type_part + ":" + ":".join([f'biolink.{v.lstrip("biolink:")}' for v in self.biolink.toolkit.get_ancestors("biolink:" + file_name_type_part, reflexive=False, formatted=True )] )
                collect_labels.add("biolink." + file_name_type_part)
                for v in self.biolink.toolkit.get_ancestors("biolink:" + file_name_type_part, reflexive=False,
                                                            formatted=True):
                    collect_labels.add(f'biolink.{v.lstrip("biolink:")}')
                nodes_with_type.append(f"{all_labels} {x}")
            args.extend(("-N " + " -N ".join(nodes_with_type)).split())
        if len(edges) > 0:
            bulk_path_root = glob.glob(storage.bulk_path('**/edges', path=input_data_path), recursive=True)[0] + os.path.sep
            edges_with_type = [f"biolink.{x.replace(bulk_path_root, '').strip(os.path.sep).split('.')[0].split('~')[1]} {x}"
                               for x in edges]
            # Edge label now no longer has 'biolink:'
            args.extend(("-R " + " -R ".join(edges_with_type)).split())
        args.extend([f"--separator={self.separator}"])
        args.extend([f"--server-url=redis://:{redisgraph['password']}@{redisgraph['host']}:{redisgraph['port']}"])
        args.extend(['--enforce-schema'])
        args.extend(['-e'])
        for lbl in collect_labels:
            args.extend([f'-i `{lbl}`:id', f'-f {lbl}:name', f'-f {lbl}:synonyms'])
        args.extend([f"{redisgraph['graph']}"])
        """ standalone_mode=False tells click not to sys.exit() """
        log.debug(f"Calling bulk_insert with extended args: {args}")
        try:
            bulk_insert(args, standalone_mode=False)
            # self.add_indexes()
        except Exception as e:
            log.error(f"Unexpected {e.__class__.__name__}: {e}")
            raise

    def add_indexes(self):
        redis_connection = self.get_redisgraph()
        all_labels = redis_connection.query(
            "Match (c) return distinct labels(c)").result_set
        all_labels = reduce(lambda x, y: x + y, all_labels, [])
        id_index_queries = [
            f'CREATE INDEX on  :`{label}`(id)' for label in all_labels
        ]
        name_index_queries = (
            "CALL db.labels() YIELD label "
            "CALL db.idx.fulltext.createNodeIndex(label, 'name', 'synonyms')")

        for query in id_index_queries:
            redis_connection.query(query=query)
        redis_connection.query(query=name_index_queries)
        log.info(f"Indexes created for {len(all_labels)} labels.")

    def get_redisgraph(self):
        return RedisGraph(
            host=self.config.redisgraph.host,
            port=self.config.redisgraph.port,
            password=self.config.redisgraph.password,
            graph=self.config.redisgraph.graph,
        )

    def validate(self):

        db = self.get_redisgraph()
        validation_queries = self.config.get(
            'validation', {}).get('queries', [])
        for key, query in validation_queries.items ():
            text = query['query']
            name = query['name']
            args = query.get('args', [{}])
            for arg in args:
                start = storage.current_time_in_millis ()
                instance = Template (text).safe_substitute (arg)
                db.query (instance)
                duration = storage.current_time_in_millis () - start
                log.info (f"Query {key}:{name} ran in {duration}ms: {instance}")

    def wait_for_tranql(self):
        retry_secs = 3
        tranql_endpoint = self.config.indexing.tranql_endpoint
        log.info(f"Contacting {tranql_endpoint}")
        graph_name = self.config["redisgraph"]["graph"]
        test_query = "SELECT disease-> phenotypic_feature " \
                     f"FROM 'redis:{graph_name}'" \
                     f"WHERE  disease='MONDO:0004979'"
        is_done_loading = False
        try:
            while not is_done_loading:
                response = requests.post(tranql_endpoint, data=test_query)
                response_code = response.status_code
                response = response.json()
                is_done_loading = "message" in response and response_code == 200
                if is_done_loading:
                    break
                else:
                    log.info(f"Tranql responsed with response: {response}")
                    log.info(f"Retrying in {retry_secs} secs...")
                time.sleep(retry_secs)
        except ConnectionError as e:
            # convert exception to be more readable.
            raise ConnectionError(
                f"Attempting to contact {tranql_endpoint} "
                f"failed due to connection error. "
                f"Please check status of Tranql server.") from e
