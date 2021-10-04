import argparse
import glob
import os
import orjson as json
import ntpath
import pathlib
import redis
import requests
import shutil
import sys
import time
import yaml
import pickle
from bmt import Toolkit
from collections import defaultdict
from enum import Enum
from io import StringIO
from kgx.utils.kgx_utils import prepare_data_dict as kgx_merge_dict
from roger import ROGER_DATA_DIR
from roger.Config import get_default_config as get_config
from roger.roger_util import get_logger
from roger.components.data_conversion_utils import TypeConversionUtil
from redisgraph_bulk_loader.bulk_insert import bulk_insert
from roger.roger_db import RedisGraph
from string import Template

log = get_logger ()
config = get_config ()

class SchemaType(Enum):
    """ High level semantic metatdata concepts.
    Categories are classes in an ontological model like Biolink.
    Predicates are links between nodes. """
    CATEGORY = "category"
    PREDICATE = "predicate"
    
class FileFormat(Enum):
    """ File formats this module knows about. """
    JSON = "json"
    YAML = "yaml"

# @TODO move this to shared file between dug , roger etc...
class Util:

    @staticmethod
    def current_time_in_millis():
        """
        Get current time in milliseconds.
        
        Returns
        -------
        int
        Time in milliseconds
        
        """
        return int(round(time.time() * 1000))

    """ A just do it approach to getting data. """
    @staticmethod
    def read_file(path):
        """ Read a file. 
        :param path: Path to a file.
        """
        text = None
        with open(path, "r") as stream:
            text = stream.read ()
        return text
    
    @staticmethod
    def read_url(url):
        """ Read data from a URL.
        :param url: The URL to read. """
        return requests.get (url).text
    
    @staticmethod
    def read_data(path):
        """ Read data from a URL or File. HTTP(S) is the only supported protocol.
        :param path: A URL or file path. """
        text = None
        if Util.is_web(path):
            text = Util.read_url (path)
        else:
            text = Util.read_file (path)
        return text
    
    @staticmethod
    def read_object(path, key=None):
        """ Read on object from a path. 
        :param path: A URL or file path. Supports YAML and JSON depending on extension.
        :param key: A configuration key. This is prepended to the path if present.
        :raises ValueError: If the key is not in the configuration. """
        if key is not None:
            prefix = config[key]
            path = f"{prefix}/{path}" if Util.is_web(prefix) \
                else os.path.join (prefix, path)
        obj = None
        if path.endswith (".yaml") or path.endswith (".yml"):
            obj = yaml.safe_load (Util.read_data (path))
        elif path.endswith (".json"):
            obj = json.loads (Util.read_data (path))
        elif path.endswith(".pickle"):
            with open(file=path, mode="rb") as stream:
                obj = pickle.load(stream)
        elif path.endswith(".jsonl"):
            obj = Util.read_data(path)
        return obj

    @staticmethod
    def is_web (uri):
        """ The URI is a web URI (starts with http or https).
        :param uri: A URI """
        return uri.startswith("http://") or uri.startswith ("https://")
    
    @staticmethod
    def write_object (obj, path, key=None):
        """ Write an object to a path. YAML and JSON supported based on extension.
        :param obj: The object to write.
        :param path: The path to write to.
        :param key: The configuration key to prepend to the path.
        """
        """ Prepend a prefix from the configuration file if a key is given. """
        if key is not None:
            prefix = config[key]
            path = f"{prefix}/{path}" if Util.is_web(prefix) \
                else os.path.join (prefix, path)
        """ Ensure the directory to be written to exists. """
        dirname = os.path.dirname (path)
        if not os.path.exists (dirname):
            os.makedirs (dirname, exist_ok=True)
        """ Write the file in the specified format. """
        if path.endswith (".yaml") or path.endswith (".yml"):
            with open(path, 'w') as outfile:
                yaml.dump (obj, outfile)
        elif path.endswith (".json"):
            with open (path, "w") as stream:
                stream.write(str(json.dumps (obj).decode('utf-8')))
        elif path.endswith(".pickle"):
            with open (path, "wb") as stream:
                pickle.dump(obj, file=stream)
        elif path.endswith(".jsonl"):
            with open (path, "w", encoding="utf-8") as stream:
                stream.write(obj)
        else:
            """ Raise an exception if invalid. """
            raise ValueError (f"Unrecognized extension: {path}")

    @staticmethod
    def kgx_path (name):
        """ Form a KGX object path.
        :path name: Name of the KGX object. """
        return str(ROGER_DATA_DIR / "kgx" / name)

    @staticmethod
    def kgx_objects (format="json"):
        """ A list of KGX objects. """
        kgx_pattern = Util.kgx_path(f"**.{format}")
        return sorted(glob.glob (kgx_pattern))
    
    @staticmethod
    def merge_path (name):
        """ Form a merged KGX object path.
        :path name: Name of the merged KGX object. """
        return str(ROGER_DATA_DIR / 'merge' / name)

    @staticmethod
    def merged_objects ():
        """ A list of merged KGX objects. """
        merged_pattern = Util.merge_path("**.json")
        return sorted(glob.glob (merged_pattern))
        
    @staticmethod
    def schema_path (name):
        """ Path to a schema object.
        :param name: Name of the object to get a path for. """
        return str(ROGER_DATA_DIR / 'schema' / name)

    @staticmethod
    def bulk_path (name):
        """ Path to a bulk load object.
        :param name: Name of the object. """
        return str(ROGER_DATA_DIR / 'bulk' / name)

    @staticmethod
    def metrics_path (name):
        """
        Path to write metrics to
        :param name:
        :return:
        """
        return str(ROGER_DATA_DIR / "metrics" / name)

    @staticmethod
    def dug_kgx_path(name):
        return str(ROGER_DATA_DIR / "dug" / "kgx" / name)

    @staticmethod
    def dug_annotation_path(name):
        return str(ROGER_DATA_DIR / "dug" / "annotations" / name)

    @staticmethod
    def dug_expanded_concepts_path(name):
        return str(ROGER_DATA_DIR / 'dug' / 'expanded_concepts' / name)

    @staticmethod
    def dug_expanded_concept_objects():
        file_pattern = Util.dug_expanded_concepts_path(os.path.join('*','expanded_concepts.pickle'))
        return sorted(glob.glob(file_pattern))

    @staticmethod
    def dug_crawl_path(name):
        return str(ROGER_DATA_DIR / 'dug' / 'crawl' / name)

    @staticmethod
    def dug_kgx_objects():
        """ A list of dug KGX objects. """
        dug_kgx_pattern = Util.dug_kgx_path("**.json")
        return sorted(glob.glob(dug_kgx_pattern))

    @staticmethod
    def dug_concepts_objects():
        """ A list of dug annotation Objects. """
        concepts_file_path = Util.dug_annotation_path(os.path.join('*','concepts.pickle'))
        return sorted(glob.glob(concepts_file_path))

    @staticmethod
    def dug_elements_objects():
        """ A list of dug annotation Objects. """
        concepts_file_path = Util.dug_annotation_path(os.path.join('*', 'elements.pickle'))
        return sorted(glob.glob(concepts_file_path))

    @staticmethod
    def dug_input_files_path(name) -> pathlib.Path:
        path = ROGER_DATA_DIR / "dug" / "input_files" / name
        if not path.exists():
            log.info(f"Input file path: {path} does not exist, creating")
            path.mkdir(parents=True, exist_ok=True)
        else:
            log.info(f"Input file path: {path} already exists")
        return path

    @staticmethod
    def mkdir(path, is_dir=False):
        directory = os.path.dirname(path) if not is_dir else path
        if not os.path.exists(directory):
            os.makedirs(directory)

    @staticmethod
    def remove(path):
        if os.path.exists(path):
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)

    @staticmethod
    def clear_dir(path):
        Util.remove(path)
        Util.mkdir(path, is_dir=True)

    @staticmethod
    def dug_topmed_path(name):
        """ Topmed source files"""
        return Util.dug_input_files_path('topmed') / name

    @staticmethod
    def dug_topmed_objects():
        topmed_file_pattern = str(Util.dug_topmed_path("topmed_*.csv"))
        return sorted(glob.glob(topmed_file_pattern))

    @staticmethod
    def dug_dd_xml_path():
        """ Topmed source files"""
        return Util.dug_input_files_path('db_gap')

    @staticmethod
    def get_files_recursive(file_name_filter, current_dir):
        file_paths = []
        for child in current_dir.iterdir():
            if child.is_dir():
                file_paths += Util.get_files_recursive(file_name_filter, child)
                continue
            if not file_name_filter(child.name):
                continue
            else:
                file_paths += [child]
        return file_paths

    @staticmethod
    def dug_dd_xml_objects():
        file_path = Util.dug_dd_xml_path()
        files = Util.get_files_recursive(lambda file_name: not file_name.startswith('._') and file_name.endswith('.xml'), file_path)
        return sorted([str(f) for f in files])

    @staticmethod
    def copy_file_to_dir(file_location, dir_name):
        return shutil.copy(file_location, dir_name)

    @staticmethod
    def read_schema (schema_type: SchemaType):
        """ Read a schema object.
        :param schema_type: Schema type of the object to read. """
        path = Util.schema_path (f"{schema_type.value}-schema.json")
        return Util.read_object (path)
    
    @staticmethod
    def get_uri (path, key):
        """ Build a URI.
        :param path: The path of an object.
        :param key: The key of a configuration value to prepend to the object. """
        # Incase config has http://..../ or http://... remove / and add back to
        # avoid double http://...//
        root_url = config[key].rstrip('/')
        return f"{root_url}/{path}"

    @staticmethod
    def get_relative_path (path):
        return os.path.join (os.path.dirname (__file__), path)

    @staticmethod
    def read_relative_object (path):
        return Util.read_object (Util.get_relative_path(path))

    @staticmethod
    def trunc(text, limit):
        return ('..' + text[-limit-2:]) if len(text) > limit else text

    @staticmethod
    def is_up_to_date (source, targets):
        target_time_list = [ os.stat (f).st_mtime for f in targets if os.path.exists(f) ]
        if len(target_time_list) == 0:
            log.debug (f"no targets found")
            return False
        source = [ os.stat (f).st_mtime for f in source if os.path.exists (f) ]
        if len(source) == 0:
            log.debug ("no source found. up to date")
            return True
        return max(source) < min(target_time_list)

    @staticmethod
    def json_line_iter(jsonl_file_path):
        f = open(file=jsonl_file_path, mode='r', encoding='utf-8')
        for line in f:
            yield json.loads(line)
        f.close()
        
class KGXModel:
    """ Abstractions for transforming Knowledge Graph Exchange formatted data. """
    def __init__(self, biolink=None, config=None):
        if not config:
            config = get_config()
        self.config = config
        self.biolink_version = self.config.kgx.biolink_model_version
        log.debug(f"Trying to get biolink version : {self.biolink_version}")
        if biolink == None:
            self.biolink = BiolinkModel(self.biolink_version)
        else:
            self.biolink = biolink
        self.redis_conn = redis.Redis(
                    host=self.config.redisgraph.host,
                    port=self.config.redisgraph.port,
                    password=self.config.redisgraph.password,
                    db=1) # uses db1 for isolation @TODO make this config param.
        self.enable_metrics = self.config.get('enable_metrics', False)

    def get_kgx_json_format(self, files: list, dataset_version: str):
        """
        Gets Json formatted kgx files. These files have a the following structure:
        {"nodes": [{"id":"..."},...], "edges": [{"id":...},...}] }

        Parameters
        ----------
        files : list of file names
        dataset_version : dataset version from dataset meta-data information

        Returns None
        -------

        """
        all_kgx_files = []
        log.info(f"getting {files}")
        for file_name in files:
            start = Util.current_time_in_millis()
            file_name = dataset_version + "/" + file_name
            file_url = Util.get_uri(file_name, "kgx_base_data_uri")
            subgraph_basename = os.path.basename(file_name)
            subgraph_path = Util.kgx_path(subgraph_basename)
            all_kgx_files.append(subgraph_path)
            if os.path.exists(subgraph_path):
                log.info(f"cached kgx: {subgraph_path}")
                continue
            subgraph = Util.read_object(file_url)
            Util.write_object(subgraph, subgraph_path)
            total_time = Util.current_time_in_millis() - start
            edges = len(subgraph['edges'])
            nodes = len(subgraph['nodes'])
            log.debug("wrote {:>45}: edges:{:>7} nodes: {:>7} time:{:>8}".format(
                Util.trunc(subgraph_path, 45), edges, nodes, total_time))
        return all_kgx_files

    def get_kgx_jsonl_format(self, files, dataset_version):
        """
        gets pairs of jsonl formatted kgx files. Files is expected to have
        all the pairs. I.e if kgx_1_nodes.jsonl exists its expected that kgx_1_edges.jsonl
        exists in the same path.
        File names should have strings *nodes*.jsonl and *edges*.jsonl.
        Parameters
        ----------
        files
        dataset_version

        Returns
        -------

        """
        # make a paired list
        paired_up = []
        log.info(f"getting {files}")
        for file_name in files:
            if "nodes" in file_name:
                paired_up.append([file_name, file_name.replace('nodes', 'edges')])
        error = False
        # validate that all pairs exist
        all_kgx_files = []
        if len(files) / 2 != len(paired_up):
            log.error("Error paired up kgx jsonl files don't match list of files specified in metadata.yaml")
            error = True
        for pairs in paired_up:
            if pairs[0] not in files:
                log.error(f"{pairs[0]} not in original list of files from metadata.yaml")
                error = True
            if pairs[1] not in files:
                error = True
                log.error(f"{pairs[1]} not in original list of files from metadata.yaml")
        if error:
            raise Exception("Metadata.yaml has inconsistent jsonl files")

        log.info(f"Paired files : {paired_up}")

        for pairs in paired_up:
            nodes = 0
            edges = 0
            start = Util.current_time_in_millis()
            for p in pairs:
                file_name = dataset_version + "/" + p
                file_url = Util.get_uri(file_name, "kgx_base_data_uri")
                subgraph_basename = os.path.basename(file_name)
                subgraph_path = Util.kgx_path(subgraph_basename)
                all_kgx_files.append(subgraph_path)
                if os.path.exists(subgraph_path):
                    log.info(f"cached kgx: {subgraph_path}")
                    continue
                data = Util.read_object(file_url)
                Util.write_object(data, subgraph_path)
                if "edges" in p:
                    edges = len(data.split('\n'))
                else:
                    nodes = len(data.split('\n'))
            total_time = Util.current_time_in_millis() - start
            log.debug("wrote {:>45}: edges:{:>7} nodes: {:>7} time:{:>8}".format(
                Util.trunc(subgraph_path, 45), edges, nodes, total_time))
            return all_kgx_files

    def get (self, dataset_version = "v1.0"):
        """ Read metadata for KGX files and downloads them locally.
        :param dataset_version: Data version to operate on.
        """
        metadata = Util.read_relative_object ("../metadata.yaml")
        data_set_list = self.config.kgx.data_sets
        for item in metadata['kgx']['versions']:
            if item['version'] == dataset_version and item['name'] in data_set_list:
                log.info(f"Getting KGX dataset {item['name']} , version {item['version']}, format {item['format']}")
                if item['format'] == 'json':
                    kgx_files_remote = self.get_kgx_json_format(item['files'], item['version'])
                elif item['format'] == 'jsonl':
                    kgx_files_remote = self.get_kgx_jsonl_format(item['files'], item['version'])
                else:
                    raise ValueError(f"Unrecognized format in metadata.yaml: {item['format']}, valid formats are `json` "
                                     f"and `jsonl`.")
        # Fetchs kgx generated from Dug Annotation workflow.
        new_files = self.fetch_dug_kgx() + kgx_files_remote
        all_files_in_dir = Util.kgx_objects(format="json") + Util.kgx_objects(format="jsonl")
        files_to_remove = [x for x in all_files_in_dir if x not in new_files]
        if len(files_to_remove):
            log.info(f"Found some old files to remove from kgx dir : {files_to_remove}")
            for file in files_to_remove:
                Util.remove(file)
                log.info(f"removed {file}")
        log.info("Done.")



    def fetch_dug_kgx(self):
        """
        Copies files from dug output dir to roger kgx dir.
        :return:
        """
        dug_kgx_files = Util.dug_kgx_objects()
        all_kgx_files = []
        log.info(f"Coping dug KGX files to {Util.kgx_path('')}. Found {len(dug_kgx_files)} kgx files to copy.")
        for file in dug_kgx_files:
            file_name = ntpath.basename(file)
            dest = Util.kgx_path(file_name)
            all_kgx_files.append(dest)
            Util.write_object({}, dest)
            log.info(f"Copying from {file} to {dest}.")
            Util.copy_file_to_dir(file, dest)
        log.info("Done coping dug KGX files.")
        return all_kgx_files

    def create_nodes_schema(self):
        """
        Extracts schema for nodes based on biolink leaf types
        :return:
        """

        category_schemas = defaultdict(lambda: None)
        category_error_nodes = set()
        merged_nodes_file = Util.merge_path("nodes.jsonl")
        log.info(f"Processing : {merged_nodes_file}")
        for node in Util.json_line_iter(merged_nodes_file):
            if not node['category']:
                category_error_nodes.add(node['id'])
                node['category'] = [BiolinkModel.root_type]
            node_type = self.biolink.get_leaf_class(node['category'])
            category_schemas[node_type] = category_schemas.get(node_type, {})
            for k in node.keys():
                current_type = type(node[k]).__name__
                if k not in category_schemas[node_type]:
                    category_schemas[node_type][k] = current_type
                else:
                    previous_type = category_schemas[node_type][k]
                    category_schemas[node_type][k] = TypeConversionUtil.compare_types(previous_type, current_type)
        if len(category_error_nodes):
            log.warn(f"some nodes didn't have category assigned. KGX file has errors."
                      f"Nodes {len(category_error_nodes)}."
                      f"Showing first 10: {list(category_error_nodes)[:10]}."
                      f"These will be treated as {BiolinkModel.root_type}.")
        """ Write node schemas. """
        self.write_schema(category_schemas, SchemaType.CATEGORY)

    def create_edges_schema(self):
        """
        Create unified schema for all edges in an edges jsonl file.
        :return:
        """
        predicate_schemas = defaultdict(lambda: None)
        merged_edges_file = Util.merge_path("edges.jsonl")
        """ Infer predicate schemas. """
        for edge in Util.json_line_iter(merged_edges_file):
            predicate = edge['predicate']
            predicate_schemas[predicate] = predicate_schemas.get(predicate, {})
            for k in edge.keys():
                current_type = type(edge[k]).__name__
                if k not in predicate_schemas[predicate]:
                    predicate_schemas[predicate][k] = current_type
                else:
                    previous_type = predicate_schemas[predicate][k]
                    predicate_schemas[predicate][k] = TypeConversionUtil.compare_types(previous_type, current_type)
        self.write_schema(predicate_schemas, SchemaType.PREDICATE)

    def create_schema (self):
        """
        Determine the schema of each type of object. We have to do this to make it possible
        to write tabular data. Need to know all possible columns in advance and correct missing
        fields.
        """
        if self.schema_up_to_date():
            log.info (f"schema is up to date.")
            return

        self.create_nodes_schema()
        self.create_edges_schema()

    def schema_up_to_date (self):
        return Util.is_up_to_date (
            source=Util.kgx_objects (),
            targets=[
                Util.schema_path (f"{SchemaType.PREDICATE.value}-schema.json"),
                Util.schema_path (f"{SchemaType.PREDICATE.value}-schema.json")
            ])
                
    def write_schema (self, schema, schema_type: SchemaType):
        """ Output the schema file. 
        :param schema: Schema to get keys from.
        :param schema_type: Type of schema to write. """
        file_name = Util.schema_path (f"{schema_type.value}-schema.json")
        log.info (f"writing schema: {file_name}")
        dictionary = { k : v for k, v in schema.items () }
        Util.write_object (dictionary, file_name)

    def read_items_from_redis(self, ids):
        chunk_size = 10_000 # batch for pipeline
        pipeline = self.redis_conn.pipeline()
        response = {}
        chunked_ids = [ids[start: start + chunk_size] for start in range(0, len(ids), chunk_size)]
        for ids in chunked_ids:
            for i in ids:
                pipeline.get(i)
            result = pipeline.execute()
            for i, res in zip(ids, result):
                if res:
                    response.update({i: json.loads(res)})
        return response

    def write_items_to_redis(self, items):
        chunk_size = 10_000  # batch for redis beyond this cap it might not be optimal, according to redis docs
        pipeline = self.redis_conn.pipeline()
        all_keys = list(items.keys())
        chunked_keys = [all_keys[start: start + chunk_size] for start in range(0, len(all_keys), chunk_size)]
        for keys in chunked_keys:
            for key in keys:
                pipeline.set(key, json.dumps(items[key]))
            pipeline.execute()

    def delete_keys(self, items):
        # deletes keys
        chunk_size = 10_000
        pipeline = self.redis_conn.pipeline()
        all_keys = list(items)
        chunked_keys = [all_keys[start: start + chunk_size] for start in range(0, len(all_keys), chunk_size)]
        for keys in chunked_keys:
            for key in keys:
                pipeline.delete(key)
            pipeline.execute()

    def delete_all_keys(self):
        all_keys = self.redis_conn.keys("*")
        log.info(f"found {len(all_keys)} to delete.")
        self.delete_keys(all_keys)
        log.info(f"deleted keys.")

    def write_redis_back_to_jsonl(self, file_name, redis_key_pattern):
        Util.mkdir(file_name)
        with open(file_name, 'w', encoding='utf-8') as f:
            start = time.time()
            keys = self.redis_conn.keys(redis_key_pattern)
            log.info(f"Grabbing {redis_key_pattern} from redis too {time.time() - start}")
            chunk_size = 500_000
            chunked_keys = [keys[start: start + chunk_size] for start in range(0, len(keys), chunk_size) ]
            for chunk in chunked_keys:
                items = self.read_items_from_redis(chunk)
                self.delete_keys(chunk)
                # transform them into lines
                items = [json.dumps(items[x]).decode('utf-8') + '\n' for x in items]
                f.writelines(items)
                log.info(f"wrote : {len(items)}")

    def kgx_merge_dict(self, dict_1, dict_2):
        # collect values that are same first
        merged = {}
        # if properties match up with value treat as one
        # get dict_1 intersection dict_2 ,
        merged = {x: dict_1[x] for x in dict_1 if dict_1.get(x) == dict_2.get(x)}
        # get dict_1 disjoint dict 2
        unique_dict_1_props = {x: dict_1[x] for x in dict_1 if x not in merged.keys()}
        # get dict_2 disjoint dict 1
        unique_dict_2_props = {x: dict_2[x] for x in dict_2 if x not in merged.keys()}
        merged.update(kgx_merge_dict(unique_dict_1_props, unique_dict_2_props))
        for keys in merged:
            attribute = merged[keys]
            # When mergeing array's for bulk loading
            # we have to make sure that items in lists
            # don't contain single-quotes.
            # Single quotes in array items break parsing of arrays on bulk loading
            # downstream.
            if isinstance(attribute, list):
                new_attribute = []
                for value in attribute:
                    if isinstance(value, str):
                        value = value.replace("'", '`')
                    new_attribute.append(value)
                attribute = new_attribute
            merged[keys] = attribute
        return merged

    def sort_node_types(self, node_dict):
        categories = node_dict.get('category')
        if not categories:
            return node_dict
        leaf_type = self.biolink.get_leaf_class(categories)
        # brings leaf class in the top
        categories = [leaf_type] + [x for x in categories if x != leaf_type]
        node_dict['category'] = categories
        return node_dict

    def merge_node_and_edges (self, nodes, edges, current_metric , data_set_name ):
        read_time = current_metric['read_kgx_file_time']
        total_time = current_metric['total_processing_time']
        # prefix keys for fetching back and writing to file.
        nodes = {f"node-{node['id']}": self.sort_node_types(node) for node in nodes}
        edges = {f"edge-{edge['subject']}-{edge['object']}-{edge['predicate']}": edge for edge in
                 edges}
        read_from_redis_time = time.time()
        # read nodes and edges scoped to current file
        nodes_in_redis = self.read_items_from_redis(list(nodes.keys()))
        edges_in_redis = self.read_items_from_redis(list(edges.keys()))
        read_from_redis_time = time.time() - read_from_redis_time
        current_metric['read_redis_time'] = read_from_redis_time
        merge_time = time.time()
        log.info(f"Found matching {len(nodes_in_redis)} nodes {len(edges_in_redis)} edges from redis...")
        for node_id in nodes_in_redis:
            nodes[node_id] = self.kgx_merge_dict(nodes[node_id], nodes_in_redis[node_id])
        for edge_id in edges_in_redis:
            edges[edge_id] = self.kgx_merge_dict(edges[edge_id], edges_in_redis[edge_id])
        # add predicate labels to edges;
        for edge_id in edges:
            edges[edge_id]['predicate_label'] = self.biolink.get_label(edges[edge_id]['predicate'])
        merge_time = time.time() - merge_time
        current_metric['merge_time'] = merge_time
        write_to_redis_time = time.time()
        self.write_items_to_redis(nodes)
        self.write_items_to_redis(edges)
        write_to_redis_time = time.time() - write_to_redis_time
        current_metric['write_to_redis_time'] = write_to_redis_time
        log.debug(
            "path {:>45} read_file:{:>5} read_nodes_from_redis:{:>7} merge_time:{:>3} write_nodes_to_redis: {"
            ":>3}".format(
                Util.trunc(data_set_name, 45), read_time, read_from_redis_time, merge_time, write_to_redis_time))
        total_file_processing_time = time.time() - total_time
        current_metric['total_processing_time'] = total_file_processing_time
        current_metric['total_nodes_in_kgx_file'] = len(nodes)
        current_metric['total_edges_in_kgx_file'] = len(edges)
        current_metric['nodes_found_in_redis'] = len(nodes_in_redis)
        current_metric['edges_found_in_redis'] = len(edges_in_redis)
        log.info(f"processing {data_set_name} took {total_file_processing_time}")
        return current_metric

    def merge (self):
        """ Merge nodes. Would be good to have something less computationally intensive. """
        data_set_version = self.config.get('kgx', {}).get('dataset_version')
        metrics = {}
        start = time.time()
        json_format_files = Util.kgx_objects("json")
        jsonl_format_files = set([
            x.replace(f'nodes_{data_set_version}.jsonl', '').replace(f'edges_{data_set_version}.jsonl', '') for x in Util.kgx_objects("jsonl")
        ])

        log.info("Deleting any redis merge keys from previous run....")
        self.delete_all_keys()

        for file in json_format_files:
            current_metric = {}
            total_time = read_time = time.time()
            current_kgx_data = Util.read_object(file)
            read_time = time.time() - read_time
            current_metric['read_kgx_file_time'] = read_time
            current_metric['total_processing_time'] = total_time
            self.merge_node_and_edges(nodes=current_kgx_data['nodes'],
                                      edges=current_kgx_data['edges'],
                                      current_metric=current_metric,
                                      data_set_name=file)

        for file in jsonl_format_files:
            current_metric = {}
            total_time = read_time = time.time()
            edges = Util.json_line_iter(Util.kgx_path(file + f'edges_{data_set_version}.jsonl'))
            nodes = Util.json_line_iter(Util.kgx_path(file + f'nodes_{data_set_version}.jsonl'))
            read_time = time.time() - read_time
            current_metric['read_kgx_file_time'] = read_time
            current_metric['total_processing_time'] = total_time
            self.merge_node_and_edges(nodes=nodes,
                                      edges=edges,
                                      current_metric=current_metric,
                                      data_set_name=file)

        log.info(f"total time for dumping to redis : {time.time() - start}")

        # now we have all nodes and edges merged in redis we scan the whole redis back to disk
        write_merge_metric = {}
        t = time.time()
        log.info("getting all nodes")
        start_nodes_jsonl = time.time()
        nodes_file_path = Util.merge_path("nodes.jsonl")
        self.write_redis_back_to_jsonl(nodes_file_path, "node-*")
        log.info(f"writing nodes to took : {time.time() - start_nodes_jsonl}")
        write_merge_metric['nodes_writing_time'] = time.time() - start_nodes_jsonl
        start_edge_jsonl = time.time()
        log.info("getting all edges")
        edge_output_file_path = Util.merge_path("edges.jsonl")
        self.write_redis_back_to_jsonl(edge_output_file_path, "edge-*")
        write_merge_metric['edges_writing_time'] = time.time() - start_edge_jsonl
        log.info(f"writing edges took: {time.time() - start_edge_jsonl}")
        write_merge_metric['total_time'] = time.time() - t
        metrics['write_jsonl'] = write_merge_metric
        metrics['total_time'] = time.time() - start
        log.info(f"total took: {time.time() - start}")
        if self.enable_metrics:
            path = Util.metrics_path('merge_metrics.yaml')
            Util.write_object(metrics, path)


class BiolinkModel:
    root_type = 'biolink:NamedThing'

    def __init__(self, bl_version='1.5.0'):
        self.bl_url = f'https://raw.githubusercontent.com/biolink/biolink-model/{bl_version}/biolink-model.yaml'
        self.toolkit = Toolkit(self.bl_url)

    def find_biolink_leaves(self, biolink_concepts):
        """
        Given a list of biolink concepts, returns the leaves removing any parent concepts.
        :param biolink_concepts: list of biolink concepts
        :return: leave concepts.
        """
        ancestry_set = set()
        all_mixins_in_tree = set()
        all_concepts = set(biolink_concepts)
        # Keep track of things like "MacromolecularMachine" in current datasets
        # @TODO remove this and make nodes as errors
        unknown_elements = set()
        for x in all_concepts:
            current_element = self.toolkit.get_element(x)
            mixins = set()
            if current_element:
                if 'mixins' in current_element and len(current_element['mixins']):
                    for m in current_element['mixins']:
                        mixins.add(self.toolkit.get_element(m).class_uri)
            else:
                unknown_elements.add(x)
            ancestors = set(self.toolkit.get_ancestors(x, reflexive=False, formatted=True))
            ancestry_set = ancestry_set.union(ancestors)
            all_mixins_in_tree = all_mixins_in_tree.union(mixins)
        leaf_set = all_concepts - ancestry_set - all_mixins_in_tree - unknown_elements
        return leaf_set

    def get_leaf_class (self, names):
        """ Return the leaf classes in the provided list of names. """
        leaves = list(self.find_biolink_leaves(names))
        return leaves[0]

    def get_label(self, class_name):
        element = self.toolkit.get_element(class_name)
        if element:
            name = element.name
            return name
        return class_name.replace("biolink:", "").replace("_", " ")


class BulkLoad:
    """ Tools for creating a Redisgraph bulk load dataset. """
    def __init__(self, biolink, config=None):
        self.biolink = biolink
        if not config:
            config = get_config()
        self.config = config
        separator = self.config.get('bulk_loader',{}).get('separator', '|')
        self.separator = chr(separator) if isinstance(separator, int) else separator

    def tables_up_to_date (self):
        return Util.is_up_to_date (
            source=[
                Util.schema_path (f"{SchemaType.PREDICATE.value}-schema.json"),
                Util.schema_path (f"{SchemaType.PREDICATE.value}-schema.json")
            ] + Util.merged_objects (),
            targets=glob.glob (Util.bulk_path ("nodes/**.csv")) + \
            glob.glob (Util.bulk_path ("edges/**.csv")))

    def create_nodes_csv_file(self):
        if self.tables_up_to_date ():
            log.info ("up to date.")
            return
        # clear out previous data
        bulk_path = Util.bulk_path("nodes")
        if os.path.exists(bulk_path):
            shutil.rmtree(bulk_path)
        categories_schema = Util.read_schema (SchemaType.CATEGORY)
        state = defaultdict(lambda: None)
        log.info(f"processing nodes")
        """ Write node data for bulk load. """

        categories = defaultdict(lambda: [])
        category_error_nodes = set()
        merged_nodes_file = Util.merge_path("nodes.jsonl")
        counter = 1
        for node in Util.json_line_iter(merged_nodes_file):
            if not node['category']:
                category_error_nodes.add(node['id'])
                node['category'] = [BiolinkModel.root_type]
            index = self.biolink.get_leaf_class(node['category'])
            categories[index].append(node)
            if len(category_error_nodes):
                log.error(f"some nodes didn't have category assigned. KGX file has errors."
                          f"Nodes {len(category_error_nodes)}. They will be typed {BiolinkModel.root_type}"
                          f"Showing first 10: {list(category_error_nodes)[:10]}.")
            # flush every 100K
            if counter % 100_000 == 0:
                self.write_bulk(Util.bulk_path("nodes"), categories, categories_schema,
                                state=state, is_relation=False)
                # reset variables.
                category_error_nodes = set()
                categories = defaultdict(lambda: [])
            counter += 1
        # write back if any thing left.
        if len(categories):
            self.write_bulk(Util.bulk_path("nodes"), categories, categories_schema,
                            state=state, is_relation=False)

    def create_edges_csv_file(self):
        """ Write predicate data for bulk load. """
        if self.tables_up_to_date ():
            log.info ("up to date.")
            return
        # Clear out previous data
        bulk_path = Util.bulk_path("edges")
        if os.path.exists(bulk_path):
            shutil.rmtree(bulk_path)
        predicates_schema = Util.read_schema(SchemaType.PREDICATE)
        predicates = defaultdict(lambda: [])
        edges_file = Util.merge_path('edges.jsonl')
        counter = 1
        state = {}
        for edge in Util.json_line_iter(edges_file):
            predicates[edge['predicate']].append(edge)
            # write out every 100K , to avoid large predicate dict.
            if counter % 100_000 == 0:
                self.write_bulk(Util.bulk_path("edges"), predicates, predicates_schema, state=state, is_relation=True)
                predicates = defaultdict(lambda : [])
            counter += 1
        # if there are some items left (if loop ended before counter reached the specified value)
        if len(predicates):
            self.write_bulk(Util.bulk_path("edges"), predicates, predicates_schema, state=state, is_relation=True)

    @staticmethod
    def create_redis_schema_header(attributes: dict, is_relation=False):
        """
        Creates col headers for csv to be used by redis bulk loader by assigning redis types
        :param attributes: dictionary of data labels with values as python type strings
        :param separator: CSV separator
        :return: list of attributes where each item  is attributeLabel:redisGraphDataType
        """
        redis_type_conversion_map = {
            'str': 'STRING',
            'float': 'FLOAT',  # Do we need to handle double
            'int': 'INT',
            'bool': 'BOOL',
            'list': 'ARRAY'
        }
        col_headers = []
        format_for_redis = lambda label, typ: f'{label}:{typ}'
        for attribute, attribute_type in attributes.items():
            col_headers.append(format_for_redis(attribute, redis_type_conversion_map[attribute_type]))
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
        """
        Groups items into a dictionary where the keys are sets of attributes set for all
        items accessed in that key.
        Eg. { set(id,name,category): [{id:'xx0',name:'bbb', 'category':['type']}....
        {id:'xx1', name:'bb2', category: ['type1']}] }
        :param objects: list of nodes or edges
        :param processed_object_ids: ids of object to skip since they are processed.
        :return: dictionary grouping based on set attributes
        """
        clustered_by_set_values = {}
        improper_keys = set()
        value_set_test = lambda x: True if (x is not None and x != [] and x != '') else False
        for obj in objects:
            # redis bulk loader needs columns not to include ':'
            # till backticks are implemented we should avoid these.
            key_filter = lambda k:  ':' not in k
            keys_with_values = frozenset([k for k in obj.keys() if value_set_test(obj[k]) and key_filter(k)])
            for key in [k for k in obj.keys() if obj[k] and not key_filter(k)]:
                improper_keys.add(key)
            # group by attributes that have values. # Why?
            # Redis bulk loader has one issue
            # imagine we have {'name': 'x'} , {'name': 'y', 'is_metabolite': true}
            # we have a common schema name:STRING,is_metabolite:BOOL
            # values `x, ` and `y,true` but x not having value for is_metabolite is not handled
            # well, redis bulk loader says we should give it default if we were to enforce schema
            # but due to the nature of the data assigning defaults is very not an option.
            # hence grouping data into several csv's might be the right way (?)
            if obj['id'] not in processed_object_ids:
                clustered_by_set_values[keys_with_values] = clustered_by_set_values.get(keys_with_values, [])
                clustered_by_set_values[keys_with_values].append(obj)
        return clustered_by_set_values, improper_keys

    def write_bulk(self, bulk_path, obj_map, schema, state={}, is_relation=False):
        """ Write a bulk load group of objects.
        :param bulk_path: Path to the bulk loader object to write.
        :param obj_map: A map of biolink type to list of objects.
        :param schema: The schema (nodes or predicates) containing identifiers.
        :param state: Track state of already written objects to avoid duplicates.
        """

        os.makedirs (bulk_path, exist_ok=True)
        processed_objects_id = state.get('processed_id', set())
        called_x_times = state.get('called_times', 0)
        called_x_times += 1
        for key, objects in obj_map.items ():
            if len(objects) == 0:
                continue
            all_keys = schema[key]
            """ Make all objects conform to the schema. """
            clustered_by_set_values, improper_redis_keys = self.group_items_by_attributes_set(objects,
                                                                                              processed_objects_id)

            if len(improper_redis_keys):
                log.warning(f"The following keys were skipped since they include conflicting `:`"
                            f" that would cause errors while bulk loading to redis."
                            f"{improper_redis_keys}")
            for index, set_attributes in enumerate(clustered_by_set_values.keys()):
                items = clustered_by_set_values[set_attributes]
                # When parted files are saved let the file names be collected here
                state['file_paths'] = state.get('file_paths', {})
                state['file_paths'][key] = state['file_paths'].get(key, {})
                out_file = state['file_paths'][key][set_attributes] = state['file_paths']\
                    .get(key, {})\
                    .get(set_attributes, '')
                # When calling write bulk , lets say we have processed some
                # chemicals from file 1 and we start processing file 2
                # if we are using just index then we might (rather will) end up adding
                # records to the wrong file so we need this to be unique as possible
                # by adding called_x_times , if we already found out-file from state obj
                # we are sure that the schemas match.

                # biolink:<TYPE> is not valid name so we need to remove :
                file_key = key.replace(':', '~')

                out_file = f"{bulk_path}/{file_key}.csv-{index}-{called_x_times}" if not out_file else out_file
                state['file_paths'][key][set_attributes] = out_file  # store back file name
                new_file = not os.path.exists(out_file)
                keys_for_header = {x: all_keys[x] for x in all_keys if x in set_attributes}
                redis_schema_header = self.create_redis_schema_header(keys_for_header, is_relation)
                with open(out_file, "a", encoding='utf-8') as stream:
                    if new_file:
                        state['file_paths'][key][set_attributes] = out_file
                        log.info(f"  --creating {out_file}")
                        stream.write(self.separator.join(redis_schema_header))
                        stream.write("\n")
                    else:
                        log.info(f"  --appending to {out_file}")
                    """ Write fields, skipping duplicate objects. """
                    for obj in items:
                        oid = str(obj['id'])
                        if oid in processed_objects_id:
                            continue
                        processed_objects_id.add(oid)
                        """ Add ID / START_ID / END_ID depending"""
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
                        # uses redis schema header to preserve order when writing lines out.
                        for column_name in redis_schema_header:
                            # last key is the type
                            obj_key = ':'.join(column_name.split(':')[:-1])
                            value = obj[obj_key]

                            if obj_key not in internal_id_fields:
                                current_type = type(value).__name__
                                expected_type = all_keys[obj_key]
                                # cast it if it doesn't match type in schema keys i.e all_keys
                                value = TypeConversionUtil.cast(obj[obj_key], all_keys[obj_key]) \
                                    if expected_type != current_type else value
                            # escape quotes .
                            values.append(str(value).replace("\"", "\\\""))
                        s = self.separator.join(values)
                        stream.write(s)
                        stream.write("\n")
        state['processed_id'] = processed_objects_id
        state['called_times'] = called_x_times

    def insert (self):

        redisgraph = {
            'host': os.getenv('REDIS_HOST'),
            'port': os.getenv('REDIS_PORT', 6379),
            'password': os.getenv('REDIS_PASSWORD'),
            'graph': os.getenv('REDIS_GRAPH'),
        }
        redisgraph = self.config.redisgraph
        nodes = sorted(glob.glob (Util.bulk_path ("nodes/**.csv*")))
        edges = sorted(glob.glob (Util.bulk_path ("edges/**.csv*")))
        graph = redisgraph['graph']
        log.info(f"bulk loading \n  nodes: {nodes} \n  edges: {edges}")

        try:
            log.info (f"deleting graph {graph} in preparation for bulk load.")
            db = self.get_redisgraph()
            db.redis_graph.delete ()
        except redis.exceptions.ResponseError:
            log.info("no graph to delete")
            
        log.info (f"bulk loading graph: {graph}")        
        args = []
        if len(nodes) > 0:
            bulk_path_root = Util.bulk_path('nodes') + os.path.sep
            nodes_with_type = [ f"{ x.replace(bulk_path_root, '').split('.')[0].replace('~', ':')} {x}"
                                for x in nodes ]
            args.extend(("-N " + " -N ".join(nodes_with_type)).split())
        if len(edges) > 0:
            bulk_path_root = Util.bulk_path('edges') + os.path.sep
            edges_with_type = [ f"{x.replace(bulk_path_root, '').strip(os.path.sep).split('.')[0].replace('~', ':')} {x}"
                               for x in edges]
            args.extend(("-R " + " -R ".join(edges_with_type)).split())
        args.extend([f"--separator={self.separator}"])
        args.extend([f"--host={redisgraph['host']}"])
        args.extend([f"--port={redisgraph['port']}"])
        args.extend([f"--password={redisgraph['password']}"])
        args.extend(['--enforce-schema'])
        args.extend([f"{redisgraph['graph']}"])
        """ standalone_mode=False tells click not to sys.exit() """
        log.debug(f"Calling bulk_insert with extended args: {args}")
        try:
            bulk_insert (args, standalone_mode=False)
        except Exception as e:
            log.error(f"Unexpected {e.__class__.__name__}: {e}")
            raise

    def get_redisgraph(self):
        return RedisGraph(
            host=self.config.redisgraph.host,
            port=self.config.redisgraph.port,
            password=self.config.redisgraph.password,
            graph=self.config.redisgraph.graph,
        )

    def validate(self):

        db = self.get_redisgraph()
        validation_queries = config.get('validation', {}).get('queries', [])
        for key, query in validation_queries.items ():
            text = query['query']
            name = query['name']
            args = query.get('args', [{}])
            for arg in args:
                start = Util.current_time_in_millis ()
                instance = Template (text).safe_substitute (arg)
                db.query (instance)
                duration = Util.current_time_in_millis () - start
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
            raise ConnectionError(f"Attempting to contact {tranql_endpoint} failed due to connection error. "
                      f"Please check status of Tranql server.")


class Roger:
    """ Consolidate Roger functionality for a cleaner interface. """

    def __init__(self, to_string=False, config=None):
        """ Initialize.
        :param to_string: Log messages to a string, available as self.log_stream.getvalue() 
        after execution completes.
        """
        import logging
        self.has_string_handler = to_string
        if not config:
            config = get_config()
        self.config = config
        if to_string:
            """ Add a stream handler to enable to_string. """
            self.log_stream = StringIO()
            self.string_handler = logging.StreamHandler (self.log_stream)
            log.addHandler (self.string_handler)
        self.biolink = BiolinkModel (config.kgx.biolink_model_version)
        self.kgx = KGXModel (self.biolink, config=config)
        self.bulk = BulkLoad (self.biolink, config=config)

    def __enter__(self):
        """ Implement Python's Context Manager interface. """
        return self
    
    def __exit__(self, exception_type, exception_value, traceback):
        """ Implement Python's Context Manager interface. We use this finalizer
        to detach the stream handler appended in the constructor.
        :param exception_type: Type of exception, if one occurred.
        :param exception_value: The exception, if one occurred.
        :param traceback: The stack trace explaining the exception. 
        """
        if exception_type or exception_value or traceback:
            log.error (msg="Error:", exc_info=(exception_type, exception_value, traceback))
        if self.has_string_handler:
            log.removeHandler (self.string_handler)
        
class RogerUtil:
    """ An interface abstracting Roger's inner workings to make it easier to
    incorporate into external tools like workflow engines. """
    @staticmethod
    def get_kgx (to_string=False, config=None):
        output = None
        log.debug("Getting KGX method called.")
        with Roger (to_string, config=config) as roger:
            dataset_version=config.get('kgx', {}).get('dataset_version')
            roger.kgx.get (dataset_version=dataset_version)
            output = roger.log_stream.getvalue () if to_string else None
        return output
    
    @staticmethod
    def create_schema (to_string=False, config=None):
        o1 = RogerUtil.create_nodes_schema(to_string=to_string, config=config)
        o2 = RogerUtil.create_edges_schema(to_string=to_string, config=config)
        output = (o1 + o2 ) if to_string else None
        return output

    @staticmethod
    def create_edges_schema(to_string=False, config=None):
        output = None
        with Roger(to_string, config=config) as roger:
            roger.kgx.create_edges_schema()
            output = roger.log_stream.getvalue() if to_string else None
        return output

    @staticmethod
    def create_nodes_schema(to_string=False, config=None):
        output = None
        with Roger(to_string, config=config) as roger:
            roger.kgx.create_nodes_schema()
            output = roger.log_stream.getvalue() if to_string else None
        return output

    @staticmethod
    def merge_nodes (to_string=False, config=None):
        output = None
        with Roger (to_string, config=config) as roger:
            roger.kgx.merge ()
            output = roger.log_stream.getvalue () if to_string else None
        return output
    
    @staticmethod
    def create_bulk_load (to_string=False, config=None):
        o1 = RogerUtil.create_bulk_nodes(to_string=to_string, config=config)
        o2 = RogerUtil.create_bulk_edges(to_string=to_string, config=config)
        output = (o1 + o2) if to_string else None
        return output

    @staticmethod
    def create_bulk_nodes(to_string=False, config=None):
        output = None
        with Roger(to_string, config=config) as roger:
            roger.bulk.create_nodes_csv_file()
            output = roger.log_stream.getvalue() if to_string else None
        return output

    @staticmethod
    def create_bulk_edges(to_string=False, config=None):
        output = None
        with Roger(to_string, config=config) as roger:
            roger.bulk.create_edges_csv_file()
            output = roger.log_stream.getvalue() if to_string else None
        return output

    @staticmethod
    def bulk_load (to_string=False, config=None):
        output = None
        with Roger (to_string, config=config) as roger:
            roger.bulk.insert ()
            output = roger.log_stream.getvalue () if to_string else None
        return output

    @staticmethod
    def validate (to_string=False, config=None):
        output = None
        with Roger (to_string, config=config) as roger:
            roger.bulk.validate ()
            output = roger.log_stream.getvalue () if to_string else None
        return output

    @staticmethod
    def check_tranql(to_string=False, config=None):
        output = None
        with Roger(to_string, config=config) as roger:
            roger.bulk.wait_for_tranql()
            output = roger.log_stream.getvalue() if to_string else None
        return output

if __name__ == "__main__":
    """ Roger CLI. """
    parser = argparse.ArgumentParser(description='Roger')
    parser.add_argument('-v', '--dataset-version', help="Dataset version.", default="v1.0")
    parser.add_argument('-d', '--data-root', help="Root of data hierarchy", default=None)
    parser.add_argument('-g', '--get-kgx', help="Get KGX objects", action='store_true')
    parser.add_argument('-l', '--load-kgx', help="Load via KGX", action='store_true')
    parser.add_argument('-s', '--create-schema', help="Infer schema", action='store_true')
    parser.add_argument('-m', '--merge-kgx', help="Merge KGX nodes", action='store_true')
    parser.add_argument('-b', '--create-bulk', help="Create bulk load", action='store_true')
    parser.add_argument('-i', '--insert', help="Do the bulk insert", action='store_true')
    parser.add_argument('-a', '--validate', help="Validate the insert", action='store_true')
    args = parser.parse_args ()

    biolink = BiolinkModel ()
    kgx = KGXModel (biolink)
    bulk = BulkLoad (biolink)
    if args.data_root is not None:
        config = get_config()
        data_root = args.data_root
        config.update({'data_root': data_root})
        log.info (f"data root:{data_root}")
    if args.get_kgx:
        kgx.get (dataset_version=args.dataset_version)
    if args.load_kgx:
        kgx.load ()
    if args.merge_kgx:
        kgx.merge ()
    if args.create_schema:
        kgx.create_schema ()
    if args.create_bulk:
        bulk.create ()
    if args.insert:
        bulk.insert ()
    if args.validate:
        bulk.validate ()

    sys.exit (0)
